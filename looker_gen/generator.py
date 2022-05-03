from dataclasses import asdict
from typing import Any, Dict, List

import os

from looker_gen.files import FileManager
from looker_gen.types import Dimension, DimensionGroup, ExploreConfig, JoinConfig, Measure, View


SNOWFLAKE_TYPE_CONVERSIONS = {
  'NUMBER': {'value': 'number'},
  'DECIMAL': {'value': 'number'},
  'NUMERIC': {'value': 'number'},
  'INT': {'value': 'number'},
  'INTEGER': {'value': 'number'},
  'BIGINT': {'value': 'number'},
  'SMALLINT': {'value': 'number'},
  'FLOAT': {'value': 'number'},
  'FLOAT4': {'value': 'number'},
  'FLOAT8': {'value': 'number'},
  'DOUBLE': {'value': 'number'},
  'DOUBLE PRECISION': {'value': 'number'},
  'REAL': {'value': 'number'},
  'VARCHAR': {'value': 'string'},
  'CHAR': {'value': 'string'},
  'CHARACTER': {'value': 'string'},
  'STRING': {'value': 'string'},
  'TEXT': {'value': 'string'},
  'BINARY': {'value': 'string'},
  'VARBINARY': {'value': 'string'},
  'BOOLEAN': {'value': 'yesno'},
  'DATE': {'value': 'time'},
  'DATETIME': {'value': 'time'},
  'TIME': {'value': 'string'},
  'TIMESTAMP': {'value': 'time'},
  'TIMESTAMP_NTZ': {'value': 'time'},
  'TIMESTAMP_TZ': {
    'value': 'time',
    'sql': "CAST(CONVERT_TIMEZONE('UTC', ${{TABLE}}.{name}) AS TIMESTAMP_NTZ)"
  },
  'TIMESTAMP_LTZ': {
    'value': 'time',
    'sql': "CAST(CONVERT_TIMEZONE('UTC', ${{TABLE}}.{name}) AS TIMESTAMP_NTZ)"
  },
  'VARIANT': {'value': 'string'},
  'OBJECT': {'value': 'string'},
  'ARRAY': {'value': 'string'},
  'GEOGRAPHY': {'value': 'string'},
}

LOOKER_DIM_GROUP_TYPES = ['time', 'duration']

def _get_model_name(node_name: str) -> str:
    return node_name.split('.')[2]

# Convert to title case and remove table prefixes
def _format_label(table_name: str) -> str:
    prefixes = ('dim_', 'fct_', 'fact_')
    label = table_name.lower()
    if label.startswith(prefixes):
        for prefix in prefixes:
            label = label.replace(prefix, '')

    label = label.replace('_', ' ').title()
    return label

class LookMLGenerator:
    def __init__(self, connection_name: str, dbt_dir: str) -> None:
        self.connection_name = connection_name

        project = FileManager.load_yaml(dbt_dir, 'dbt_project.yml')
        dbt_target_location = os.path.join(dbt_dir, project['target-path'])
        
        self.project_name = project['name']
        
        self.catalog = FileManager.load_json(dbt_target_location, 'catalog.json')
        self.manifest = FileManager.load_json(dbt_target_location, 'manifest.json')
        
        # format column names for lookups
        for node_name in self.catalog['nodes'].keys():
            formatted = {k.lower(): v for k, v in self.catalog['nodes'][node_name]['columns'].items()}
            self.catalog['nodes'][node_name]['columns'] = formatted

        # build explores
        self.explores = self.build_explores()

    def get_model_targets(self, models: str) -> List[str]:
        if models is None:
            return self.catalog['nodes'].keys()

        return {self.get_node_name(m.lower().strip()) for m in models.split(',')}

    def get_node_name(self, table_name: str) -> str:
        return f'model.{self.project_name}.{table_name}'

    def get_columns_for_node(self, node_name: str) -> Dict:
        return self.catalog['nodes'][node_name]['columns']

    def get_column_config(self, node_name: str, column_name: str) -> Dict[str, Any]:
        return self.manifest['nodes'][node_name]['columns'][column_name]['meta'].get('looker-gen', dict())

    def get_table_config(self, node_name: str) -> Dict[str, Any]:
        return self.manifest['nodes'][node_name]['config']['meta'].get('looker-gen', dict())

    def build_dimension(self, node_name: str, column_name: str) -> Dimension:
        # log.info(f'begin column {node_name} {column_name}')
        args = {}

        catalog = self.catalog['nodes'][node_name]['columns']
        manifest = self.manifest['nodes'][node_name]['columns']

        # should dim groups have type and datatype?
        # https://docs.looker.com/reference/field-params/datatype?version=22.6&lookml=new
        conversion = SNOWFLAKE_TYPE_CONVERSIONS[catalog[column_name]['type']]
        args['sql'] = conversion['sql'].format(name=catalog[column_name]['name']) if 'sql' in conversion else '${{TABLE}}.{0}'.format(catalog[column_name]['name'])
        args['type'] = conversion['value']

        if column_name in manifest:
            if manifest[column_name]['description'] != '':
                args['description'] = manifest[column_name]['description']

            config = self.get_column_config(node_name=node_name, column_name=column_name)
            args = {**args, **{k: v for k, v in config.items() if k != 'measures'}}

        return Dimension(column_name, looker_args=args)

    def build_dimensions_for_table(self, node_name: str) -> List[Dimension]:
        columns = self.get_columns_for_node(node_name=node_name)
        dims = [
            self.build_dimension(node_name=node_name, column_name=k) for k, v in columns.items()
            if SNOWFLAKE_TYPE_CONVERSIONS[v['type']]['value'] not in LOOKER_DIM_GROUP_TYPES
        ]

        return dims

    def build_dimension_group(self, node_name: str, column_name: str) -> DimensionGroup:
        timeframes = ['raw', 'time', 'hour', 'date', 'week', 'month', 'quarter', 'year']

        dim = self.build_dimension(node_name=node_name, column_name=column_name)

        return DimensionGroup(
            name=dim.name,
            timeframes=timeframes,
            looker_args=dim.looker_args
        )

    def build_dimension_groups_for_table(self, node_name: str) -> List[DimensionGroup]:
        columns = self.get_columns_for_node(node_name=node_name)
        dim_groups = [
            self.build_dimension_group(node_name=node_name, column_name=k) for k, v in columns.items()
            if SNOWFLAKE_TYPE_CONVERSIONS[v['type']]['value'] in LOOKER_DIM_GROUP_TYPES
        ]

        return dim_groups
        
    def build_measures(self, node_name: str, column_name: str) -> List[Measure]:
        config = self.get_column_config(node_name=node_name, column_name=column_name)

        def parse_measure_args(measure: Dict[str, Any], column_name: str) -> Dict[str, Any]:
            sql = f'${{{column_name}}}'
            looker_args = {k: v for k, v in measure.items() if k != 'name'}
            return {**looker_args, 'sql': sql}

        if 'measures' in config and config['measures'] is not None:
            return [Measure(m['name'], parse_measure_args(m, column_name)) for m in config['measures']]

        return []

    def build_measures_for_table(self, node_name: str) -> List[Measure]:
        manifest = self.manifest['nodes'][node_name]['columns']
        count = Measure('count', {'type': 'count'})
        nested_measures = [self.build_measures(node_name=node_name, column_name=column) for column in manifest.keys()]
        flatten = [measure for sublist in nested_measures for measure in sublist]
        flatten.append(count)
        return flatten

    def build_view(self, node_name: str, dimensions: List[Dimension], dimension_groups: List[DimensionGroup], measures: List[Measure]) -> View:
        schema = self.catalog['nodes'][node_name]['metadata']['schema']
        table = self.catalog['nodes'][node_name]['metadata']['name']
        config = self.get_table_config(node_name)

        if 'view_label' not in config:
            config['view_label'] = _format_label(table)

        return View(
            table.lower(),
            looker_args=config,
            sql_table_name=f'"{schema}"."{table}"',
            dimensions=dimensions,
            dimension_groups=dimension_groups,
            measures=measures
        )

    def build_explores(self) -> Dict[str, ExploreConfig]:
        explores: Dict[str, ExploreConfig] = {}

        def join_config_from_dict(join: Dict[str, Any]) -> JoinConfig:
            looker_args = {k: v for k, v in join.items() if k != 'name'}
            return JoinConfig(join['name'], looker_args)

        for node_name, node in self.manifest['nodes'].items():
            model_name = _get_model_name(node_name)
            config = self.get_table_config(node_name)

            if 'explore' in config:
                joins = [join_config_from_dict(j) for j in config['explore']['joins']]
                looker_args = {k: v for k, v in config['explore'].items() if k not in ['name', 'joins']}
                explore = ExploreConfig(model_name, joins, looker_args)
                explores[model_name] = explore

        return explores

    def build_model(self) -> Dict[str, Any]:
        import_string = '/explores/{0}.explore.lkml'
        return {
            'connection': f'{self.connection_name}',
            'includes': [import_string.format(e) for e in self.explores.keys()]
        }
