from typing import Any, Dict, List, Set
from looker_gen.files import FileManager

from looker_gen.project import DBTProject
from looker_gen.types import (
    Dimension,
    DimensionGroup,
    ExploreConfig,
    JoinConfig,
    Measure,
    ModelName,
    NodeName,
    View,
)

SNOWFLAKE_TYPE_CONVERSIONS = {
    "NUMBER": {"value": "number"},
    "DECIMAL": {"value": "number"},
    "NUMERIC": {"value": "number"},
    "INT": {"value": "number"},
    "INTEGER": {"value": "number"},
    "BIGINT": {"value": "number"},
    "SMALLINT": {"value": "number"},
    "FLOAT": {"value": "number"},
    "FLOAT4": {"value": "number"},
    "FLOAT8": {"value": "number"},
    "DOUBLE": {"value": "number"},
    "DOUBLE PRECISION": {"value": "number"},
    "REAL": {"value": "number"},
    "VARCHAR": {"value": "string"},
    "CHAR": {"value": "string"},
    "CHARACTER": {"value": "string"},
    "STRING": {"value": "string"},
    "TEXT": {"value": "string"},
    "BINARY": {"value": "string"},
    "VARBINARY": {"value": "string"},
    "BOOLEAN": {"value": "yesno"},
    "DATE": {"value": "time"},
    "DATETIME": {"value": "time"},
    "TIME": {"value": "string"},
    "TIMESTAMP": {"value": "time"},
    "TIMESTAMP_NTZ": {"value": "time"},
    "TIMESTAMP_TZ": {
        "value": "time",
        "sql": "CAST(CONVERT_TIMEZONE('UTC', ${{TABLE}}.\"{name}\") AS TIMESTAMP_NTZ)",
    },
    "TIMESTAMP_LTZ": {
        "value": "time",
        "sql": "CAST(CONVERT_TIMEZONE('UTC', ${{TABLE}}.\"{name}\") AS TIMESTAMP_NTZ)",
    },
    "VARIANT": {"value": "string"},
    "OBJECT": {"value": "string"},
    "ARRAY": {"value": "string"},
    "GEOGRAPHY": {"value": "string"},
}

LOOKER_DIM_GROUP_TYPES = ["time", "duration"]


# Convert to title case and remove table prefixes
def _format_label(table_name: str) -> str:
    prefixes = ("dim_", "fct_", "fact_")
    label = table_name.lower()
    if label.startswith(prefixes):
        for prefix in prefixes:
            label = label.replace(prefix, "")

    label = label.replace("_", " ").title()
    return label


class LookMLGenerator:
    def __init__(self, dbt_dir: str) -> None:
        self.project = DBTProject(dbt_dir)
        self.explores = self.build_explores()

    def get_model_targets(self, models: str) -> Set[str]:
        if models is None:
            return {
                k
                for k in self.project.catalog["nodes"].keys()
                if k.startswith(self.project.model_prefix)
            }

        return {
            self.project.get_node_name(m.lower().strip()) for m in models.split(",")
        }

    def get_column_config(
        self, node_name: NodeName, column_name: str
    ) -> Dict[str, Any]:
        manifest = self.project.get_manifest_for_node(node_name)
        if column_name in manifest:
            return manifest[column_name]["meta"].get("looker-gen", dict())

        return dict()

    def get_table_config(self, node_name: NodeName) -> Dict[str, Any]:
        return self.project.manifest["nodes"][node_name]["config"]["meta"].get(
            "looker-gen", dict()
        )

    def is_dimension(
        self, node_name: NodeName, column_name: str, catalog: Dict
    ) -> bool:
        config = self.get_column_config(node_name, column_name)
        ignored = "ignore-dim" in config
        if (
            SNOWFLAKE_TYPE_CONVERSIONS[catalog["type"]]["value"]
            in LOOKER_DIM_GROUP_TYPES
        ) or ignored:
            return False

        return True

    def is_dimension_group(
        self, node_name: NodeName, column_name: str, catalog: Dict
    ) -> bool:
        config = self.get_column_config(node_name, column_name)
        ignored = "ignore-dim" in config
        if (
            SNOWFLAKE_TYPE_CONVERSIONS[catalog["type"]]["value"]
            in LOOKER_DIM_GROUP_TYPES
        ) and not ignored:
            return True

        return False

    def is_custom_dimension(self, node_name: NodeName, column_name: str) -> bool:
        manifest = self.project.get_manifest_for_node(node_name)

        # Column has no declaration in dbt
        if column_name not in manifest:
            return False

        # Column has declared 'looker-only' and set 'column-type'
        column_meta = manifest[column_name]["meta"]
        if "looker-gen" in column_meta and "looker-only" in column_meta["looker-gen"]:
            if column_meta["looker-gen"].get("column-type", None) in {
                "dim",
                "dimension",
            }:
                return True

        return False

    def build_dimension(self, node_name: NodeName, column_name: str) -> Dimension:
        args = {}

        catalog = self.project.get_catalog_for_node(node_name)
        manifest = self.project.get_manifest_for_node(node_name)

        # should dim groups have type and datatype?
        # https://docs.looker.com/reference/field-params/datatype?version=22.6&lookml=new
        conversion = SNOWFLAKE_TYPE_CONVERSIONS[catalog[column_name]["type"]]
        args["sql"] = (
            conversion["sql"].format(name=catalog[column_name]["name"])
            if "sql" in conversion
            else '${{TABLE}}."{0}"'.format(catalog[column_name]["name"])
        )
        args["type"] = conversion["value"]

        if column_name in manifest:
            if manifest[column_name]["description"] != "":
                args["description"] = manifest[column_name]["description"]

            config = self.get_column_config(
                node_name=node_name, column_name=column_name
            )
            args = {**args, **{k: v for k, v in config.items() if k != "measures"}}

        # Match Looker name formatting
        formatted_name = (
            column_name[:-3] if column_name.endswith("_at") else column_name
        )
        return Dimension(formatted_name, looker_args=args)

    def build_custom_dimension(self, config: Dict[str, Any]) -> Dimension:
        args = {
            k: v
            for k, v in config["meta"]["looker-gen"].items()
            if k not in {"column-type", "looker-only", "measures"}
        }
        if "description" in config and config["description"] != "":
            args["description"] = config["description"]

        return Dimension(config["name"], args)

    def build_dimensions_for_table(self, node_name: NodeName) -> List[Dimension]:
        columns = self.project.get_catalog_for_node(node_name=node_name)

        dims = [
            self.build_dimension(node_name=node_name, column_name=k)
            for k, v in columns.items()
            if self.is_dimension(node_name, k, v)
        ]

        return dims

    def build_dimension_group(
        self, node_name: NodeName, column_name: str
    ) -> DimensionGroup:
        timeframes = ["raw", "time", "hour", "date", "week", "month", "quarter", "year"]
        dim = self.build_dimension(node_name=node_name, column_name=column_name)

        return DimensionGroup(
            name=dim.name, timeframes=timeframes, looker_args=dim.looker_args
        )

    def build_dimension_groups_for_table(
        self, node_name: NodeName
    ) -> List[DimensionGroup]:
        columns = self.project.get_catalog_for_node(node_name=node_name)
        dim_groups = [
            self.build_dimension_group(node_name=node_name, column_name=k)
            for k, v in columns.items()
            if self.is_dimension_group(node_name, k, v)
        ]

        return dim_groups

    def build_measures(self, node_name: NodeName, column_name: str) -> List[Measure]:
        config = self.get_column_config(node_name, column_name)
        manifest = self.project.get_manifest_for_node(node_name)
        catalog = self.project.get_catalog_for_node(node_name)

        def parse_measure_args(
            measure: Dict[str, Any], column_name: str
        ) -> Dict[str, Any]:
            name_in_db = catalog[column_name]["name"]
            looker_args = {k: v for k, v in measure.items() if k != "name"}
            looker_args["sql"] = f'${{TABLE}}."{name_in_db}"'

            if (
                "description" in manifest[column_name]
                and manifest[column_name]["description"] != ""
            ):
                looker_args["description"] = manifest[column_name]["description"]

            return looker_args

        if "measures" in config and config["measures"] is not None:
            return [
                Measure(m["name"], parse_measure_args(m, column_name))
                for m in config["measures"]
            ]

        return []

    def build_measures_for_table(self, node_name: NodeName) -> List[Measure]:
        manifest = self.project.get_manifest_for_node(node_name)
        count = Measure("count", {"type": "count"})
        nested_measures = [
            self.build_measures(node_name=node_name, column_name=column)
            for column in manifest.keys()
        ]
        flatten = [measure for sublist in nested_measures for measure in sublist]
        flatten.append(count)
        return flatten

    def build_view_from_node(self, node_name: NodeName,  files: FileManager) -> View:
        catalog = self.project.get_catalog_for_node(node_name)
        manifest = self.project.get_manifest_for_node(node_name)

        # find columns in manifest that do not exist in db catalog
        diff = set(manifest.keys()).difference(set(catalog.keys()))

        # TODO: Add support for custom measures, dim groups?
        custom_dims = [
            self.build_custom_dimension(manifest[col])
            for col in diff
            if self.is_custom_dimension(node_name, col)
        ]

        metadata = self.project.get_catalog_metadata_for_node(node_name)
        schema: str = metadata["schema"]
        table: str = metadata["name"]
        config = self.get_table_config(node_name)

        if "view_label" not in config:
            config["view_label"] = _format_label(table)

        dimensions = [*self.build_dimensions_for_table(node_name), *custom_dims]
        dimension_groups = self.build_dimension_groups_for_table(node_name)
        measures = self.build_measures_for_table(node_name)

        file_path = self.project.build_view_path(node_name, files)

        return View(
            table.lower(),
            looker_args=config,
            sql_table_name=f'"{schema}"."{table}"',
            dimensions=dimensions,
            dimension_groups=dimension_groups,
            measures=measures,
            file_path=file_path
        )

    def build_explore_config(
        self, model_name: ModelName, table_config: Dict[str, Any]
    ) -> ExploreConfig:
        def join_config_from_dict(join: Dict[str, Any]) -> JoinConfig:
            # node_name = self.project.get_node_name(join["name"])
            # relative_path = self.project._build_view_relative_path(node_name)
            relative_path = self.project.build_view_path_for_explore(join["name"])
            looker_args = {k: v for k, v in join.items() if k != "name"}
            return JoinConfig(join["name"], looker_args, relative_path)

        if table_config["explore"] is None:
            return ExploreConfig(model_name, [], {})


        join_configs = table_config.get("explore", {}).get("joins", {})
        joins = [join_config_from_dict(j) for j in join_configs]
        looker_args = {
            k: v
            for k, v in table_config["explore"].items()
            if k not in ["name", "joins"]
        }

        # explore name is aliased
        name = model_name
        if "name" in table_config["explore"]:
            looker_args["from"] = model_name
            name = table_config["explore"]["name"]

        if "label" not in table_config["explore"]:
            looker_args["label"] = _format_label(name)

        return ExploreConfig(name, joins, looker_args)

    def build_explores(self) -> Dict[str, ExploreConfig]:
        explores: Dict[str, ExploreConfig] = {}

        # TODO: get relative path for view and save on explores object
        for node_name in self.project.manifest["nodes"].keys():
            model_name = self.project.get_model_name(node_name)
            config = self.get_table_config(node_name)

            if "explore" in config:
                explore = self.build_explore_config(model_name, config)
                explores[model_name] = explore

        # print(explores)
        return explores

    def build_explore_from_config(self, config: ExploreConfig, files: FileManager):
        print("build_explore_from_config")
        print(config)

        # def build_view_import(join: JoinConfig) -> str:
        #     print()
        #     return str(files.views_dir.joinpath(join.relative_path))

        join_imports = list(str(files.views_dir.joinpath(j.relative_path)) for j in config.joins)
        parent_import = str(files.views_dir.joinpath(self.project.build_view_path_for_explore(config.name)))

        args = {**config.looker_args, "name": config.name}
        joins = [j.as_dict() for j in config.joins]

        # # TODO: views dir should be parametized
        # import_string = "/views/{0}.view.lkml"
        # # use a set to get unqiue values
        # # TODO: use import_name to _build_view_relative_path for view
        # # TODO: this logic needs to live somehere else, make this model dumber
        # join_imports = list({import_string.format(j.import_name()) for j in self.joins})

        # parent_import = import_string.format(self.import_name())

        # args = {**self.looker_args, "name": self.name}
        # joins = [j.as_dict() for j in self.joins]

        return {
            "includes": [parent_import, *sorted(join_imports)],
            "explore": {**args, "joins": joins},
        }


    def build_explore_export(self) -> Dict[str, Any]:
        import_string = "/explores/{0}.explore.lkml"
        return {
            "includes": sorted([import_string.format(e) for e in self.explores.keys()]),
        }
