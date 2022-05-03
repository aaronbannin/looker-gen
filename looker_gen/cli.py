import os
from typing import List, Optional, Set

import click
import lkml

from looker_gen.files import FileManager
from looker_gen.generator import LookMLGenerator, _get_model_name


def get_schema_targets(schemas: str) -> Optional[Set[str]]:
    if schemas is None:
        return None

    return {s.lower().strip() for s in schemas.split(',')}

@click.command()
@click.option('-c', '--connection-name', 'connection_name', help='Name of DB connection in Looker', type=click.STRING)
@click.option('-d', '--dbt-dir', 'dbt_dir', default='./', help='Location of directory DBT project. Default is "./"', type=click.STRING)
@click.option('-m', '--models', help='Build views and associated explores for the provided tables, comma seperated list', type=click.STRING)
@click.option('-o', '--output-dir', 'output_dir', default='./lookml', help='Destination for generated LookML files; using your current LookML repo is encouraged. Default is "./lookml"', type=click.STRING)
@click.option('-s', '--schemas', help='Build lookml only for the provided schemas, comma seperated list', type=click.STRING)
def main(connection_name: str, dbt_dir: str, models: str, output_dir: str, schemas: str) -> None:
    # Can we get some configs from dbt_project.yml?
    files = FileManager(output_dir)
    generator = LookMLGenerator(connection_name, dbt_dir)
    model_targets = generator.get_model_targets(models)
    schema_targets = get_schema_targets(schemas=schemas)

    for node_name in model_targets:
        # print(f'begin node={node_name}')
        schema = str(generator.catalog['nodes'][node_name]['metadata']['schema'])
        
        if schema_targets is not None and schema.lower() not in schema_targets:
            # make debug log line
            # print(f'{node_name} schema {schema} does not match target schemas, skipping')
            continue

        view = generator.build_view(
            node_name=node_name, 
            dimensions=generator.build_dimensions_for_table(node_name=node_name), 
            dimension_groups=generator.build_dimension_groups_for_table(node_name=node_name), 
            measures=generator.build_measures_for_table(node_name=node_name)
        )

        # TODO: save in schema dirs
        # save to file
        view_name = '{0}.view.lkml'.format(view.name)
        view_path = os.path.join(files.views_dir, view_name)
        with open(view_path, 'w') as outfile:
            lkml.dump(view.as_dict(), outfile)

        table_name = _get_model_name(node_name)
        if table_name in generator.explores:
            # lookml = {"includes": ["*.view"], "explore": [generator.explores[table_name]]}
            # print(lookml)
            explore = generator.explores[table_name].as_dict()
            explore_file = '{0}.explore.lkml'.format(table_name)
            explore_path = os.path.join(files.explores_dir, explore_file)
            with open(explore_path, 'w') as explore_file:
                lkml.dump(explore, explore_file)

        models = generator.build_model()
        models_name = 'looker-gen.model.lkml'
        models_path = os.path.join(files.models_dir, models_name)
        with open(models_path, 'w') as modelfile:
            lkml.dump(models, modelfile)


if __name__ == '__main__':
    main()
