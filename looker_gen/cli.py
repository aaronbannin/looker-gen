import os
from typing import Optional, Set

import click
import lkml

from looker_gen.files import FileManager
from looker_gen.generator import LookMLGenerator, _get_model_name
from looker_gen.logging import log
from looker_gen.looker import linter


def get_schema_targets(schemas: str) -> Optional[Set[str]]:
    if schemas is None:
        return None

    return {s.lower().strip() for s in schemas.split(',')}

@click.command()
@click.option('-c', '--connection-name', 'connection_name', help='Name of DB connection in Looker', required=True, type=click.STRING)
@click.option('-d', '--dbt-dir', 'dbt_dir', default='./', help='Location of directory DBT project. Does not resolve "~/". Default is "./"', type=click.Path(exists=True, file_okay=False))
@click.option('-m', '--models', help='Build views and associated explores for the provided tables, comma seperated list', type=click.STRING)
@click.option('-o', '--output-dir', 'output_dir', default='./lookml', help='Destination for generated LookML files; using your current LookML repo is encouraged. Does not resolve "~/". Default is "./lookml"', type=click.Path(exists=True, file_okay=False))
@click.option('-s', '--schemas', help='Build lookml only for the provided schemas, comma seperated list', type=click.STRING)
def gen(connection_name: str, dbt_dir: str, models: str, output_dir: str, schemas: str) -> None:
    """
    Generate LookML files from a dbt project.
    """

    print(f'Using dbt-dir {dbt_dir} and outputting to {output_dir}')

    # Can we get some configs from dbt_project.yml?
    files = FileManager(output_dir)
    generator = LookMLGenerator(connection_name, dbt_dir)
    model_targets = generator.get_model_targets(models)
    schema_targets = get_schema_targets(schemas=schemas)

    for node_name in model_targets:
        log.debug(f'begin node={node_name}')
        schema = str(generator.catalog['nodes'][node_name]['metadata']['schema'])
        
        if schema_targets is not None and schema.lower() not in schema_targets:
            log.debug(f'{node_name} schema {schema} does not match target schemas, skipping')
            continue

        view = generator.build_view_from_node(node_name)

        # TODO: save in schema dirs
        # save to file
        view_name = '{0}.view.lkml'.format(view.name)
        view_path = os.path.join(files.views_dir, view_name)
        with open(view_path, 'w') as outfile:
            lkml.dump(view.as_dict(), outfile)

        table_name = _get_model_name(node_name)
        if table_name in generator.explores:
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

@click.command()
@click.option('-c', '--test-content', 'test_content', default=False, help='Run content validation', type=click.BOOL)
@click.option('-l', '--looker-dir', 'looker_dir', default='./', help='Location of directory LookML repo. Does not resolve "~/". Default is "./"', type=click.Path(exists=True, file_okay=False))
@click.option('-p', '--project-name', 'project_name', help='Name of project in Looker', required=True, type=click.STRING)
def validate(looker_dir: str, project_name: str, test_content: bool) -> None:
    """
    Validate LookML using Looker validation tools.
    Requires your local LookML branch to be pushed to origin (e.g. Github).
    """
    print(f'Running with looker-dir {looker_dir}')
    linter(looker_dir, project_name, test_content)
