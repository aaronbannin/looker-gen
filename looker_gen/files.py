import json
from pathlib import Path
from typing import Dict, List

import yaml

from looker_gen import config, ViewDirectoryStructure
from looker_gen.types import ModelName, NodeName, View


class FileManager:
    def __init__(self, output_dir) -> None:
        self.pwd = Path.cwd()
        self.output_dir = Path(output_dir)
        self.explores_dir = self.output_dir.joinpath("explores")
        self.views_dir = self.output_dir.joinpath("views")

        # provision output dirs
        for dir in [self.explores_dir, self.views_dir]:
            if not dir.exists():
                Path.mkdir(dir, parents=True)

    @staticmethod
    def load_json(prefix: str, name: str) -> Dict:
        path = Path(prefix).joinpath(name)
        with open(path, "r") as f:
            return json.load(f)

    @staticmethod
    def load_yaml(prefix: str, name: str) -> Dict:
        path = Path(prefix).joinpath(name)
        with open(path, "r") as f:
            return yaml.safe_load(f)

    @staticmethod
    def build_models_dir_mapping(
        dbt_path: Path, models_dirs: List[str]
    ) -> Dict[ModelName, Path]:
        models = dbt_path.joinpath(models_dirs[0]).glob("**/*")
        return {m.stem: m for m in models if m.suffix == ".sql"}

    def verify_output_dir(self) -> None:
        if not self.output_dir.exists():
            raise Exception(f"Cannon find destination {self.output_dir}")


class DBTProject:
    def __init__(self, dbt_dir) -> None:
        self.dbt_path = Path(dbt_dir)
        project = FileManager.load_yaml(dbt_dir, "dbt_project.yml")
        dbt_target_location = self.dbt_path.joinpath(project["target-path"])
        models_dirs = project.get("model-paths", ["models"])

        self.project_name = project["name"]
        self.model_prefix = f"model.{self.project_name}"

        self.catalog = FileManager.load_json(dbt_target_location, "catalog.json")
        self.manifest = FileManager.load_json(dbt_target_location, "manifest.json")

        # make column names lower case for lookups; we are not case sensitive
        for node_name in self.catalog["nodes"].keys():
            formatted = {
                k.lower(): v for k, v in self.get_catalog_for_node(node_name).items()
            }
            self.catalog["nodes"][node_name]["columns"] = formatted

        for node_name in self.manifest["nodes"].keys():
            formatted = {
                k.lower(): v for k, v in self.get_manifest_for_node(node_name).items()
            }
            self.manifest["nodes"][node_name]["columns"] = formatted

        tmp_dir_mapping = FileManager.build_models_dir_mapping(self.dbt_path, models_dirs)
        self.models_dir_mapping = tmp_dir_mapping
        print(self.models_dir_mapping)
        # self.models_dir_mapping = {
        #     self.get_node_name(k): v for k, v in tmp_dir_mapping.items()
        # }

    def get_node_name(self, table_name: str) -> NodeName:
        return f"{self.model_prefix}.{table_name}"

    def get_catalog_for_node(self, node_name: NodeName) -> Dict:
        return self.catalog["nodes"][node_name]["columns"]

    def get_manifest_for_node(self, node_name: NodeName) -> Dict:
        return self.manifest["nodes"][node_name]["columns"]

    def build_view_path(self, files: FileManager, view: View) -> Path:
        file_name = "{0}.view.lkml".format(view.name)

        if config.view_dir_structure == ViewDirectoryStructure.flat:
            return files.views_dir.joinpath(file_name)
        elif config.view_dir_structure == ViewDirectoryStructure.dbt:
            project_path = self.models_dir_mapping[view.name]
            relative_path = project_path.relative_to(self.dbt_path)
            path = files.views_dir.joinpath(relative_path)
            print(f'path={path}')

            if not path.parent.exists():
                path.parent.mkdir(parents=True)

            return path
