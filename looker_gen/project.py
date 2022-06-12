from pathlib import Path
from typing import Dict

from looker_gen import config, ViewDirectoryStructure
from looker_gen.files import FileManager
from looker_gen.types import ModelName, NodeName


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

        self.models_dir_mapping = FileManager.build_models_dir_mapping(
            self.dbt_path, models_dirs
        )

    @staticmethod
    def get_model_name(node_name: NodeName) -> ModelName:
        return node_name.split(".")[2]

    def get_node_name(self, table_name: str) -> NodeName:
        return f"{self.model_prefix}.{table_name}"

    def get_catalog_for_node(self, node_name: NodeName) -> Dict:
        return self.catalog["nodes"][node_name]["columns"]

    def get_catalog_metadata_for_node(self, node_name: NodeName) -> Dict:
        return self.catalog["nodes"][node_name]["metadata"]

    def get_manifest_for_node(self, node_name: NodeName) -> Dict:
        return self.manifest["nodes"][node_name]["columns"]

    def _build_view_relative_path(self, node_name: NodeName) -> Path:
        """
        Build relative path based ViewDirectoryStructure config

        Will be appended to view output dir later
        """
        manifest = self.manifest["nodes"][node_name]

        if config.view_dir_structure == ViewDirectoryStructure.flat:
            return Path()

        elif config.view_dir_structure == ViewDirectoryStructure.dbt:
            relative_path = Path(manifest["path"])
            return relative_path.parent

        elif config.view_dir_structure == ViewDirectoryStructure.database:
            relative_path = Path(f'{manifest["database"]}/{manifest["schema"]}'.lower())
            return relative_path

        else:
            raise ValueError("Unable to build path for view directory config")

    def build_view_path(self, model_name: ModelName) -> Path:
        file_name = f"{model_name}.view.lkml"
        node_name = self.get_node_name(model_name)

        relative_path = self._build_view_relative_path(node_name)
        return relative_path.joinpath(file_name)
