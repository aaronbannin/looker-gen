import json
from pathlib import Path
from typing import Dict, List

import yaml

from looker_gen.types import ModelName


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
        return {m.stem: m.parent for m in models if m.suffix == ".sql"}

    def verify_output_dir(self) -> None:
        if not self.output_dir.exists():
            raise Exception(f"Cannon find destination {self.output_dir}")
