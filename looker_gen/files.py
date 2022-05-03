import json
import os
from typing import Dict

import yaml


class FileManager:
    def __init__(self, output_dir) -> None:
        self.pwd = os.path.dirname(os.path.realpath(__file__))
        self.output_dir = output_dir
        self.explores_dir = os.path.join(output_dir, 'explores')
        self.views_dir = os.path.join(output_dir, 'views')
        self.models_dir = os.path.join(output_dir, 'models')

        # provision output dirs
        for dir in [self.explores_dir, self.views_dir, self.models_dir]:
            if not os.path.exists(dir):
                os.makedirs(dir)       

    @staticmethod
    def load_json(prefix: str, name: str) -> Dict:
        path = os.path.join(prefix, name)
        with open(path, 'r') as f:
            return json.load(f)

    @staticmethod
    def load_yaml(prefix: str, name: str) -> Dict:
        path = os.path.join(prefix, name)
        with open(path, 'r') as f:
            return yaml.load(f, Loader=yaml.Loader)

    def verify_output_dir(self) -> None:
        if not os.path.exists(self.output_dir):
            raise Exception(f'Cannon find destination {self.output_dir}')
