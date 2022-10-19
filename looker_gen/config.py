from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from os import getenv
from pathlib import Path


class ViewDirectoryStructure(Enum):
    """
    Sets directory structure for generated views.
    Project structure shown below; this config determins the structure within `views/` dir.
    ```
    LookMLRepo/
    ├─ explores/
    │  ├─ looker-gen.explore.lkml
    │  ├─ ....explore.lkml
    ├─ views/
    ├─ ....view.lkml
    ```

    `flat`: Default; No subdirectories within `views`
    `dbt`: Match directory structure of dbt project
    `db`: Match database.schema structure of target database
    """

    flat = "flat"
    dbt = "dbt"
    database = "database"

    @classmethod
    def _missing_(cls, value: object) -> ViewDirectoryStructure:
        return cls.flat


@dataclass(frozen=True)
class Config:
    """
    Project configurations set with `looker-gen.ini` file.
    """

    view_dir_structure: ViewDirectoryStructure
    type_mapping: Path

    # TODO
    # view_dir: set name of view directory


def import_config() -> Config:
    dir_config = getenv("LOOKERGEN_DIR_CONFIG", ViewDirectoryStructure.flat.value)
    view_dir_structure = ViewDirectoryStructure(dir_config)

    type_mapping = None
    mapping_path = getenv("LOOKERGEN_TYPE_MAPPING", None)
    if mapping_path:
        type_mapping = Path(mapping_path)

    return Config(view_dir_structure, type_mapping)
