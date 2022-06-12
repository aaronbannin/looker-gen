from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from os import getenv

from looker_gen.logging import log


CONFIG_FILE_NAME = "looker-gen.ini"


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

    # TODO
    # type_mappings: customize type mapping from db to looker
    # view_dir: set name of view directory


def import_config() -> Config:
    dir_config = getenv("LOOKERGEN_DIR_CONFIG", ViewDirectoryStructure.flat.value)
    view_dir_structure = ViewDirectoryStructure(dir_config)

    return Config(view_dir_structure=view_dir_structure)
