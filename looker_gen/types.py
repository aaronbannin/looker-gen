from __future__ import annotations
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List


ModelName = str
NodeName = str


@dataclass
class LookerType:
    name: str
    looker_args: Dict[str, Any]

    def __lt__(self, other: LookerType) -> bool:
        return self.name < other.name

    def as_dict(self) -> Dict:
        return {
            **self.looker_args,
            **asdict(
                self,
                dict_factory=lambda x: {
                    k: v for (k, v) in x if v is not None and k not in {"looker_args", "relative_path"}
                },
            ),
        }


@dataclass
class JoinConfig(LookerType):
    relative_path: Path

    def import_name(self) -> str:
        return self.looker_args["from"] if "from" in self.looker_args else self.name


@dataclass
class ExploreConfig:
    name: ModelName
    joins: List[JoinConfig]
    looker_args: Dict[str, Any]

    # TODO: change to get_*
    def import_name(self) -> str:
        return self.looker_args["from"] if "from" in self.looker_args else self.name


@dataclass
class Dimension(LookerType):
    pass


@dataclass
class DimensionGroup(LookerType):
    name: str
    # TODO: make init only?
    timeframes: List[str]
    looker_args: Dict[str, Any]


@dataclass
class Measure(LookerType):
    pass


@dataclass
class View:
    name: str
    sql_table_name: str
    dimensions: List[Dimension]
    dimension_groups: List[DimensionGroup]
    measures: List[Measure]
    looker_args: Dict[str, Any]
    # TODO: this needs to be removed?
    file_path: Path

    def as_dict(self) -> Dict:
        return {
            "view": {
                "name": self.name,
                "sql_table_name": self.sql_table_name,
                **{k: v for k, v in self.looker_args.items() if k != "explore"},
                "dimensions": [d.as_dict() for d in sorted(self.dimensions)],
                "dimension_groups": [
                    d.as_dict() for d in sorted(self.dimension_groups)
                ],
                "measures": [m.as_dict() for m in sorted(self.measures)],
            }
        }
