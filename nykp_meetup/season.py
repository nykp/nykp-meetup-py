from dataclasses import dataclass
from datetime import datetime
from typing import Sequence, Union

import pendulum


@dataclass
class Season:
    name: str
    start_date: datetime
    end_date: datetime

    def __post_init__(self):
        if isinstance(self.start_date, str):
            self.start_date = pendulum.parse(self.start_date)
        if isinstance(self.end_date, str):
            self.end_date = pendulum.parse(self.end_date)

    def overlaps_with(self, other: "Season") -> bool:
        if self.start_date <= other.start_date:
            first = self
            second = other
        else:
            first = other
            second = self
        return first.end_date >= second.start_date


class Seasons:
    class ValidationError(Exception):
        pass

    def __init__(self, seasons, allow_overlap=False):
        if isinstance(seasons, Season):
            seasons = [seasons]
        elif isinstance(seasons, Seasons):
            seasons = seasons.seasons
        if not allow_overlap and self.any_overlapping(seasons):
            raise Seasons.ValidationError("Overlapping seasons present. To override this, set allow_overlap=True.")
        self._seasons = seasons
        self._allow_overlap = allow_overlap

    @staticmethod
    def any_overlapping(seasons: Sequence[Season]):
        for i in range(len(seasons) - 1):
            for j in range(i + 1, len(seasons)):
                if seasons[i].overlaps_with(seasons[j]):
                    return True
        return False

    def __add__(self, other: Union[Season, Sequence[Season], "Seasons"]) -> "Seasons":
        if not isinstance(other, Seasons):
            other = Seasons(other, allow_overlap=self._allow_overlap)
        return Seasons(self._seasons + other._seasons, allow_overlap=(self._allow_overlap and other._allow_overlap))

    def __iter__(self):
        return iter(self._seasons)
