from datetime import datetime
import pickle
from typing import Optional, Sequence, Union

import numpy as np
import os
import pandas as pd
import pendulum

from nykp_meetup.query.event_signups import get_all_events_attendees
from nykp_meetup.season import Season, Seasons


def _to_datetime(dt) -> datetime:
    if isinstance(dt, datetime):
        return dt
    else:
        return pendulum.parse(dt)


class EventParticipationStats:
    def __init__(
        self,
        group_name: str,
        all_events_attendees: Union[pd.DataFrame, Sequence[dict]],
        seasons: Optional[Union[Season, Seasons, Sequence[Season]]] = None
    ):
        self.group_name = group_name
        self.all_events_attendees = pd.DataFrame(all_events_attendees)
        self.all_events_attendees["dateTime"] = self.all_events_attendees["dateTime"].map(_to_datetime)
        self.seasons = None
        if seasons:
            self.add_seasons(seasons)

    def save(self, path: str):
        data = {"group_name": self.group_name,
                "all_events_attendees": self.all_events_attendees.drop(columns="season", errors="ignore"),
                "seasons": self.seasons}
        dir_ = os.path.split(path)[0]
        if dir_:
            os.makedirs(dir_, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(data, f)

    @classmethod
    def load(cls, path: str) -> "EventParticipationStats":
        with open(path, "rb") as f:
            data = pickle.load(f)
        return cls(**data)

    def add_seasons(self, seasons: Union[Season, Sequence[Season]]):
        if isinstance(seasons, Season):
            seasons = [seasons]
        self.seasons = (self.seasons or Seasons([])) + seasons
        if "season" not in self.all_events_attendees.columns:
            self.all_events_attendees["season"] = np.nan
        for season in seasons:
            row_bools = ((self.all_events_attendees["dateTime"] >= season.start_date)
                         & (self.all_events_attendees["dateTime"] <= season.end_date))
            self.all_events_attendees.loc[row_bools, "season"] = season.name

    @classmethod
    def generate(cls, group_name: str, seasons=None, **kwargs) -> "EventParticipationStats":
        all_events_attendees = get_all_events_attendees(group_name, **kwargs)
        return cls(group_name, all_events_attendees, seasons)

    def get_events(self, by_season=False) -> pd.DataFrame:
        columns = ["event_id", "title", "dateTime", "going"]
        if "season" in self.all_events_attendees:
            columns.append("season")
        events = (
            self.all_events_attendees
            .groupby(columns, dropna=by_season)
            .count()["name"]
            .reset_index()
            .drop(columns="name")
        )
        return events

    def _get_attendees_total(self) -> pd.DataFrame:
        attendee_cols = ["name", "city", "state"]
        attendee_events = (
            self.all_events_attendees
            .groupby(attendee_cols)
            .count()["event_id"]
            .reset_index()
            .rename(columns={"event_id": "events"})
        )
        return attendee_events.sort_values("events", ascending=False).reset_index(drop=True)

    def _get_attendees_by_season(self) -> pd.DataFrame:
        all_events_attendees = self.all_events_attendees.copy()
        attendee_cols = ["name", "city", "state"]
        attendee_sessions_by_season = (
            all_events_attendees
            .groupby(attendee_cols + ["season"])
            .count()["event_id"]
            .reset_index()
            .pivot(index=attendee_cols, columns="season", values="event_id")
            .reset_index()
            .rename_axis(columns=None)
        )
        attendee_sessions_by_season["total"] = attendee_sessions_by_season.drop(columns=attendee_cols).sum(axis=1)
        return attendee_sessions_by_season.sort_values("total", ascending=False).reset_index(drop=True)

    def get_attendees(self, by_season=False) -> pd.DataFrame:
        if by_season:
            return self._get_attendees_by_season()
        else:
            return self._get_attendees_total()

    def get_attendee_stats(self, season: Optional[str] = None) -> dict:
        events_df = self.get_events(by_season=(season is not None))
        if season is None:
            attendees_df = self.all_events_attendees
        else:
            attendees_df = self.all_events_attendees[self.all_events_attendees["season"] == season]
        unique_attendees = attendees_df["name"].nunique()
        stats = {
            "sessions": len(events_df),
            "cumulative session participation": events_df["going"].sum(),
            "unique participants": unique_attendees,
            "median attendance": events_df["going"].median(),
        }
        stats["session titles"] = events_df["title"].value_counts().to_dict()
        return stats

    def _print_season_report(self, season: Optional[str] = None):
        if season:
            print(season.upper())
        stats = self.get_attendee_stats(season)
        for k, v in stats.items():
            print(f"{k}: {v}")

    def print_report(self, total=False, seasons=None):
        no_season = ("season" not in self.all_events_attendees or self.all_events_attendees["season"].count() == 0)
        if total is False and no_season:
            total = True
        if total:
            self._print_season_report()
        else:
            if seasons is None:
                seasons = self.all_events_attendees["season"].dropna().unique()
            elif isinstance(seasons, str):
                seasons = [seasons]
            for i, season in enumerate(seasons):
                self._print_season_report(season)
                if i < (len(seasons) - 1):
                    print()
