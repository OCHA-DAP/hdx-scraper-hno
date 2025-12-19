from datetime import datetime
from typing import Dict

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.dateparse import parse_date


class TimePeriodHelper:
    def __init__(self, configuration: Configuration, year: int):
        time_periods = configuration.get("time_periods")
        self._start_date = None
        self._end_date = None
        if time_periods:
            year_timeperiod = time_periods.get(year)
            if year_timeperiod:
                self._start_date = parse_date(year_timeperiod["start_date"])
                self._end_date = parse_date(year_timeperiod["end_date"])
        if not self._start_date:
            self._start_date = parse_date(f"{year}-01-01")
            self._end_date = parse_date(f"{year}-12-31")
        self._year = year

    def get_year(self) -> int:
        return self._year

    def get_startdate(self) -> datetime:
        return self._start_date

    def get_enddate(self) -> datetime:
        return self._end_date

    def set_time_period(self, dataset: Dataset) -> None:
        dataset.set_time_period(self._start_date, self._end_date)

    def set_time_period_given_existing(
        self, dataset: Dataset, time_period: Dict
    ) -> None:
        start_date = time_period["startdate"]
        if self._start_date < start_date:
            start_date = self._start_date
        end_date = time_period["enddate"]
        if self._end_date > end_date:
            end_date = self._end_date
        dataset.set_time_period(start_date, end_date)
