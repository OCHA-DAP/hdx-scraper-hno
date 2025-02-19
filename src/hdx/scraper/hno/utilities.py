from typing import Dict

from hdx.data.dataset import Dataset
from hdx.utilities.dateparse import parse_date


def set_time_period(dataset: Dataset, time_period: Dict, year: int) -> None:
    start_date = time_period["startdate"]
    new_start_date = parse_date(f"{year}-01-01")
    if new_start_date < start_date:
        start_date = new_start_date
    end_date = time_period["enddate"]
    new_end_date = parse_date(f"{year}-12-31")
    if new_end_date > end_date:
        end_date = new_end_date
    dataset.set_time_period(start_date, end_date)
