from datetime import datetime
from typing import Dict, Tuple

from hdx.data.dataset import Dataset


def get_admin_lookup(reference_period_start: datetime) -> Dict:
    # HAPI API call to get admin 2 table given reference_period_start
    return {}


def get_resource_ref(dataset: Dataset) -> Tuple[str, str]:
    resource = dataset.get_resource()
    resource_id = resource["id"]
    resource_ref = resource_id  # lookup needed here
    return resource_id, resource_ref


def get_reference_period(dataset: Dataset) -> Tuple[datetime, datetime]:
    time_period = dataset.get_time_period()
    return time_period["startdate_str"], time_period["enddate_str"]
