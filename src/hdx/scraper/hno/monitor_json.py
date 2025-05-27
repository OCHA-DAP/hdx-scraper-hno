from os.path import join
from typing import Dict, List

from .caseload_json import CaseloadJSON
from hdx.utilities.saver import save_json


class MonitorJSON:
    def __init__(self, saved_dir: str, save_test_data: bool = False) -> None:
        self._saved_dir = saved_dir
        self._save_test_data = save_test_data
        self._locations = []
        self._caseloads = []
        self._json = {
            "data": {
                "locations": self._locations,
                "caseloads": self._caseloads,
            }
        }

    def set_last_published(self, last_published_version: str, last_published_date: str):
        if self._save_test_data:
            self._json["data"]["lastPublishedVersion"] = last_published_version
            self._json["data"]["lastPublishedDate"] = last_published_date

    def add_location(self, location: Dict) -> None:
        if self._save_test_data:
            self._locations.append(location)

    def set_global_clusters(self, clusters: List) -> None:
        if self._save_test_data:
            self._json["data"]["planGlobalClusters"] = clusters

    def add_caseload_json(self, caseload_json: CaseloadJSON) -> None:
        if caseload_json._caseload is not None:
            self._caseloads.append(caseload_json._caseload)

    def save(self, plan_id: str) -> None:
        if self._save_test_data:
            path = join(
                self._saved_dir,
                f"test_{plan_id}-responsemonitoring-includecaseloaddisaggregation-true-includeindicatordisaggregation-false-disaggregationonlytotal-false.json",
            )
            save_json(
                self._json,
                path,
            )
