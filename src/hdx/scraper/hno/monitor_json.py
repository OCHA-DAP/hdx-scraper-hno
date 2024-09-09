from os.path import join
from typing import Dict, List

from .caseload_json import CaseloadJSON
from hdx.utilities.saver import save_json


class MonitorJSON:
    def __init__(self, saved_dir: str, save_test_data: bool = False) -> None:
        self.saved_dir = saved_dir
        self.save_test_data = save_test_data
        self.locations = []
        self.caseloads = []
        self.json = {
            "data": {"locations": self.locations, "caseloads": self.caseloads}
        }

    def add_location(self, location: Dict) -> None:
        if self.save_test_data:
            self.locations.append(location)

    def set_global_clusters(self, clusters: List) -> None:
        if self.save_test_data:
            self.json["data"]["planGlobalClusters"] = clusters

    def add_caseload_json(self, caseload_json: CaseloadJSON) -> None:
        if caseload_json.caseload is not None:
            self.caseloads.append(caseload_json.caseload)

    def save(self, plan_id: str) -> None:
        if self.save_test_data:
            path = join(
                self.saved_dir,
                f"test_{plan_id}-responsemonitoring-includecaseloaddisaggregation-true-includeindicatordisaggregation-false-disaggregationonlytotal-false.json",
            )
            save_json(
                self.json,
                path,
            )
