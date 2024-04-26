from os.path import join
from typing import Dict

from hdx.utilities.saver import save_json


class ProgressJSON:
    def __init__(
        self, year: int, saved_dir: str, save_test_data: bool = False
    ) -> None:
        self.path = join(saved_dir, f"test_progress-{year}.json")
        self.save_test_data = save_test_data
        self.plans = []
        self.json = {"data": {"plans": self.plans}}

    def add_plan(self, plan: Dict) -> None:
        if self.save_test_data:
            self.plans.append(plan)

    def save(self):
        if self.save_test_data:
            save_json(
                self.json,
                self.path,
            )
