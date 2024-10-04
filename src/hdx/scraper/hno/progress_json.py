from os.path import join
from typing import Dict

from hdx.utilities.saver import save_json


class ProgressJSON:
    def __init__(
        self, year: int, saved_dir: str, save_test_data: bool = False
    ) -> None:
        self._path = join(saved_dir, f"test_progress-{year}.json")
        self._save_test_data = save_test_data
        self._plans = []
        self._json = {"data": {"plans": self._plans}}

    def add_plan(self, plan: Dict) -> None:
        if self._save_test_data:
            self._plans.append(plan)

    def save(self):
        if self._save_test_data:
            save_json(
                self._json,
                self._path,
            )
