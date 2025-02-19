from logging import getLogger
from typing import Dict, List, Optional, Tuple

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.scraper.hno.utilities import set_time_period

logger = getLogger(__name__)


class HAPIDatasetGenerator:
    def __init__(
        self,
        configuration: Configuration,
        year: int,
        rows: Dict,
        countries_with_data: List[str],
    ) -> None:
        self._configuration = configuration["hapi_dataset"]
        self._year = year
        self._rows = rows
        self._countries_with_data = countries_with_data
        self.slugified_name = self._configuration["name"]

    def generate_dataset(self) -> Tuple[Dataset, Dict]:
        title = self._configuration["title"]
        logger.info(f"Creating dataset: {title}")
        dataset = Dataset(
            {
                "name": self.slugified_name,
                "title": title,
            }
        )
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("40d10ece-49de-4791-9aed-e164f1d16dd1")
        dataset.set_expected_update_frequency("Every month")
        dataset.add_tags(self._configuration["tags"])
        dataset["dataset_source"] = self._configuration["dataset_source"]
        dataset["license_id"] = self._configuration["license_id"]
        dataset.set_subnational(True)

        resource_config = self._configuration["resource"]
        return dataset, resource_config

    def set_time_period(self, dataset: Dataset, time_period: Dict):
        set_time_period(dataset, time_period, self._year)

    def generate_needs_dataset(
        self,
        folder: str,
        countries_with_data: List[str],
        dataset_id: str,
        resource_id: str,
        time_period: Optional[Dict],
    ) -> Optional[Dataset]:
        if len(self._rows) == 0:
            logger.warning("Humanitarian needs has no data!")
            return None

        dataset, resource_config = self.generate_dataset()
        dataset.add_country_locations(countries_with_data)

        if time_period:
            self.set_time_period(dataset, time_period)
        else:
            dataset.set_time_period_year_range(self._year)

        resource_name = resource_config["name"]
        resourcedata = {
            "name": f"{resource_name} {self._year}",
            "description": resource_config["description"],
        }
        hxltags = resource_config["hxltags"]
        filename = resource_config["filename"]

        for row in self._rows.values():
            row["dataset_hdx_id"] = dataset_id
            row["resource_hdx_id"] = resource_id

        success, _ = dataset.generate_resource_from_iterable(
            list(hxltags.keys()),
            (self._rows[key] for key in sorted(self._rows)),
            hxltags,
            folder,
            f"{filename}_{self._year}.csv",
            resourcedata,
        )
        if success is False:
            logger.warning(f"{resource_name} has no data!")
            return None

        dataset.preview_off()
        return dataset
