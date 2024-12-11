import logging
from copy import copy
from typing import Callable, Dict, List, Optional

from slugify import slugify

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.country import Country

logger = logging.getLogger(__name__)


class DatasetGenerator:
    def __init__(
        self,
        configuration: Configuration,
        year: int,
    ) -> None:
        self._max_admin = int(configuration["max_admin"])
        self._resource_description = configuration["resource_description"]
        self._global_hxltags = configuration["hxltags"]
        self._country_hxltags = copy(self._global_hxltags)
        del self._country_hxltags["Country ISO3"]
        self._year = year

    def generate_resource(
        self,
        dataset: Dataset,
        resource_name: str,
        hxltags: Dict,
        rows: Dict,
        folder: str,
        filename: str,
        highest_admin: int,
    ) -> bool:
        resourcedata = {
            "name": resource_name,
            "description": self._resource_description,
        }

        headers = list(hxltags.keys())
        if headers[0] == "Country ISO3":
            index = 1
        else:
            index = 0
        for i in range(highest_admin, self._max_admin):
            del headers[highest_admin * 2 + index]
            del headers[highest_admin * 2 + index]

        success, results = dataset.generate_resource_from_iterable(
            headers,
            (rows[key] for key in sorted(rows)),
            hxltags,
            folder,
            filename,
            resourcedata,
        )
        return success

    def generate_dataset(
        self,
        title: str,
        name: str,
        resource_name: str,
        filename: str,
        hxltags: Dict,
        rows: Dict,
        folder: str,
        highest_admin: int,
    ) -> Optional[Dataset]:
        logger.info(f"Creating dataset: {title}")
        slugified_name = slugify(name).lower()
        dataset = Dataset(
            {
                "name": slugified_name,
                "title": title,
            }
        )
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("49f12a06-1605-4f98-89f1-eaec37a0fdfe")
        dataset.set_expected_update_frequency("Every year")

        tags = [
            "hxl",
            "humanitarian needs overview - hno",
            "people in need - pin",
        ]
        dataset.add_tags(tags)

        dataset.set_time_period_year_range(self._year)
        dataset.set_subnational(True)

        success = self.generate_resource(
            dataset,
            resource_name,
            hxltags,
            rows,
            folder,
            filename,
            highest_admin,
        )
        if success is False:
            logger.warning(f"{name} has no data!")
            return None
        return dataset

    @staticmethod
    def get_automated_resource_filename(countryiso3: str, year: int):
        # eg. afg_hpc_needs_api_2024.csv
        return f"{countryiso3.lower()}_hpc_needs_api_{year}.csv"

    @classmethod
    def move_resource(
        cls,
        resources: List[Resource],
        countryiso3: str,
        year: int,
        filename: str,
    ):
        insert_before = f"{countryiso3.lower()}_hpc_needs_{year}"
        from_index = None
        to_index = None
        for i, resource in enumerate(resources):
            resource_name = resource["name"]
            if resource_name == filename:
                from_index = i
            elif resource_name.startswith(insert_before):
                to_index = i
        if to_index is None:
            # insert at the start if a manual resource for year cannot be found
            to_index = 0
        resource = resources.pop(from_index)
        if from_index < to_index:
            # to index was calculated while element was in front
            to_index -= 1
        resources.insert(to_index, resource)
        return resource

    def add_country_resource(
        self,
        dataset: Dataset,
        countryiso3: str,
        rows: Dict,
        folder: str,
        year: int,
        highest_admin: int,
    ) -> Optional[Resource]:
        filename = self.get_automated_resource_filename(countryiso3, year)
        success = self.generate_resource(
            dataset,
            filename,
            self._country_hxltags,
            rows,
            folder,
            filename,
            highest_admin,
        )
        if not success:
            return None
        resources = dataset.get_resources()
        return self.move_resource(resources, countryiso3, year, filename)

    def get_country_dataset(
        self,
        countryiso3: str,
        read_fn: Callable[[str], Dataset] = Dataset.read_from_hdx,
    ) -> Optional[Dataset]:
        countryname = Country.get_country_name_from_iso3(countryiso3)
        if countryname is None:
            logger.error(f"Unknown ISO 3 code {countryiso3}!")
            return None
        name = f"{countryname} - Humanitarian Needs"
        slugified_name = slugify(name).lower()
        return read_fn(slugified_name)

    def get_global_dataset(
        self,
        read_fn: Callable[[str], Dataset] = Dataset.read_from_hdx,
    ) -> Optional[Dataset]:
        name = "Global HPC HNO"
        slugified_name = slugify(name).lower()
        return read_fn(slugified_name)

    def add_global_resource(
        self,
        dataset: Dataset,
        rows: Dict,
        folder: str,
        year: int,
        highest_admin: int,
    ) -> Optional[Resource]:
        filename = f"hpc_hno_{year}.csv"
        success = self.generate_resource(
            dataset,
            filename,
            self._global_hxltags,
            rows,
            folder,
            filename,
            highest_admin,
        )
        if not success:
            return None
        resources = dataset.get_resources()
        return self.move_resource(resources, "", year, filename)

    def generate_global_dataset(
        self,
        folder: str,
        rows: Dict,
        countries_with_data: List[str],
        year: int,
        highest_admin: Optional[int],
    ) -> Optional[Dataset]:
        if not rows or highest_admin is None:
            return None
        title = "Global Humanitarian Programme Cycle, Humanitarian Needs"
        name = "Global HPC HNO"
        resource_name = f"{name} {year}"
        filename = f"hpc_hno_{year}.csv"
        dataset = self.generate_dataset(
            title,
            name,
            resource_name,
            filename,
            self._global_hxltags,
            rows,
            folder,
            highest_admin,
        )
        dataset.add_country_locations(countries_with_data)
        return dataset
