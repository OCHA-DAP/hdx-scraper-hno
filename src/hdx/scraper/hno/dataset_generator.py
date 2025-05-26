import logging
from copy import copy
from typing import Callable, Dict, List, Optional, Tuple

from slugify import slugify

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.scraper.hno.utilities import set_time_period

logger = logging.getLogger(__name__)


class DatasetGenerator:
    global_name = "Global HPC HNO"
    slugified_name = slugify(global_name).lower()

    def __init__(
        self,
        configuration: Configuration,
        year: int,
    ) -> None:
        self._max_admin = int(configuration["max_admin"])
        self._resource_description = configuration["resource_description"]
        self.resource_description_extra = configuration["resource_description_extra"]
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
        resource_description_extra: bool = False,
        p_coded: bool = None,
    ) -> Tuple[bool, Dict]:
        if highest_admin == 0:
            extra_text = f"national {self._year}"
        else:
            extra_text = f"subnational {self._year}"
        description = self._resource_description.replace("<>", extra_text)
        if resource_description_extra:
            description += f" {resource_description_extra}"
        resourcedata = {
            "name": resource_name,
            "description": self._resource_description.replace("<>", extra_text),
        }
        if p_coded:
            resourcedata["p_coded"] = p_coded

        headers = list(hxltags.keys())
        if headers[0] == "Country ISO3":
            index = 1
        else:
            index = 0
        for i in range(highest_admin, self._max_admin):
            del headers[highest_admin * 2 + index]
            del headers[highest_admin * 2 + index]

        return dataset.generate_resource_from_iterable(
            headers,
            (rows[key] for key in sorted(rows)),
            hxltags,
            folder,
            filename,
            resourcedata,
        )

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
        p_coded: bool = None,
    ) -> Tuple[Optional[Dataset], Optional[Resource]]:
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

        success, results = self.generate_resource(
            dataset,
            resource_name,
            hxltags,
            rows,
            folder,
            filename,
            highest_admin,
            p_coded=p_coded,
        )
        if success is False:
            logger.warning(f"{name} has no data!")
            return None, None
        return dataset, results["resource"]

    def get_automated_resource_filename(self, countryiso3: str):
        # eg. afg_hpc_needs_api_2024.csv
        return f"{countryiso3.lower()}_hpc_needs_api_{self._year}.csv"

    def set_dataset_time_period(self, dataset: Dataset):
        time_period = dataset.get_time_period()
        set_time_period(dataset, time_period, self._year)

    def add_country_resource(
        self,
        dataset: Dataset,
        countryiso3: str,
        rows: Dict,
        folder: str,
        highest_admin: int,
    ) -> Optional[Resource]:
        filename = self.get_automated_resource_filename(countryiso3)
        p_coded = True if highest_admin > 0 else None
        success, _ = self.generate_resource(
            dataset,
            filename,
            self._country_hxltags,
            rows,
            folder,
            filename,
            highest_admin,
            True,
            p_coded=p_coded,
        )
        if not success:
            return None
        self.set_dataset_time_period(dataset)
        insert_before = f"{countryiso3.lower()}_hpc_needs_{self._year}"
        return dataset.move_resource(filename, insert_before)

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

    def add_global_resource(
        self,
        dataset: Dataset,
        rows: Dict,
        folder: str,
        highest_admin: int,
    ) -> Optional[Resource]:
        filename = f"hpc_hno_{self._year}.csv"
        resource_name = f"{self.global_name} {self._year}"
        success, _ = self.generate_resource(
            dataset,
            resource_name,
            self._global_hxltags,
            rows,
            folder,
            filename,
            highest_admin,
            p_coded=True,
        )
        if not success:
            return None
        self.set_dataset_time_period(dataset)
        insert_before = f"hpc_hno_{self._year - 1}.csv"
        return dataset.move_resource(resource_name, insert_before)

    def generate_global_dataset(
        self,
        folder: str,
        rows: Dict,
        countries_with_data: List[str],
        highest_admin: Optional[int],
    ) -> Tuple[Optional[Dataset], Optional[Resource]]:
        if not rows or highest_admin is None:
            return None, None
        title = "Global Humanitarian Programme Cycle, Humanitarian Needs"
        resource_name = f"{self.global_name} {self._year}"
        filename = f"hpc_hno_{self._year}.csv"
        dataset, resource = self.generate_dataset(
            title,
            self.global_name,
            resource_name,
            filename,
            self._global_hxltags,
            rows,
            folder,
            highest_admin,
            p_coded=True,
        )
        dataset.add_country_locations(countries_with_data)
        return dataset, resource

    def generate_country_dataset(
        self,
        countryiso3: str,
        folder: str,
        rows: Dict,
        highest_admin: Optional[int],
    ) -> Optional[Dataset]:
        if not rows or highest_admin is None:
            return None
        countryname = Country.get_country_name_from_iso3(countryiso3)
        title = f"{countryname}: Humanitarian Needs"
        filename = self.get_automated_resource_filename(countryiso3)
        p_coded = True if highest_admin > 0 else None

        dataset, _ = self.generate_dataset(
            title,
            title,
            filename,
            filename,
            self._country_hxltags,
            rows,
            folder,
            highest_admin,
            p_coded=p_coded,
        )
        dataset.add_country_location(countryiso3)
        return dataset
