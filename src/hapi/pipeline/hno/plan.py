import logging
from datetime import datetime
from typing import Dict, List

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class Plan:
    def __init__(
        self,
        configuration: Configuration,
        today: datetime,
        countryiso3s_to_process: str = "",
    ):
        self.hpc_url = configuration["hpc_url"]
        self.population_status_lookup = configuration["population_status"]
        self.sector_lookup = configuration["sector"]
        self.category_lookup = configuration["category"]
        self.hxltags = configuration["hxltags_narrow"]
        self.year = today.year
        if countryiso3s_to_process:
            self.countryiso3s_to_process = countryiso3s_to_process.split(",")
        else:
            self.countryiso3s_to_process = None
        self.adminone = AdminLevel(admin_level=1)
        self.adminone.setup_from_url(countryiso3s=self.countryiso3s_to_process)
        self.admintwo = AdminLevel(admin_level=2)
        self.admintwo.setup_from_url(countryiso3s=self.countryiso3s_to_process)

    def get_plan_ids_and_countries(self, retriever: Retrieve):
        json = retriever.download_json(
            f"{self.hpc_url}fts/flow/plan/overview/progress/{self.year}"
        )
        plan_ids_countries = []
        for plan in json["data"]["plans"]:
            plan_id = plan["id"]
            if plan["planType"]["name"] != "Humanitarian response plan":
                continue
            countries = plan["countries"]
            if len(countries) != 1:
                continue
            countryiso3 = countries[0]["iso3"]
            if (
                self.countryiso3s_to_process
                and countryiso3 not in self.countryiso3s_to_process
            ):
                continue
            plan_ids_countries.append({"iso3": countryiso3, "id": plan_id})
        return plan_ids_countries

    def get_locations(self, countryiso3: str, data: Dict):
        location_mapping = {}
        valid_pcodes = 0
        invalid_pcodes = 0
        for location in data["locations"]:
            adminlevel = location.get("adminLevel")
            if adminlevel is None or adminlevel > 2:
                continue
            if adminlevel != 0:
                if adminlevel == 1:
                    admin = self.adminone
                else:
                    admin = self.admintwo
                if location["pcode"] in admin.pcodes:
                    valid_pcodes += 1
                else:
                    invalid_pcodes += 1
            location_mapping[location["id"]] = location
        if valid_pcodes / (valid_pcodes + invalid_pcodes) > 0.9:
            process_adm = True
        else:
            logger.error(f"Country {countryiso3} has many invalid pcodes!")
            process_adm = False
        return location_mapping, process_adm

    def get_sector_code(self, caseload: Dict, warnings: List):
        sector_ref = caseload["caseloadCustomRef"]
        sector_info = self.sector_lookup.get(sector_ref)
        if sector_info is None:
            sector_description = caseload["caseloadDescription"]
            warnings.append(
                f"Unknown sector {sector_description} ({sector_ref})."
            )
            return None
        return sector_info["code"]

    def fill_population_status(self, row: Dict, data: Dict):
        for input_key in self.population_status_lookup:
            header_tag = self.population_status_lookup[input_key]
            key = header_tag["header"]
            if input_key not in data:
                value = None
            else:
                value = data[input_key]
            if value is None:
                value = ""
            row[key] = value

    def process(self, retriever: Retrieve, countryiso3: str, plan_id: str):
        logger.info(f"Processing {countryiso3}")
        try:
            json = retriever.download_json(
                f"{self.hpc_url}plan/{plan_id}/responseMonitoring?includeCaseloadDisaggregation=true&includeIndicatorDisaggregation=false&disaggregationOnlyTotal=false",
            )
        except DownloadError as err:
            logger.exception(err)
            return
        data = json["data"]

        location_mapping, process_adm = self.get_locations(countryiso3, data)

        errors = []
        warnings = []
        rows = {}
        for caseload in data["caseloads"]:
            sector_code = self.get_sector_code(caseload, warnings)
            if not sector_code:
                continue
            national_row = {
                "Admin 1 PCode": "",
                "Admin 2 PCode": "",
                "Sector": sector_code,
                "Gender": "",
                "Age Group": "",
                "Disabled": "",
                "Population Group": "",
            }

            self.fill_population_status(national_row, caseload)

            # adm1, adm2, sector, gender, age_range, disabled, population group
            key = ("", "", sector_code, "", "", "", "")
            rows[key] = national_row

            for attachment in caseload["disaggregatedAttachments"]:
                location_id = attachment["locationId"]
                location = location_mapping.get(location_id)
                if not location:
                    error = f"Location {location_id} in {countryiso3} does not exist!"
                    errors.append(error)
                    continue
                adminlevel = location.get("adminLevel")
                if adminlevel == 0:
                    adm1 = ""
                    adm2 = ""
                else:
                    if not process_adm:
                        continue
                    pcode = location["pcode"]
                    if adminlevel == 1:
                        adm1 = pcode
                        adm2 = ""
                    elif adminlevel == 2:
                        adm1 = self.admintwo.pcode_to_parent[pcode]
                        adm2 = pcode
                    else:
                        continue
                row = {
                    "Admin 1 PCode": adm1,
                    "Admin 2 PCode": adm2,
                    "Sector": sector_code,
                }
                category_label = attachment["categoryLabel"]
                category_info = self.category_lookup.get(
                    category_label.lower()
                )
                if category_info is None:
                    category_name = attachment["categoryName"]
                    warnings.append(
                        f"Unknown category {category_name} ({category_label})."
                    )
                    continue
                gender = category_info.get("gender", "")
                row["Gender"] = gender
                age = category_info.get("age", "")
                row["Age Group"] = age
                disabled = category_info.get("disabled", "")
                row["Disabled"] = disabled
                population_group = category_info.get("group", "")
                row["Population Group"] = population_group

                data = {
                    x["metricType"]: x["value"]
                    for x in attachment["dataMatrix"]
                }
                self.fill_population_status(row, data)

                # adm1, adm2, sector, gender, age_range, disabled, population group
                key = (
                    adm1,
                    adm2,
                    sector_code,
                    gender,
                    age,
                    disabled,
                    population_group,
                )
                rows[key] = row

        for warning in dict.fromkeys(warnings):
            logger.warning(warning)
        for error in dict.fromkeys(errors):
            logger.error(error)
        return rows

    def generate_dataset(self, countryiso3: str, rows: Dict, folder: str):
        if rows is None:
            return None
        countryname = Country.get_country_name_from_iso3(countryiso3)
        if countryname is None:
            logger.error(f"Unknown ISO 3 code {countryiso3}!")
            return None, None, None
        title = f"{countryname} - Humanitarian Needs Overview"
        name = f"HNO Data for {countryiso3}"
        filename = f"hno_data_{countryiso3.lower()}.csv"

        logger.info(f"Creating dataset: {title}")
        slugified_name = slugify(name).lower()
        dataset = Dataset(
            {
                "name": slugified_name,
                "title": title,
            }
        )
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("hdx")
        dataset.set_expected_update_frequency("Every year")
        dataset.set_subnational(True)
        dataset.add_country_location(countryiso3)

        tags = [
            "hxl",
            "humanitarian needs overview - hno",
            "people in need - pin",
        ]
        dataset.add_tags(tags)

        dataset.set_time_period_year_range(self.year)

        resourcedata = {
            "name": name,
            "description": "HNO data with HXL tags",
        }

        success, results = dataset.generate_resource_from_iterator(
            list(next(iter(rows.values())).keys()),
            (rows[key] for key in sorted(rows)),
            self.hxltags,
            folder,
            filename,
            resourcedata,
        )
        if success is False:
            logger.warning(f"{name} has no data!")
            return None
        return dataset
