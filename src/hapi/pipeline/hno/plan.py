import logging
from datetime import datetime
from typing import Dict, List, Tuple

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
        self.category_lookup = configuration["category"]
        self.hxltags = configuration["hxltags_narrow"]
        self.year = today.year
        if countryiso3s_to_process:
            self.countryiso3s_to_process = countryiso3s_to_process.split(",")
        else:
            self.countryiso3s_to_process = None
        self.adminone = AdminLevel(admin_level=1)
        self.adminone.setup_from_url(countryiso3s=self.countryiso3s_to_process)
        self.adminone.load_pcode_formats()
        self.admintwo = AdminLevel(admin_level=2)
        self.admintwo.setup_from_url(countryiso3s=self.countryiso3s_to_process)
        self.admintwo.load_pcode_formats()

    def get_plan_ids_and_countries(self, retriever: Retrieve) -> List:
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

    def get_location_mapping(
        self, countryiso3: str, data: Dict
    ) -> Tuple[Dict, bool]:
        location_mapping = {}
        valid_pcodes = 0
        invalid_pcodes = 0
        for location in data["locations"]:
            adminlevel = location.get("adminLevel")
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

    @staticmethod
    def get_cluster_mapping(caseload: Dict) -> Dict:
        cluster_mapping = {None: "ALL"}
        clusters = caseload["planGlobalClusters"]
        for cluster in clusters:
            cluster_code = cluster["globalClusterCode"]
            for plan_cluster_code in cluster["planClusters"]:
                if plan_cluster_code in cluster_mapping:
                    cluster_mapping[plan_cluster_code] = ""
                else:
                    cluster_mapping[plan_cluster_code] = cluster_code
        return cluster_mapping

    # def get_sector_code(self, caseload: Dict, warnings: List) -> str:
    #     sector_ref = caseload["caseloadCustomRef"]
    #     sector_info = self.sector_lookup.get(sector_ref)
    #     if sector_info is None:
    #         sector_description = caseload["caseloadDescription"]
    #         warnings.append(
    #             f"Unknown sector {sector_description} ({sector_ref})."
    #         )
    #         return None
    #     return sector_info["code"]

    def fill_population_status(self, row: Dict, data: Dict) -> None:
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

    def process(
        self, retriever: Retrieve, countryiso3: str, plan_id: str
    ) -> List:
        logger.info(f"Processing {countryiso3}")
        try:
            json = retriever.download_json(
                f"{self.hpc_url}plan/{plan_id}/responseMonitoring?includeCaseloadDisaggregation=true&includeIndicatorDisaggregation=false&disaggregationOnlyTotal=false",
            )
        except DownloadError as err:
            logger.exception(err)
            return
        data = json["data"]

        location_mapping, process_adm = self.get_location_mapping(
            countryiso3, data
        )
        cluster_mapping = self.get_cluster_mapping(data)

        errors = []
        warnings = []
        rows = {}
        for caseload in data["caseloads"]:
            caseload_description = caseload["caseloadDescription"]
            entity_id = caseload["entityId"]
            sector_code = cluster_mapping.get(entity_id, "NO_SECTOR_CODE")
            #           sector_code = self.get_sector_code(caseload, warnings)
            if sector_code is None:
                warnings.append(
                    f"Unknown sector {caseload_description} ({entity_id})."
                )
                continue
            # HACKY CODE TO DEAL WITH DIFFERENT AORS UNDER PROTECTION
            if sector_code == "":
                description_lower = caseload_description.lower()
                if any(
                    x in description_lower
                    for x in ("child", "niñez", "infancia")
                ):
                    sector_code = "PRO_CPM"
                elif any(
                    x in description_lower for x in ("housing", "logement")
                ):
                    sector_code = "PRO_HLP"
                elif any(
                    x in description_lower
                    for x in ("gender", "genre", "género")
                ):
                    sector_code = "PRO_GBV"
                elif any(x in description_lower for x in ("mine", "minas")):
                    sector_code = "PRO_MIN"
                elif any(
                    x in description_lower
                    for x in ("protection", "protección")
                ):
                    if (
                        "total" in description_lower
                        or "general" not in description_lower
                    ):
                        sector_code = "PRO"
                elif any(
                    x in description_lower for x in ("protection", "total")
                ):
                    sector_code = "PRO"
                else:
                    warnings.append(
                        f"Unknown sector {caseload_description} ({entity_id})."
                    )
                    continue
            national_row = {
                "Admin 1 PCode": "",
                "Admin 2 PCode": "",
                "Sector": sector_code,
                "Gender": "t",
                "Age Group": "all",
                "Disabled": "",
                "Population Group": "all",
            }

            self.fill_population_status(national_row, caseload)

            # adm1, adm2, sector, gender, age_range, disabled, population group
            key = ("", "", sector_code, "", "", "", "all")
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
                        if pcode not in self.adminone.pcodes:
                            pcode = self.adminone.convert_admin_pcode_length(
                                countryiso3, pcode
                            )
                            if not pcode:
                                continue
                        adm1 = pcode
                        adm2 = ""
                    elif adminlevel == 2:
                        pcode = self.admintwo.convert_admin_pcode_length(
                            countryiso3, pcode
                        )
                        if not pcode:
                            continue
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
                gender = category_info.get("gender")
                if gender is None:
                    gender = "t"
                    gender_key = ""  # make t be first after sorting by key
                else:
                    gender_key = gender
                row["Gender"] = gender
                age = category_info.get("age")
                if age is None:
                    age = "all"
                    age_key = ""  # make all be first after sorting by key
                else:
                    age_key = age
                row["Age Group"] = age
                disabled = category_info.get("disabled", "")
                row["Disabled"] = disabled
                population_group = category_info.get("group", "all")
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
                    gender_key,
                    age_key,
                    disabled,
                    population_group,
                )
                existing_row = rows.get(key)
                if not existing_row:
                    rows[key] = row
                    continue
                for key, value in row.items():
                    if value:
                        existing_row[key] = value

        for warning in dict.fromkeys(warnings):
            logger.warning(warning)
        for error in dict.fromkeys(errors):
            logger.error(error)
        return rows

    def generate_dataset(
        self, countryiso3: str, rows: Dict, folder: str
    ) -> Dataset:
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
