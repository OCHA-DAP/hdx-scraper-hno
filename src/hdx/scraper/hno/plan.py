import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.dateparse import parse_date
from hdx.utilities.retriever import Retrieve
from slugify import slugify

from .caseload_json import CaseloadJSON
from .monitor_json import MonitorJSON
from .progress_json import ProgressJSON

logger = logging.getLogger(__name__)


class Plan:
    def __init__(
        self,
        configuration: Configuration,
        year: int,
        countryiso3s_to_process: str = "",
        pcodes_to_process: str = "",
    ) -> None:
        self.hpc_url = configuration["hpc_url"]
        self.population_status_lookup = configuration["population_status"]
        self.category_lookup = configuration["category"]
        self.hxltags_narrow = configuration["hxltags_narrow"]
        self.year = year
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
        if pcodes_to_process:
            self.pcodes_to_process = pcodes_to_process.split(",")
        else:
            self.pcodes_to_process = None

    def get_plan_ids_and_countries(
        self, retriever: Retrieve, progress_json: ProgressJSON
    ) -> List:
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
            plan["caseLoads"] = []
            progress_json.add_plan(plan)
            plan_ids_countries.append({"iso3": countryiso3, "id": plan_id})
        progress_json.save()
        return plan_ids_countries

    def get_location_mapping(
        self,
        countryiso3: str,
        data: Dict,
        monitor_json: MonitorJSON,
        errors: List,
        warnings: List,
    ) -> Tuple[Dict, bool]:
        location_mapping = {}
        valid_pcodes = 0
        invalid_pcodes = set()
        for location in data["locations"]:
            adminlevel = location.get("adminLevel")
            if adminlevel == 1:
                admin = self.adminone
            elif adminlevel == 2:
                admin = self.admintwo
            else:
                admin = None
            if admin:
                pcode = location["pcode"].strip()
                if pcode not in admin.pcodes:
                    pcode = admin.convert_admin_pcode_length(
                        countryiso3, pcode
                    )
                if pcode in admin.pcodes:
                    valid_pcodes += 1
                    location["pcode"] = pcode
                    if self.pcodes_to_process:
                        if pcode in self.pcodes_to_process:
                            monitor_json.add_location(location)
                    else:
                        monitor_json.add_location(location)
                else:
                    invalid_pcodes.add((location["pcode"], location["name"]))
            elif adminlevel == 0:
                monitor_json.add_location(location)
            location_mapping[location["id"]] = location
        if valid_pcodes / (valid_pcodes + len(invalid_pcodes)) > 0.9:
            process_adm = True
        else:
            errors.append(f"Country {countryiso3} has many invalid pcodes!")
            process_adm = False
        for location in sorted(invalid_pcodes):
            warnings.append(f"Invalid pcode: {location[0]} - {location[1]}")
        return location_mapping, process_adm

    @staticmethod
    def get_cluster_mapping(data: Dict, monitor_json: MonitorJSON) -> Dict:
        cluster_mapping = {None: "ALL"}
        clusters = data["planGlobalClusters"]
        for cluster in clusters:
            cluster_code = cluster["globalClusterCode"]
            for plan_cluster_code in cluster["planClusters"]:
                if plan_cluster_code in cluster_mapping:
                    cluster_mapping[plan_cluster_code] = ""
                else:
                    cluster_mapping[plan_cluster_code] = cluster_code
        monitor_json.set_global_clusters(clusters)
        return cluster_mapping

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
        self,
        retriever: Retrieve,
        countryiso3: str,
        plan_id: str,
        monitor_json: MonitorJSON,
    ) -> Tuple[Optional[datetime], Optional[Dict]]:
        logger.info(f"Processing {countryiso3}")
        try:
            json = retriever.download_json(
                f"{self.hpc_url}plan/{plan_id}/responseMonitoring?includeCaseloadDisaggregation=true&includeIndicatorDisaggregation=false&disaggregationOnlyTotal=false",
            )
        except DownloadError as err:
            logger.exception(err)
            return None, None
        data = json["data"]

        errors = []
        warnings = []
        location_mapping, process_adm = self.get_location_mapping(
            countryiso3,
            data,
            monitor_json,
            errors,
            warnings,
        )
        cluster_mapping = self.get_cluster_mapping(data, monitor_json)

        rows = {}

        for caseload in data["caseloads"]:
            caseload_description = caseload["caseloadDescription"]
            entity_id = caseload["entityId"]
            sector_code = cluster_mapping.get(entity_id, "NO_SECTOR_CODE")
            if sector_code == "NO_SECTOR_CODE":
                warnings.append(
                    f"Unknown sector {caseload_description} ({entity_id})."
                )
                continue
            # HACKY CODE TO DEAL WITH DIFFERENT AORS UNDER PROTECTION
            if sector_code == "":
                description_lower = caseload_description.lower()
                if any(
                    x in description_lower
                    for x in ("child", "enfant", "niñez", "infancia")
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
                    if any(
                        x in description_lower for x in ("total", "overall")
                    ):
                        sector_code = "PRO"
                    elif any(
                        x in description_lower for x in ("general", "générale")
                    ):
                        continue
                    else:
                        warnings.append(
                            f"Mapping protection AOR {caseload_description} ({entity_id}) to PRO."
                        )
                        sector_code = "PRO"
                else:
                    warnings.append(
                        f"Unknown sector {caseload_description} ({entity_id})."
                    )
                    continue

            if sector_code == "ALL":
                sector_code_key = ""
            else:
                sector_code_key = sector_code
            national_row = {
                "Admin 1 PCode": "",
                "Admin 2 PCode": "",
                "Sector": sector_code,
                "Gender": "a",
                "Age Range": "ALL",
                "Min Age": "",
                "Max Age": "",
                "Disabled": "a",
                "Population Group": "ALL",
            }

            self.fill_population_status(national_row, caseload)

            # adm1, adm2, sector, gender, age_range, disabled, population group
            key = ("", "", sector_code_key, "a", "", "a", "ALL")
            rows[key] = national_row

            caseload_json = CaseloadJSON(caseload, monitor_json.save_test_data)
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
                    if (
                        self.pcodes_to_process
                        and pcode not in self.pcodes_to_process
                    ):
                        continue
                    if adminlevel == 1:
                        adm1 = pcode
                        adm2 = ""
                    elif adminlevel == 2:
                        adm1 = self.admintwo.pcode_to_parent.get(pcode, "")
                        if not adm1:
                            errors.append(
                                f"Cannot find parent pcode of {pcode}!"
                            )
                        adm2 = pcode
                    else:
                        continue
                caseload_json.add_disaggregated_attachment(attachment)
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
                    gender = "a"
                row["Gender"] = gender
                min_age = category_info.get("min_age")
                max_age = category_info.get("max_age")
                if min_age is None:
                    min_age = ""
                    if max_age is None:
                        max_age = ""
                        age_range = "ALL"
                    else:
                        age_range = f"0-{max_age}"
                elif max_age is None:
                    max_age = ""
                    age_range = f"{min_age}+"
                else:
                    age_range = f"{min_age}-{max_age}"
                if age_range == "ALL":
                    age_range_key = ""
                else:
                    age_range_key = age_range
                row["Age Range"] = age_range
                row["Min Age"] = min_age
                row["Max Age"] = max_age
                disabled = category_info.get("disabled", "a")
                row["Disabled"] = disabled
                population_group = category_info.get("group", "ALL")
                row["Population Group"] = population_group

                pop_data = {
                    x["metricType"]: x["value"]
                    for x in attachment["dataMatrix"]
                }
                self.fill_population_status(row, pop_data)

                # adm1, adm2, sector, gender, age_range, disabled, population group
                key = (
                    adm1,
                    adm2,
                    sector_code_key,
                    gender,
                    age_range_key,
                    disabled,
                    population_group,
                )
                existing_row = rows.get(key)
                if existing_row:
                    for key, value in row.items():
                        if value and not existing_row.get(key):
                            existing_row[key] = value
                else:
                    rows[key] = row
            monitor_json.add_caseload_json(caseload_json)

        for warning in dict.fromkeys(warnings):
            logger.warning(warning)
        for error in dict.fromkeys(errors):
            logger.error(error)
        monitor_json.save(plan_id)
        published = parse_date(data["lastPublishedDate"], "%d/%m/%Y")
        return published, rows

    def generate_dataset(
        self, countryiso3: str, rows: Dict, folder: str
    ) -> Optional[Dataset]:
        if not rows:
            return None
        countryname = Country.get_country_name_from_iso3(countryiso3)
        if countryname is None:
            logger.error(f"Unknown ISO 3 code {countryiso3}!")
            return None
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

        success, results = dataset.generate_resource_from_iterable(
            list(self.hxltags_narrow.keys()),
            (rows[key] for key in sorted(rows)),
            self.hxltags_narrow,
            folder,
            filename,
            resourcedata,
        )
        if success is False:
            logger.warning(f"{name} has no data!")
            return None
        return dataset
