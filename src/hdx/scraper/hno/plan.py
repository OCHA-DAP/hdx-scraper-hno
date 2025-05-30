import logging
from copy import deepcopy
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .caseload_json import CaseloadJSON
from .mappings import get_category_mapping
from .monitor_json import MonitorJSON
from .progress_json import ProgressJSON
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.scraper.framework.utilities.reader import Read
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.dateparse import parse_date

logger = logging.getLogger(__name__)


class Plan:
    def __init__(
        self,
        configuration: Configuration,
        year: int,
        error_handler: HDXErrorHandler,
        countryiso3s_to_process: Optional[List[str]] = None,
        pcodes_to_process: Optional[List[str]] = None,
    ) -> None:
        self._hpc_url = configuration["hpc_url"]
        self._max_admin = configuration["max_admin"]
        self._population_status_lookup = configuration["population_status"]
        self._year = year
        self._error_handler = error_handler
        self._countryiso3s_to_process = countryiso3s_to_process
        self._pcodes_to_process = pcodes_to_process
        self._global_rows = {}
        self._highest_admin = {}
        self._category_mapping = get_category_mapping()
        self._used_category_mappings = set()

    def get_plan_ids_and_countries(self, progress_json: ProgressJSON) -> List:
        json = Read.get_reader("hpc_basic").download_json(
            f"{self._hpc_url}fts/flow/plan/overview/progress/{self._year}"
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
                self._countryiso3s_to_process
                and countryiso3 not in self._countryiso3s_to_process
            ):
                continue
            plan["caseLoads"] = []
            progress_json.add_plan(plan)
            plan_ids_countries.append({"iso3": countryiso3, "id": plan_id})
        progress_json.save()
        return sorted(plan_ids_countries, key=lambda x: x["iso3"])

    def get_location_mapping(
        self,
        countryiso3: str,
        data: Dict,
        monitor_json: MonitorJSON,
    ) -> Dict:
        location_mapping = {}
        for location in data["locations"]:
            adminlevel = location.get("adminLevel")
            if adminlevel > self._max_admin:
                raise ValueError(
                    f"Admin level: {adminlevel} for {countryiso3} is not supported!"
                )
            if adminlevel >= 1:
                pcode = location["pcode"].strip()
                if self._pcodes_to_process:
                    if pcode in self._pcodes_to_process:
                        monitor_json.add_location(location)
                else:
                    monitor_json.add_location(location)
            elif adminlevel == 0:
                monitor_json.add_location(location)
            location_mapping[location["id"]] = location
        return location_mapping

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

    def fill_population_status_info(self, row: Dict, data: Dict) -> None:
        for input_key in self._population_status_lookup:
            header_tag = self._population_status_lookup[input_key]
            key = header_tag["header"]
            row[key] = data.get(input_key, "")
        row["Info"] = "|".join(sorted(row["Info"]))

    def process(
        self,
        countryiso3: str,
        plan_id: str,
        monitor_json: MonitorJSON,
    ) -> Tuple[Optional[datetime], Optional[Dict]]:
        logger.info(f"Processing {countryiso3}")
        try:
            json = Read.get_reader("hpc_bearer").download_json(
                f"{self._hpc_url}plan/{plan_id}/responseMonitoring?includeCaseloadDisaggregation=true&includeIndicatorDisaggregation=false&disaggregationOnlyTotal=false",
            )
        except DownloadError as err:
            logger.exception(err)
            return None, None
        data = json["data"]

        publish_disaggregated = False
        last_published_version = data["lastPublishedVersion"]
        last_published_date = data["lastPublishedDate"]
        monitor_json.set_last_published(last_published_version, last_published_date)
        if float(last_published_version) >= 1:
            publish_disaggregated = True

        location_mapping = self.get_location_mapping(
            countryiso3,
            data,
            monitor_json,
        )
        cluster_mapping = self.get_cluster_mapping(data, monitor_json)

        rows = {}
        highest_admin = 0
        for caseload in data["caseloads"]:
            caseload_description = caseload["caseloadDescription"]
            entity_id = caseload["entityId"]
            cluster = cluster_mapping.get(entity_id, "NO_CLUSTER_CODE")
            if cluster != "ALL" and publish_disaggregated is False:
                continue
            base_row = {
                "Category": "",
                "Description": caseload_description,
                "Info": set(),
            }

            # No cluster code provided
            if cluster == "NO_CLUSTER_CODE":
                cluster = ""
                self._error_handler.add_message(
                    "HumanitarianNeeds",
                    "HPC",
                    f"caseload {caseload_description} no cluster for entity {entity_id} in {countryiso3}",
                    message_type="warning",
                )
                base_row["Info"].add(f"No cluster for entity {entity_id}")
            # HACKY CODE TO DEAL WITH DIFFERENT AORS UNDER PROTECTION
            elif cluster == "":
                description_lower = caseload_description.lower()
                if any(
                    x in description_lower
                    for x in ("child", "enfant", "niñez", "infancia")
                ):
                    cluster = "PRO-CPN"
                elif any(x in description_lower for x in ("housing", "logement")):
                    cluster = "PRO-HLP"
                elif any(
                    x in description_lower for x in ("gender", "genre", "género", "gbv")
                ):
                    cluster = "PRO-GBV"
                elif any(x in description_lower for x in ("mine", "minas")):
                    cluster = "PRO-MIN"
                elif any(x in description_lower for x in ("protection", "protección")):
                    if any(
                        x in description_lower
                        for x in ("total", "overall", "general", "générale")
                    ):
                        cluster = "PRO"
                    else:
                        cluster = "PRO"
                        self._error_handler.add_message(
                            "HumanitarianNeeds",
                            "HPC",
                            f"caseload {caseload_description} ({entity_id}) mapped to PRO in {countryiso3}",
                            message_type="warning",
                        )
                else:
                    cluster = ""
                    self._error_handler.add_message(
                        "HumanitarianNeeds",
                        "HPC",
                        f"caseload {caseload_description} ({entity_id}) unknown cluster in {countryiso3}",
                        message_type="error",
                    )
                    base_row["Info"].add(f"No cluster for {caseload_description}")

            base_row["Cluster"] = cluster
            national_row = deepcopy(base_row)
            for i in range(self._max_admin):
                national_row[f"Admin {i + 1} PCode"] = ""
                national_row[f"Admin {i + 1} Name"] = ""

            self.fill_population_status_info(national_row, caseload)

            # adm code, cluster, caseload_description, category
            key = ("", cluster, caseload_description, "")
            rows[key] = national_row
            global_row = deepcopy(national_row)
            global_row["Country ISO3"] = countryiso3
            key = (countryiso3, "", cluster, caseload_description, "")
            self._global_rows[key] = global_row

            caseload_json = CaseloadJSON(caseload, monitor_json._save_test_data)
            if publish_disaggregated:
                for attachment in caseload["disaggregatedAttachments"]:
                    row = deepcopy(base_row)
                    location_id = attachment["locationId"]
                    location = location_mapping.get(location_id)
                    adm_codes = ["" for _ in range(self._max_admin)]
                    adm_names = ["" for _ in range(self._max_admin)]
                    if location:
                        adminlevel = location.get("adminLevel")
                        if adminlevel != 0:
                            pcode = location["pcode"]
                            if (
                                self._pcodes_to_process
                                and pcode not in self._pcodes_to_process
                            ):
                                continue
                            if adminlevel > highest_admin:
                                highest_admin = adminlevel
                            name = location["name"]
                            adm_codes[adminlevel - 1] = pcode
                            adm_names[adminlevel - 1] = name
                            caseload_json.add_disaggregated_attachment(attachment)
                    else:
                        adminlevel = 0
                        self._error_handler.add_message(
                            "HumanitarianNeeds",
                            "HPC",
                            f"caseload {caseload_description} ({entity_id}) unknown location {location_id} in {countryiso3}",
                            message_type="error",
                        )
                        row["Info"].add(f"Unknown location {location_id}")

                    for i, adm_code in enumerate(adm_codes):
                        adm_name = adm_names[i]
                        row[f"Admin {i + 1} PCode"] = adm_code
                        row[f"Admin {i + 1} Name"] = adm_name

                    category_name = attachment["categoryName"].lower()
                    result = self._category_mapping.get(category_name)
                    if result:
                        self._used_category_mappings.add(result)
                    else:
                        self._error_handler.add_message(
                            "HumanitarianNeeds",
                            "HPC",
                            f"caseload {caseload_description} ({entity_id}) unknown category {category_name} in {countryiso3}",
                            message_type="error",
                        )
                        row["Info"].add(f"{category_name} not found")
                    category = attachment["categoryLabel"]
                    row["Category"] = category

                    pop_data = {
                        x["metricType"]: x["value"] for x in attachment["dataMatrix"]
                    }
                    self.fill_population_status_info(row, pop_data)

                    # adm code, cluster, description, category
                    if adminlevel == 0:
                        adm_code = ""
                    else:
                        adm_code = adm_codes[adminlevel - 1]
                    key = (
                        adm_code,
                        cluster,
                        caseload_description,
                        category,
                    )
                    existing_row = rows.get(key)
                    if existing_row:
                        for key, value in row.items():
                            if value and not existing_row.get(key):
                                existing_row[key] = value
                    else:
                        rows[key] = row
                    key = (
                        countryiso3,
                        adm_code,
                        cluster,
                        caseload_description,
                        category,
                    )
                    existing_row = self._global_rows.get(key)
                    if existing_row:
                        for key, value in row.items():
                            if value and not existing_row.get(key):
                                existing_row[key] = value
                    else:
                        global_row = deepcopy(row)
                        global_row["Country ISO3"] = countryiso3
                        self._global_rows[key] = global_row

            monitor_json.add_caseload_json(caseload_json)

        self._highest_admin[countryiso3] = highest_admin
        monitor_json.save(plan_id)
        published = parse_date(last_published_date, "%d/%m/%Y")
        return published, rows

    def get_global_rows(self) -> Dict:
        return self._global_rows

    def get_highest_admin(self, countryiso3: str) -> Optional[int]:
        return self._highest_admin.get(countryiso3)

    def get_global_highest_admin(self) -> Optional[int]:
        return max(self._highest_admin.values(), default=None)

    def get_used_category_mappings(self) -> List:
        used_category_mappings = [("categoryName", "Gender", "Age", "Disability",
                                        "Population Group")]
        used_category_mappings.extend(sorted(self._used_category_mappings))
        return used_category_mappings
