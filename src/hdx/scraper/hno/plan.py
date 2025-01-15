import logging
from copy import copy
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .caseload_json import CaseloadJSON
from .monitor_json import MonitorJSON
from .progress_json import ProgressJSON
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.location.adminlevel import AdminLevel
from hdx.scraper.framework.utilities.reader import Read
from hdx.scraper.framework.utilities.sector import Sector
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.dateparse import parse_date

logger = logging.getLogger(__name__)


class Plan:
    def __init__(
        self,
        configuration: Configuration,
        year: int,
        error_handler: HDXErrorHandler,
        countryiso3s_to_process: str = "",
        pcodes_to_process: str = "",
    ) -> None:
        self._hpc_url = configuration["hpc_url"]
        self._max_admin = configuration["max_admin"]
        self._population_status_lookup = configuration["population_status"]
        self._year = year
        self._error_handler = error_handler
        if countryiso3s_to_process:
            self._countryiso3s_to_process = countryiso3s_to_process.split(",")
        else:
            self._countryiso3s_to_process = None
        if pcodes_to_process:
            self._pcodes_to_process = pcodes_to_process.split(",")
        else:
            self._pcodes_to_process = None
        self._sector = Sector()
        self._global_rows = {}
        self._highest_admin = {}

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

    def setup_admins(self):
        retriever = Read.get_reader()
        libhxl_12_dataset = AdminLevel.get_libhxl_dataset(
            retriever=retriever
        ).cache()
        libhxl_all_dataset = AdminLevel.get_libhxl_dataset(
            url=AdminLevel.admin_all_pcodes_url, retriever=retriever
        ).cache()
        libhxl_format_dataset = AdminLevel.get_libhxl_dataset(
            url=AdminLevel.formats_url, retriever=retriever
        ).cache()
        self._admins = []
        for i in range(self._max_admin):
            admin = AdminLevel(admin_level=i + 1, retriever=retriever)
            if admin.admin_level < 3:
                admin.setup_from_libhxl_dataset(
                    libhxl_dataset=libhxl_12_dataset,
                    countryiso3s=self._countryiso3s_to_process,
                )
            else:
                admin.setup_from_libhxl_dataset(
                    libhxl_dataset=libhxl_all_dataset,
                    countryiso3s=self._countryiso3s_to_process,
                )
            admin.load_pcode_formats_from_libhxl_dataset(libhxl_format_dataset)
            self._admins.append(admin)

    def get_location_mapping(
        self,
        countryiso3: str,
        data: Dict,
        monitor_json: MonitorJSON,
    ) -> Dict:
        location_mapping = {}
        for location in data["locations"]:
            adminlevel = location.get("adminLevel")
            if adminlevel == 0:
                admin = None
            elif adminlevel <= self._max_admin:
                admin = self._admins[adminlevel - 1]
            else:
                raise ValueError(
                    f"Admin level: {adminlevel} for {countryiso3} is not supported!"
                )
            if admin:
                pcode = location["pcode"].strip()
                if pcode not in admin.pcodes:
                    try:
                        pcode = admin.convert_admin_pcode_length(
                            countryiso3, pcode
                        )
                    except IndexError:
                        location["valid"] = "N"
                if pcode in admin.get_pcode_list():
                    location["pcode"] = pcode
                    location["valid"] = "Y"
                else:
                    location["valid"] = "N"
                if self._pcodes_to_process:
                    if pcode in self._pcodes_to_process:
                        monitor_json.add_location(location)
                else:
                    monitor_json.add_location(location)
            elif adminlevel == 0:
                location["valid"] = "Y"
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

    def fill_population_status(self, row: Dict, data: Dict) -> None:
        for input_key in self._population_status_lookup:
            header_tag = self._population_status_lookup[input_key]
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
            return None, None, 0
        data = json["data"]

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
            sector_orig = cluster_mapping.get(entity_id, "NO_SECTOR_CODE")
            if sector_orig == "NO_SECTOR_CODE":
                self._error_handler.add_message(
                    "HumanitarianNeeds",
                    "HPC",
                    f"caseload {caseload_description} ({entity_id}) unknown sector",
                    message_type="warning",
                )
                continue
            # HACKY CODE TO DEAL WITH DIFFERENT AORS UNDER PROTECTION
            if sector_orig == "":
                description_lower = caseload_description.lower()
                if any(
                    x in description_lower
                    for x in ("child", "enfant", "niñez", "infancia")
                ):
                    sector_orig = "PRO_CPM"
                elif any(
                    x in description_lower for x in ("housing", "logement")
                ):
                    sector_orig = "PRO_HLP"
                elif any(
                    x in description_lower
                    for x in ("gender", "genre", "género")
                ):
                    sector_orig = "PRO_GBV"
                elif any(x in description_lower for x in ("mine", "minas")):
                    sector_orig = "PRO_MIN"
                elif any(
                    x in description_lower
                    for x in ("protection", "protección")
                ):
                    if any(
                        x in description_lower for x in ("total", "overall")
                    ):
                        sector_orig = "PRO"
                    elif any(
                        x in description_lower for x in ("general", "générale")
                    ):
                        continue
                    else:
                        self._error_handler.add_message(
                            "HumanitarianNeeds",
                            "HPC",
                            f"caseload {caseload_description} ({entity_id}) mapped to PRO",
                            message_type="warning",
                        )
                        sector_orig = "PRO"
                else:
                    self._error_handler.add_message(
                        "HumanitarianNeeds",
                        "HPC",
                        f"caseload {caseload_description} ({entity_id}) unknown sector",
                        message_type="error",
                    )
                    continue

            sector_code = self._sector.get_sector_code(sector_orig)
            if not sector_code:
                self._error_handler.add_missing_value_message(
                    "HumanitarianNeeds",
                    "HPC",
                    "sector",
                    sector_orig,
                )
                continue
            if sector_code == "Intersectoral":
                sector_code_key = ""
            else:
                sector_code_key = sector_code
            national_row = {
                "Valid Location": "Y",
                "Sector": sector_code,
                "Category": "",
            }
            for i, adminlevel in enumerate(self._admins):
                national_row[f"Admin {i+1} PCode"] = ""
                national_row[f"Admin {i+1} Name"] = ""

            self.fill_population_status(national_row, caseload)

            # adm code, sector, category
            key = ("", sector_code_key, "")
            rows[key] = national_row
            global_row = copy(national_row)
            global_row["Country ISO3"] = countryiso3
            key = (countryiso3, "", "", sector_code_key, "")
            self._global_rows[key] = global_row

            caseload_json = CaseloadJSON(
                caseload, monitor_json._save_test_data
            )
            for attachment in caseload["disaggregatedAttachments"]:
                location_id = attachment["locationId"]
                location = location_mapping.get(location_id)
                if not location:
                    self._error_handler.add_message(
                        "HumanitarianNeeds",
                        "HPC",
                        f"caseload {caseload_description} ({entity_id}) unknown location {location_id} in {countryiso3}",
                        message_type="error",
                    )
                    continue
                adminlevel = location.get("adminLevel")
                adm_codes = ["" for _ in self._admins]
                adm_names = ["" for _ in self._admins]
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
                    for i in range(adminlevel - 1, 0, -1):
                        parent = self._admins[i].pcode_to_parent.get(pcode, "")
                        if not parent:
                            self._error_handler.add_message(
                                "HumanitarianNeeds",
                                "HPC",
                                f"no parent pcode of {pcode}",
                                message_type="error",
                            )
                        adm_codes[i - 1] = parent
                        pcode = parent

                caseload_json.add_disaggregated_attachment(attachment)

                category = attachment["categoryLabel"]
                row = {
                    "Valid Location": location["valid"],
                    "Sector": sector_code,
                    "Category": category,
                }
                for i, adm_code in enumerate(adm_codes):
                    adm_name = adm_names[i]
                    row[f"Admin {i+1} PCode"] = adm_code
                    row[f"Admin {i+1} Name"] = adm_name

                pop_data = {
                    x["metricType"]: x["value"]
                    for x in attachment["dataMatrix"]
                }
                self.fill_population_status(row, pop_data)

                # adm code, sector, category
                if adminlevel == 0:
                    adm_code = ""
                else:
                    adm_code = adm_codes[adminlevel - 1]
                key = (
                    adm_code,
                    sector_code_key,
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
                    sector_code_key,
                    category,
                )
                existing_row = self._global_rows.get(key)
                if existing_row:
                    for key, value in row.items():
                        if value and not existing_row.get(key):
                            existing_row[key] = value
                else:
                    global_row = copy(row)
                    global_row["Country ISO3"] = countryiso3
                    self._global_rows[key] = global_row

            monitor_json.add_caseload_json(caseload_json)

        self._highest_admin[countryiso3] = highest_admin
        monitor_json.save(plan_id)
        published = parse_date(data["lastPublishedDate"], "%d/%m/%Y")
        return published, rows

    def get_global_rows(self) -> Dict:
        return self._global_rows

    def get_highest_admin(self, countryiso3: str) -> Optional[int]:
        return self._highest_admin.get(countryiso3)

    def get_global_highest_admin(self) -> Optional[int]:
        return max(self._highest_admin.values(), default=None)
