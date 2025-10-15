import logging
from copy import deepcopy
from datetime import datetime
from typing import Dict, List, Optional

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.scraper.framework.utilities.hapi_admins import complete_admins
from hdx.scraper.framework.utilities.reader import Read
from hdx.scraper.framework.utilities.sector import Sector
from hdx.utilities.dateparse import iso_string_from_datetime
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import get_numeric_if_possible

logger = logging.getLogger(__name__)


class HAPIOutput:
    def __init__(
        self,
        configuration: Configuration,
        year: int,
        error_handler: HDXErrorHandler,
        slugified_name: str,
        countryiso3s_to_process: Optional[List[str]] = None,
    ) -> None:
        self._max_admin = configuration["max_admin"]
        self._population_status_mapping = configuration["population_status_mapping"]
        time_period_start = datetime(year, 1, 1)
        time_period_end = datetime(year, 12, 31, 23, 59, 59)
        self.start_date = iso_string_from_datetime(time_period_start)
        self.end_date = iso_string_from_datetime(time_period_end)
        self._error_handler = error_handler
        self._slugified_name = slugified_name
        self._countryiso3s_to_process = countryiso3s_to_process
        self._sector = Sector()
        self._negative_values_by_iso3 = {}
        self._rounded_values_by_iso3 = {}
        self._global_rows = {}

    def setup_admins(self):
        retriever = Read.get_reader()
        libhxl_12_dataset = AdminLevel.get_libhxl_dataset(retriever=retriever).cache()
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

    def process(
        self,
        countryiso3: str,
        rows: Dict,
    ) -> None:
        logger.info("Processing HAPI output")
        for key, row in rows.items():
            ignore = False
            for i in range(self._max_admin, 2, -1):
                value = row.get(f"Admin {i} PCode", row.get(f"Admin {i} Name"))
                if value:
                    ignore = True
                    self._error_handler.add_message(
                        "HumanitarianNeeds",
                        self._slugified_name,
                        f"admin {i}: {value} ignored",
                        message_type="warning",
                    )
                    break
            if ignore:
                continue
            admcode, cluster, caseload_description, category = key
            base_hapi_row = {
                "warning": set(),
                "error": set(),
            }
            provider_adm_names = [row["Admin 1 Name"], row["Admin 2 Name"]]
            adm_codes = [row["Admin 1 PCode"], row["Admin 2 PCode"]]
            adm_names = ["", ""]
            adm_level, warnings = complete_admins(
                self._admins,
                countryiso3,
                provider_adm_names,
                adm_codes,
                adm_names,
            )
            for warning in warnings:
                self._error_handler.add_message(
                    "HumanitarianNeeds",
                    self._slugified_name,
                    warning,
                    message_type="warning",
                )
                base_hapi_row["warning"].add(warning)

            base_hapi_row["location_code"] = countryiso3
            base_hapi_row["has_hrp"] = (
                "Y" if Country.get_hrp_status_from_iso3(countryiso3) else "N"
            )
            base_hapi_row["in_gho"] = (
                "Y" if Country.get_gho_status_from_iso3(countryiso3) else "N"
            )
            base_hapi_row["provider_admin1_name"] = provider_adm_names[0]
            base_hapi_row["provider_admin2_name"] = provider_adm_names[1]
            base_hapi_row["admin1_code"] = adm_codes[0]
            base_hapi_row["admin1_name"] = adm_names[0]
            base_hapi_row["admin2_code"] = adm_codes[1]
            base_hapi_row["admin2_name"] = adm_names[1]
            base_hapi_row["admin_level"] = adm_level
            if cluster:
                sector_code = self._sector.get_code(cluster)
                if sector_code:
                    base_hapi_row["sector_code"] = sector_code
                    base_hapi_row["sector_name"] = self._sector.get_name(
                        sector_code, ""
                    )
                    if sector_code == "Intersectoral":
                        sector_code_key = ""
                    else:
                        sector_code_key = sector_code
                else:
                    base_hapi_row["sector_code"] = ""
                    base_hapi_row["sector_name"] = ""
                    self._error_handler.add_missing_value_message(
                        "HumanitarianNeeds",
                        self._slugified_name,
                        "cluster",
                        cluster,
                    )
                    base_hapi_row["error"].add(f"No cluster mapping for {cluster}")
                    sector_code_key = f"ZZY: {cluster}"
            else:
                sector_code_key = f"ZZZ: {caseload_description}"

            base_hapi_row["category"] = category

            for (
                header,
                population_status,
            ) in self._population_status_mapping.items():
                value = row.get(header)
                if value:
                    hapi_row = deepcopy(base_hapi_row)
                    hapi_row["population_status"] = population_status
                    value = get_numeric_if_possible(value)
                    if value < 0:
                        dict_of_lists_add(
                            self._negative_values_by_iso3,
                            countryiso3,
                            str(value),
                        )
                        value = ""
                        hapi_row["error"].add("Negative value")
                    elif isinstance(value, float):
                        dict_of_lists_add(
                            self._rounded_values_by_iso3,
                            countryiso3,
                            str(value),
                        )
                        value = round(value)
                        hapi_row["warning"].add("Rounded value")
                    hapi_row["population"] = value
                    hapi_row["reference_period_start"] = self.start_date
                    hapi_row["reference_period_end"] = self.end_date
                    hapi_row["warning"] = "|".join(sorted(hapi_row["warning"]))
                    errors = "|".join(sorted(hapi_row["error"]))
                    if row["Info"]:
                        if errors:
                            errors = f"{row['Info']}|{errors}"
                        else:
                            errors = row["Info"]
                    hapi_row["error"] = errors
                    key = (
                        countryiso3,
                        provider_adm_names[0],
                        provider_adm_names[1],
                        adm_codes[0],
                        adm_codes[1],
                        sector_code_key,
                        category,
                        population_status,
                    )
                    self._global_rows[key] = hapi_row

    def add_negative_rounded_errors(
        self, resource_name: str, dataset_name: str
    ) -> None:
        for countryiso3, values in self._negative_values_by_iso3.items():
            self._error_handler.add_multi_valued_message(
                "HumanitarianNeeds",
                dataset_name,
                f"negative population value(s) removed in {countryiso3}",
                values,
                resource_name=resource_name,
                err_to_hdx=True,
            )
        for countryiso3, values in self._rounded_values_by_iso3.items():
            self._error_handler.add_multi_valued_message(
                "HumanitarianNeeds",
                "HPC",
                f"population value(s) rounded in {countryiso3}",
                values,
                message_type="warning",
            )

    def get_global_rows(self) -> Dict:
        return self._global_rows
