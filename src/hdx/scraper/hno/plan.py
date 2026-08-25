import logging
from copy import deepcopy
from datetime import datetime

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.dateparse import parse_date

from . import graphql_reader, queries

logger = logging.getLogger(__name__)

# Dimensions making up a disaggregation "Category" label, in display order.
# See docs/decisions/0004-customreference-as-cluster-source.md context: the
# new API's own Name values for these dimensions don't match the old REST
# API's category-label vocabulary word-for-word (eg. "Y<18" here vs.
# "Children" in the old API) - this is an expected, real change in the
# published Category text, not a bug.
_CATEGORY_DIMENSIONS = (
    "ageGroup",
    "gender",
    "populationStatus",
    "settlementType",
    "disabilityStatus",
    "maternalStatus",
)


class Plan:
    def __init__(
        self,
        configuration: Configuration,
        year: int,
        error_handler: HDXErrorHandler,
        countryiso3s_to_process: list[str] | None = None,
        pcodes_to_process: list[str] | None = None,
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
        self._released_dates = {}

    def get_plan_ids_and_countries(self) -> list:
        items = graphql_reader.paginate(
            self._hpc_url,
            queries.PLAN_DISCOVERY_QUERY,
            {"year": self._year},
            connection_field="plans",
            filename_prefix=f"plans_{self._year}",
        )
        plan_ids_countries = []
        for plan in items:
            locations = plan["location"]["items"]
            if len(locations) != 1:
                continue
            countryiso3 = locations[0]["ISO3"]
            if (
                self._countryiso3s_to_process
                and countryiso3 not in self._countryiso3s_to_process
            ):
                continue
            plan_id = plan["Id"]
            self._released_dates[plan_id] = plan["ReleasedDate"]
            plan_ids_countries.append({"iso3": countryiso3, "id": plan_id})
        return sorted(plan_ids_countries, key=lambda x: x["iso3"])

    def fill_population_status_info(self, row: dict, data: dict) -> None:
        for input_key, key in self._population_status_lookup.items():
            row[key] = data.get(input_key, "")
        row["Info"] = "|".join(sorted(row["Info"]))

    @staticmethod
    def _cluster_code(attachment: dict) -> str:
        if attachment["EntityMainType"] == "Plan":
            return "ALL"
        # CustomReference is "<order>-<code>" eg. "3-PRO-CPN" -> "PRO-CPN"
        _, _, code = (attachment["CustomReference"] or "").partition("-")
        return code

    @staticmethod
    def _category_label(fact: dict) -> str:
        parts = [fact[dim]["Name"] for dim in _CATEGORY_DIMENSIONS if fact.get(dim)]
        return " - ".join(parts)

    def process(
        self,
        countryiso3: str,
        plan_id: int,
    ) -> tuple[datetime | None, dict | None]:
        logger.info(f"Processing {countryiso3}")
        try:
            attachments_data = graphql_reader.execute_query(
                self._hpc_url,
                queries.CASELOAD_ATTACHMENTS_QUERY,
                {"planId": plan_id, "first": 50},
                filename=f"attachments_{plan_id}.json",
            )
            facts = graphql_reader.paginate(
                self._hpc_url,
                queries.CASELOAD_FACTS_QUERY,
                {"planId": plan_id},
                connection_field="attachmentFacts",
                filename_prefix=f"attachmentfacts_{plan_id}",
            )
        except (DownloadError, graphql_reader.GraphQLError) as err:
            logger.exception(err)
            return None, None

        attachments = {a["Id"]: a for a in attachments_data["attachments"]["items"]}
        if not attachments:
            return None, None

        # Reassemble one row per (attachment, location, category) group from the
        # per-metric fact rows the new API returns, and separately collect each
        # attachment's IsTotal rows into a flat {HPCType: value} dict matching
        # the shape fill_population_status_info expects (mirrors the old REST
        # API's flat per-caseload totals and dataMatrix-per-category, which
        # this API instead expresses as individual AttachmentFact rows).
        totals: dict[int, dict] = {}
        groups: dict[tuple, dict] = {}
        highest_admin = 0
        for fact in facts:
            attachment_id = fact["AttachmentId"]
            attachment = attachments.get(attachment_id)
            if not attachment:
                continue
            hpc_type = fact["metricType"]["HPCType"]
            value = fact["ValueNum"]
            if fact["IsTotal"]:
                totals.setdefault(attachment_id, {})[hpc_type] = value
                continue
            if not attachment["HasDisaggregatedData"]:
                continue
            location = fact.get("location")
            if location is None and fact.get("LocationId") is not None:
                self._error_handler.add_message(
                    "HumanitarianNeeds",
                    "HPC",
                    f"unknown location {fact['LocationId']} in {countryiso3}",
                    message_type="error",
                )
                continue
            adminlevel = location["AdminLevel"] if location else 0
            if adminlevel > self._max_admin:
                raise ValueError(
                    f"Admin level: {adminlevel} for {countryiso3} is not supported!"
                )
            pcode = location["Pcode"].strip() if location and adminlevel != 0 else ""
            if (
                adminlevel != 0
                and self._pcodes_to_process
                and pcode not in self._pcodes_to_process
            ):
                continue
            if adminlevel > highest_admin:
                highest_admin = adminlevel
            category = self._category_label(fact)
            group_key = (attachment_id, fact.get("LocationId"), category)
            group = groups.setdefault(
                group_key,
                {
                    "adminlevel": adminlevel,
                    "pcode": pcode,
                    "name": location["Name"] if location and adminlevel != 0 else "",
                    "category": category,
                    "metrics": {},
                },
            )
            group["metrics"][hpc_type] = value

        rows = {}
        for attachment_id, attachment in attachments.items():
            cluster = self._cluster_code(attachment)
            caseload_description = attachment["Name"] or ""
            base_row = {
                "Category": "",
                "Description": caseload_description,
                "Info": set(),
            }
            if not cluster:
                self._error_handler.add_message(
                    "HumanitarianNeeds",
                    "HPC",
                    f"caseload {caseload_description} no cluster for attachment {attachment_id} in {countryiso3}",
                    message_type="warning",
                )
                base_row["Info"].add(f"No cluster for attachment {attachment_id}")

            base_row["Cluster"] = cluster
            national_row = deepcopy(base_row)
            for i in range(self._max_admin):
                national_row[f"Admin {i + 1} PCode"] = ""
                national_row[f"Admin {i + 1} Name"] = ""
            self.fill_population_status_info(
                national_row, totals.get(attachment_id, {})
            )
            key = ("", cluster, caseload_description, "")
            rows[key] = national_row
            global_row = deepcopy(national_row)
            global_row["Country ISO3"] = countryiso3
            key = (countryiso3, "", cluster, caseload_description, "")
            self._global_rows[key] = global_row

        for (attachment_id, _location_id, category), group in groups.items():
            attachment = attachments[attachment_id]
            cluster = self._cluster_code(attachment)
            caseload_description = attachment["Name"] or ""
            row = {
                "Category": category,
                "Description": caseload_description,
                "Info": set(),
                "Cluster": cluster,
            }
            for i in range(self._max_admin):
                row[f"Admin {i + 1} PCode"] = ""
                row[f"Admin {i + 1} Name"] = ""
            adminlevel = group["adminlevel"]
            if adminlevel != 0:
                row[f"Admin {adminlevel} PCode"] = group["pcode"]
                row[f"Admin {adminlevel} Name"] = group["name"]
            self.fill_population_status_info(row, group["metrics"])

            adm_code = group["pcode"] if adminlevel != 0 else ""
            key = (adm_code, cluster, caseload_description, category)
            existing_row = rows.get(key)
            if existing_row:
                for row_key, value in row.items():
                    if value and not existing_row.get(row_key):
                        existing_row[row_key] = value
            else:
                rows[key] = row
            key = (countryiso3, adm_code, cluster, caseload_description, category)
            existing_row = self._global_rows.get(key)
            if existing_row:
                for row_key, value in row.items():
                    if value and not existing_row.get(row_key):
                        existing_row[row_key] = value
            else:
                global_row = deepcopy(row)
                global_row["Country ISO3"] = countryiso3
                self._global_rows[key] = global_row

        self._highest_admin[countryiso3] = highest_admin
        released_date = self._released_dates.get(plan_id)
        published = parse_date(released_date) if released_date else None
        return published, rows

    def get_global_rows(self) -> dict:
        return self._global_rows

    def get_highest_admin(self, countryiso3: str) -> int | None:
        return self._highest_admin.get(countryiso3)

    def get_global_highest_admin(self) -> int | None:
        return max(self._highest_admin.values(), default=None)
