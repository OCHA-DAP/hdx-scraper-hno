import logging
from datetime import UTC, datetime
from os.path import join

from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.utilities.dateparse import parse_date
from hdx.utilities.path import temp_dir
from pytest_check import check

from hdx.scraper.hno import graphql_reader
from hdx.scraper.hno.plan import Plan

logger = logging.getLogger(__name__)


class TestHumanitarianNeeds:
    """Exercises Plan against real captured GraphQL fixtures (Afghanistan and
    Sudan HNO 2025, PlanIds 1263/1220) via the standard save=False/use_saved=True
    Retrieve replay pattern - see docs/decisions/0001-migrate-to-fabric-graphql-api.md.

    Facts are paginated per attachment (sector), not per plan - see
    docs/decisions/0001-migrate-to-fabric-graphql-api.md's note on the
    combined-stream pagination-scale problem this replaced. Each
    attachmentfacts_{plan_id}_{attachment_id}_p1.json fixture is one real
    captured page (first=15) for that attachment, with hasNextPage/endCursor
    forced to false/null for compact, deterministic replay - everything else
    in each fixture is an unmodified real API response. Row counts/values
    below are real output from that data, not fabricated.

    NOTE: this only covers the migrated HPC-fetch layer (Plan). The downstream
    dataset-generation/HAPI-output re-baselining lives in
    test_downstream_2025.py.
    """

    def test_get_plan_ids_and_countries_and_process(self, configuration):
        with HDXErrorHandler() as error_handler:
            with temp_dir(
                "TestHNO",
                delete_on_success=True,
                delete_on_failure=False,
            ) as tempdir:
                today = parse_date("25/08/2026", "%d/%m/%Y")
                input_dir = join("tests", "fixtures", "input")
                graphql_reader.create_readers(
                    tempdir,
                    input_dir,
                    tempdir,
                    False,
                    True,
                    hdx_auth="test",
                    hpc_subscription_key="test",
                    today=today,
                )
                plan = Plan(
                    configuration,
                    2025,
                    error_handler,
                    ["AFG", "SDN"],
                )
                plan_ids_countries = plan.get_plan_ids_and_countries()
                check.equal(
                    plan_ids_countries,
                    [
                        {"iso3": "AFG", "id": 1263},
                        {"iso3": "SDN", "id": 1220},
                    ],
                )

                published, rows = plan.process("AFG", 1263)
                check.equal(published, datetime(2024, 12, 19, 0, 0, tzinfo=UTC))
                check.equal(len(rows), 211)
                highest_admin = plan.get_highest_admin("AFG")
                check.equal(highest_admin, 1)
                key_value_pairs = list(rows.items())
                key, value = key_value_pairs[0]
                check.equal(key, ("", "ALL", "Final HRP caseload", ""))
                check.equal(
                    value,
                    {
                        "Category": "",
                        "Description": "Final HRP caseload",
                        "Info": "",
                        "Cluster": "ALL",
                        "Admin 1 PCode": "",
                        "Admin 1 Name": "",
                        "Admin 2 PCode": "",
                        "Admin 2 Name": "",
                        "Admin 3 PCode": "",
                        "Admin 3 Name": "",
                        "Admin 4 PCode": "",
                        "Admin 4 Name": "",
                        "Admin 5 PCode": "",
                        "Admin 5 Name": "",
                        "Population": "",
                        "In Need": "",
                        "Targeted": "",
                        "Affected": "",
                        "Reached": "",
                    },
                )
                key, value = key_value_pairs[45]
                check.equal(
                    key,
                    (
                        "AF01",
                        "EDU",
                        "Final HRP caseload",
                        "Y<18 - Male - Internally Displaced Persons",
                    ),
                )
                check.equal(
                    value,
                    {
                        "Category": "Y<18 - Male - Internally Displaced Persons",
                        "Description": "Final HRP caseload",
                        "Info": "",
                        "Cluster": "EDU",
                        "Admin 1 PCode": "AF01",
                        "Admin 1 Name": "Kabul",
                        "Admin 2 PCode": "",
                        "Admin 2 Name": "",
                        "Admin 3 PCode": "",
                        "Admin 3 Name": "",
                        "Admin 4 PCode": "",
                        "Admin 4 Name": "",
                        "Admin 5 PCode": "",
                        "Admin 5 Name": "",
                        "Population": "",
                        "In Need": 0.0,
                        "Targeted": "",
                        "Affected": "",
                        "Reached": "",
                    },
                )
                key, value = key_value_pairs[210]
                check.equal(
                    key,
                    (
                        "AF01",
                        "PRO-HLP",
                        "Housing, Land and Property",
                        "Y<18 - Male - Internally Displaced Persons",
                    ),
                )
                check.equal(value["In Need"], 5.0)

                published, rows = plan.process("SDN", 1220)
                check.equal(published, datetime(2024, 12, 31, 0, 0, tzinfo=UTC))
                check.equal(len(rows), 224)
                highest_admin = plan.get_highest_admin("SDN")
                check.equal(highest_admin, 2)
                key_value_pairs = list(rows.items())
                key, value = key_value_pairs[0]
                check.equal(key, ("", "EDU", "Education", ""))
                key, value = key_value_pairs[45]
                check.equal(
                    key,
                    ("SD01001", "HEA", "Health", "Host Community"),
                )
                check.equal(
                    value,
                    {
                        "Category": "Host Community",
                        "Description": "Health",
                        "Info": "",
                        "Cluster": "HEA",
                        "Admin 1 PCode": "",
                        "Admin 1 Name": "",
                        "Admin 2 PCode": "SD01001",
                        "Admin 2 Name": "Jebel Awlia",
                        "Admin 3 PCode": "",
                        "Admin 3 Name": "",
                        "Admin 4 PCode": "",
                        "Admin 4 Name": "",
                        "Admin 5 PCode": "",
                        "Admin 5 Name": "",
                        "Population": "",
                        "In Need": 14524.0,
                        "Targeted": "",
                        "Affected": "",
                        "Reached": "",
                    },
                )
                key, value = key_value_pairs[223]
                check.equal(
                    key,
                    ("SD02120", "RR", "Refugee Response", "Female"),
                )
                check.equal(value["In Need"], 0.0)

                global_rows = plan.get_global_rows()
                check.equal(len(global_rows), 435)
                check.equal(error_handler.shared_errors["error"], {})
