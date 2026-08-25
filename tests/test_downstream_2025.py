"""Re-baselined downstream test (item 4 of the GraphQL migration follow-ups):
exercises dataset_generator.py/hapi_output.py/hapi_dataset_generator.py -
none of which changed in the migration - against the real 2025 Afghanistan/
Sudan rows Plan now produces from the captured GraphQL fixtures. CSV fixtures
here are real output from this pipeline, generated once and reviewed for
sanity, then checked in as the comparison baseline (the same bootstrapping
any first-write of a byte-exact comparison test requires).

This uses the same small (2-page) fixture sample as test_main.py, so absolute
row/dataset counts are modest - the point is exercising the real code path
end-to-end, not matching the old pipeline's full-scale row counts.
"""

from os.path import join

from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.path import temp_dir
from pytest_check import check

from hdx.scraper.hno import graphql_reader
from hdx.scraper.hno.dataset_generator import DatasetGenerator
from hdx.scraper.hno.hapi_dataset_generator import HAPIDatasetGenerator
from hdx.scraper.hno.hapi_output import HAPIOutput
from hdx.scraper.hno.plan import Plan
from hdx.scraper.hno.timeperiod_helper import TimePeriodHelper


class TestDownstreamGeneration:
    def test_dataset_and_hapi_generation(self, configuration):
        fixtures_dir = join("tests", "fixtures")
        input_dir = join(fixtures_dir, "input")
        with HDXErrorHandler() as error_handler:
            with temp_dir(
                "TestDownstream2025", delete_on_success=True, delete_on_failure=False
            ) as tempdir:
                today = parse_date("25/08/2026", "%d/%m/%Y")
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
                plan = Plan(configuration, 2025, error_handler, ["AFG", "SDN"])
                plan_ids_countries = plan.get_plan_ids_and_countries()
                timeperiod_helper = TimePeriodHelper(configuration, 2025)
                dataset_generator = DatasetGenerator(configuration, timeperiod_helper)
                hapi_output = HAPIOutput(
                    configuration,
                    timeperiod_helper,
                    error_handler,
                    dataset_generator.global_name,
                    ["AFG", "SDN"],
                )
                hapi_output.setup_admins()

                countries_with_data = []
                highest_admin_by_country = {}
                for plan_id_country in plan_ids_countries:
                    countryiso3 = plan_id_country["iso3"]
                    plan_id = plan_id_country["id"]
                    published, rows = plan.process(countryiso3, plan_id)
                    hapi_output.process(countryiso3, rows)
                    countries_with_data.append(countryiso3)
                    highest_admin = plan.get_highest_admin(countryiso3)
                    highest_admin_by_country[countryiso3] = highest_admin
                    dataset = dataset_generator.generate_country_dataset(
                        countryiso3, tempdir, rows, highest_admin
                    )
                    if countryiso3 == "AFG":
                        check.equal(dataset["name"], "afghanistan-humanitarian-needs")
                        check.equal(dataset["title"], "Afghanistan: Humanitarian Needs")
                        check.equal(
                            [r["name"] for r in dataset.get_resources()],
                            ["afg_hpc_needs_api_2025.csv"],
                        )
                        assert_files_same(
                            join(fixtures_dir, "afg_hpc_needs_api_2025.csv"),
                            join(tempdir, "afg_hpc_needs_api_2025.csv"),
                        )
                    elif countryiso3 == "SDN":
                        check.equal(dataset["name"], "sudan-humanitarian-needs")
                        check.equal(dataset["title"], "Sudan: Humanitarian Needs")
                        check.equal(
                            [r["name"] for r in dataset.get_resources()],
                            ["sdn_hpc_needs_api_2025.csv"],
                        )
                        assert_files_same(
                            join(fixtures_dir, "sdn_hpc_needs_api_2025.csv"),
                            join(tempdir, "sdn_hpc_needs_api_2025.csv"),
                        )

                check.equal(countries_with_data, ["AFG", "SDN"])

                global_rows = plan.get_global_rows()
                global_highest_admin = plan.get_global_highest_admin()
                dataset, resource = dataset_generator.generate_global_dataset(
                    tempdir, global_rows, countries_with_data, global_highest_admin
                )
                check.equal(
                    dataset,
                    {
                        "data_update_frequency": "365",
                        "dataset_date": "[2025-01-01T00:00:00 TO 2025-12-08T23:59:59]",
                        "groups": [{"name": "afg"}, {"name": "sdn"}],
                        "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                        "name": "global-hpc-hno",
                        "owner_org": "49f12a06-1605-4f98-89f1-eaec37a0fdfe",
                        "subnational": "1",
                        "tags": [
                            {
                                "name": "humanitarian needs overview-hno",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "people in need-pin",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                        ],
                        "title": "Global Humanitarian Programme Cycle, Humanitarian Needs",
                    },
                )
                assert_files_same(
                    join(fixtures_dir, "hpc_hno_2025.csv"),
                    join(tempdir, "hpc_hno_2025.csv"),
                )

                hapi_output.add_negative_rounded_errors(
                    "global-hpc-hno", "Global HPC HNO 2025"
                )
                hapi_global_rows = hapi_output.get_global_rows()
                check.equal(len(hapi_global_rows), 48)
                key_value_pairs = list(hapi_global_rows.items())
                key, value = key_value_pairs[0]
                check.equal(
                    key,
                    (
                        "AFG",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "Y<18 - Male - Internally Displaced Persons",
                        "all",
                    ),
                )
                check.equal(value["population"], 53557)
                check.equal(value["sector_code"], "Intersectoral")
                key, value = key_value_pairs[-1]
                check.equal(
                    key,
                    (
                        "SDN",
                        "",
                        "Um Dafoug",
                        "SD03",
                        "SD03146",
                        "EDU",
                        "Host Community",
                        "INN",
                    ),
                )
                check.equal(value["population"], 1303)
                check.equal(value["sector_name"], "Education")

                hapi_dataset_generator = HAPIDatasetGenerator(
                    configuration,
                    timeperiod_helper,
                    hapi_global_rows,
                    countries_with_data,
                )
                hapi_dataset = hapi_dataset_generator.generate_needs_dataset(
                    tempdir,
                    countries_with_data,
                    "test-dataset-id",
                    "test-resource-id",
                    None,
                )
                check.equal(hapi_dataset["name"], "hdx-hapi-humanitarian-needs")
                check.equal(
                    [r["name"] for r in hapi_dataset.get_resources()],
                    ["Global Affected People: Humanitarian Needs 2025"],
                )
                assert_files_same(
                    join(fixtures_dir, "hdx_hapi_humanitarian_needs_global_2025.csv"),
                    join(tempdir, "hdx_hapi_humanitarian_needs_global_2025.csv"),
                )

                # "Refugee Response"/RR is a real, pre-existing data-quality
                # quirk (not recognised by hdx-python-pipelineutils' Sector
                # lookup table) also visible in the old REST-era pipeline
                # (which flagged it as an unmapped entity) - not introduced by
                # this migration.
                check.equal(
                    error_handler.shared_errors["error"],
                    {
                        "HumanitarianNeeds - Global HPC HNO": {
                            "HumanitarianNeeds - Global HPC HNO - cluster RR not found"
                        }
                    },
                )
