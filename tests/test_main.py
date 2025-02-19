import logging
from datetime import datetime, timezone
from os.path import join

import pytest
from pytest_check import check

from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.scraper.framework.utilities.reader import Read
from hdx.scraper.hno.dataset_generator import DatasetGenerator
from hdx.scraper.hno.hapi_dataset_generator import HAPIDatasetGenerator
from hdx.scraper.hno.hapi_output import HAPIOutput
from hdx.scraper.hno.monitor_json import MonitorJSON
from hdx.scraper.hno.plan import Plan
from hdx.scraper.hno.progress_json import ProgressJSON
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.path import temp_dir

logger = logging.getLogger(__name__)


class TestHumanitarianNeeds:
    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="function")
    def input_dataset_afg(self, configuration, input_dir):
        return Dataset.load_from_json(
            join(input_dir, "afghanistan-humanitarian-needs.json")
        )

    @pytest.fixture(scope="function")
    def input_dataset_sdn(self, configuration, input_dir):
        return Dataset.load_from_json(
            join(input_dir, "sudan-humanitarian-needs.json")
        )

    def test_all(
        self,
        configuration,
        fixtures_dir,
        input_dir,
        input_dataset_afg,
        input_dataset_sdn,
    ):
        def read_dataset(name):
            if "afghanistan" in name:
                return input_dataset_afg
            elif "sudan" in name:
                return input_dataset_sdn

        with HDXErrorHandler() as error_handler:
            with temp_dir(
                "TestHNO",
                delete_on_success=True,
                delete_on_failure=False,
            ) as tempdir:
                today = parse_date("09/10/2024")
                year = today.year
                Read.create_readers(
                    tempdir,
                    input_dir,
                    tempdir,
                    False,
                    True,
                    today=today,
                )
                plan = Plan(
                    configuration,
                    year,
                    error_handler,
                    ["AFG", "SDN"],
                    pcodes_to_process=["AF01", "AF0101", "SD01", "SD01001"],
                )
                dataset_generator = DatasetGenerator(configuration, year)
                hapi_output = HAPIOutput(
                    configuration,
                    year,
                    error_handler,
                    dataset_generator.global_name,
                )
                hapi_output.setup_admins()

                progress_json = ProgressJSON(year, input_dir, False)
                plan_ids_countries = plan.get_plan_ids_and_countries(
                    progress_json
                )
                check.equal(
                    plan_ids_countries,
                    [
                        {"id": 1185, "iso3": "AFG"},
                        {"id": 1188, "iso3": "SDN"},
                    ],
                )

                monitor_json = MonitorJSON(input_dir, False)
                countryiso3 = "AFG"
                published, rows = plan.process(
                    countryiso3, "1185", monitor_json
                )
                check.equal(
                    published, datetime(2024, 5, 17, 0, 0, tzinfo=timezone.utc)
                )
                check.equal(len(rows), 1230)
                highest_admin = plan.get_highest_admin(countryiso3)
                check.equal(highest_admin, 2)
                key_value_pairs = list(rows.items())
                key, value = key_value_pairs[0]
                check.equal(key, ("", "ALL", "Final HNRP Caseload", ""))
                check.equal(
                    value,
                    {
                        "Category": "",
                        "Description": "Final HNRP Caseload",
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
                        "Population": 44532600,
                        "In Need": 23666389,
                        "Targeted": 17327995,
                        "Affected": None,
                        "Reached": 17327995,
                    },
                )
                key, value = key_value_pairs[256]
                check.equal(
                    key,
                    (
                        "AF01",
                        "FSC",
                        "Food Security",
                        "People with Disabilities",
                    ),
                )
                check.equal(
                    value,
                    {
                        "Category": "People with Disabilities",
                        "Description": "Food Security",
                        "Info": "",
                        "Cluster": "FSC",
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
                        "In Need": 188796,
                        "Targeted": 188796,
                        "Affected": "",
                        "Reached": "",
                    },
                )
                key, value = key_value_pairs[381]
                check.equal(key, ("AF0101", "HEA", "Health", "Elderly"))
                check.equal(
                    value,
                    {
                        "Category": "Elderly",
                        "Description": "Health",
                        "Info": "",
                        "Cluster": "HEA",
                        "Admin 1 PCode": "",
                        "Admin 1 Name": "",
                        "Admin 2 PCode": "AF0101",
                        "Admin 2 Name": "Kabul",
                        "Admin 3 PCode": "",
                        "Admin 3 Name": "",
                        "Admin 4 PCode": "",
                        "Admin 4 Name": "",
                        "Admin 5 PCode": "",
                        "Admin 5 Name": "",
                        "Population": "",
                        "In Need": 57392,
                        "Targeted": 36895,
                        "Affected": "",
                        "Reached": "",
                    },
                )
                key, value = key_value_pairs[557]
                check.equal(
                    key,
                    (
                        "AF0101",
                        "PRO",
                        "Protection (overall)",
                        "Children - Female - Refugees",
                    ),
                )
                check.equal(
                    value,
                    {
                        "Category": "Children - Female - Refugees",
                        "Description": "Protection (overall)",
                        "Info": "",
                        "Cluster": "PRO",
                        "Admin 1 PCode": "",
                        "Admin 1 Name": "",
                        "Admin 2 PCode": "AF0101",
                        "Admin 2 Name": "Kabul",
                        "Admin 3 PCode": "",
                        "Admin 3 Name": "",
                        "Admin 4 PCode": "",
                        "Admin 4 Name": "",
                        "Admin 5 PCode": "",
                        "Admin 5 Name": "",
                        "Population": "",
                        "In Need": 118,
                        "Targeted": 118,
                        "Affected": "",
                        "Reached": "",
                    },
                )
                key, value = key_value_pairs[616]
                check.equal(
                    key,
                    (
                        "",
                        "PRO-GBV",
                        "Gender-Based Violence (GBV)",
                        "Adult - Male",
                    ),
                )
                check.equal(
                    value,
                    {
                        "Category": "Adult - Male",
                        "Description": "Gender-Based Violence (GBV)",
                        "Info": "",
                        "Cluster": "PRO-GBV",
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
                        "In Need": 589051,
                        "Targeted": 29147,
                        "Affected": "",
                        "Reached": "",
                    },
                )
                key, value = key_value_pairs[1215]
                check.equal(
                    key,
                    (
                        "AF0101",
                        "WSH",
                        "Water, Sanitation and Hygiene",
                        "Children - Male - Temporary Sites",
                    ),
                )
                check.equal(
                    value,
                    {
                        "Category": "Children - Male - Temporary Sites",
                        "Description": "Water, Sanitation and Hygiene",
                        "Info": "",
                        "Cluster": "WSH",
                        "Admin 1 PCode": "",
                        "Admin 1 Name": "",
                        "Admin 2 PCode": "AF0101",
                        "Admin 2 Name": "Kabul",
                        "Admin 3 PCode": "",
                        "Admin 3 Name": "",
                        "Admin 4 PCode": "",
                        "Admin 4 Name": "",
                        "Admin 5 PCode": "",
                        "Admin 5 Name": "",
                        "Population": "",
                        "In Need": 166,
                        "Targeted": 166,
                        "Affected": "",
                        "Reached": "",
                    },
                )
                hapi_output.process(countryiso3, rows)
                hapi_output.add_negative_rounded_errors(countryiso3)

                dataset = dataset_generator.get_country_dataset(
                    "AFG", read_fn=read_dataset
                )
                check.equal(dataset["name"], "afghanistan-humanitarian-needs")
                check.equal(
                    dataset["title"], "Afghanistan: Humanitarian Needs"
                )
                resource_names = [x["name"] for x in dataset.get_resources()]
                check.equal(
                    resource_names,
                    [
                        "afg_hpc_needs_2024.xlsx",
                        "afg_hpc_needs_2023.xlsx",
                        "afg_hpc_needs_2022.xlsx",
                        "afg_hpc_needs_2021.xlsx",
                        "afg_hpc_needs_2020.xlsx",
                        "afg_hpc_needs_2019.xlsx",
                        "afg_hpc_needs_2018.xlsx",
                        "afg_hpc_needs_2017.xlsx",
                    ],
                )
                _ = dataset_generator.add_country_resource(
                    dataset, "AFG", rows, tempdir, highest_admin
                )
                resource_names = [x["name"] for x in dataset.get_resources()]
                filename = "afg_hpc_needs_api_2024.csv"
                check.equal(
                    resource_names,
                    [
                        filename,
                        "afg_hpc_needs_2024.xlsx",
                        "afg_hpc_needs_2023.xlsx",
                        "afg_hpc_needs_2022.xlsx",
                        "afg_hpc_needs_2021.xlsx",
                        "afg_hpc_needs_2020.xlsx",
                        "afg_hpc_needs_2019.xlsx",
                        "afg_hpc_needs_2018.xlsx",
                        "afg_hpc_needs_2017.xlsx",
                    ],
                )
                _ = dataset_generator.add_country_resource(
                    dataset, "AFG", rows, tempdir, highest_admin
                )
                resource_names = [x["name"] for x in dataset.get_resources()]
                filename = "afg_hpc_needs_api_2024.csv"
                check.equal(
                    resource_names,
                    [
                        filename,
                        "afg_hpc_needs_2024.xlsx",
                        "afg_hpc_needs_2023.xlsx",
                        "afg_hpc_needs_2022.xlsx",
                        "afg_hpc_needs_2021.xlsx",
                        "afg_hpc_needs_2020.xlsx",
                        "afg_hpc_needs_2019.xlsx",
                        "afg_hpc_needs_2018.xlsx",
                        "afg_hpc_needs_2017.xlsx",
                    ],
                )
                expected_file = join(fixtures_dir, filename)
                actual_file = join(tempdir, filename)
                assert_files_same(expected_file, actual_file)

                countryiso3 = "SDN"
                published, rows = plan.process(
                    countryiso3, "1188", monitor_json
                )
                check.equal(
                    published, datetime(2024, 5, 13, 0, 0, tzinfo=timezone.utc)
                )
                check.equal(len(rows), 235)
                highest_admin = plan.get_highest_admin(countryiso3)
                check.equal(highest_admin, 2)
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
                        "Population": 50990034,
                        "In Need": 24786370,
                        "Targeted": 14657114,
                        "Affected": 28928873,
                        "Reached": None,
                    },
                )
                key, value = key_value_pairs[116]
                check.equal(
                    key, ("", "PRO", "Protection (overall)", "Children")
                )
                check.equal(
                    value,
                    {
                        "Category": "Children",
                        "Description": "Protection (overall)",
                        "Info": "",
                        "Cluster": "PRO",
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
                        "In Need": 4255433,
                        "Targeted": 1985278,
                        "Affected": "",
                        "Reached": "",
                    },
                )
                key, value = key_value_pairs[205]
                check.equal(
                    key,
                    (
                        "SD01001",
                        "SHL",
                        "Shelter and Non-Food Items",
                        "Hostcommunities",
                    ),
                )
                check.equal(
                    value,
                    {
                        "Category": "Hostcommunities",
                        "Description": "Shelter and Non-Food Items",
                        "Info": "",
                        "Cluster": "SHL",
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
                        "In Need": 4413,
                        "Targeted": 706,
                        "Affected": "",
                        "Reached": "",
                    },
                )

                dataset_generator._year = 2021
                dataset = dataset_generator.get_country_dataset(
                    "SDN", read_fn=read_dataset
                )
                check.equal(dataset["name"], "sudan-humanitarian-needs")
                check.equal(dataset["title"], "Sudan: Humanitarian Needs")
                resource_names = [x["name"] for x in dataset.get_resources()]
                check.equal(
                    resource_names,
                    [
                        "sdn_hpc_needs_2024.xlsx",
                        "sdn_hpc_needs_2023.xlsx",
                        "sdn_hpc_needs_2022.xlsx",
                        "sdn_hpc_needs_2021.xlsx",
                        "sdn_hpc_needs_2020.xlsx",
                        "sdn_hpc_needs_2019.xlsx",
                        "sdn_hpc_needs_2018.xlsx",
                        "sdn_hpc_needs_2017.xlsx",
                        "sdn_hpc_needs_2016.xlsx",
                        "sdn_hpc_needs_2015.xlsx",
                    ],
                )
                hapi_output.process(countryiso3, rows)
                hapi_output.add_negative_rounded_errors(countryiso3)

                _ = dataset_generator.add_country_resource(
                    dataset, "SDN", rows, tempdir, highest_admin
                )
                resource_names = [x["name"] for x in dataset.get_resources()]
                filename = "sdn_hpc_needs_api_2021.csv"
                check.equal(
                    resource_names,
                    [
                        "sdn_hpc_needs_2024.xlsx",
                        "sdn_hpc_needs_2023.xlsx",
                        "sdn_hpc_needs_2022.xlsx",
                        filename,
                        "sdn_hpc_needs_2021.xlsx",
                        "sdn_hpc_needs_2020.xlsx",
                        "sdn_hpc_needs_2019.xlsx",
                        "sdn_hpc_needs_2018.xlsx",
                        "sdn_hpc_needs_2017.xlsx",
                        "sdn_hpc_needs_2016.xlsx",
                        "sdn_hpc_needs_2015.xlsx",
                    ],
                )
                _ = dataset_generator.add_country_resource(
                    dataset, "SDN", rows, tempdir, highest_admin
                )
                resource_names = [x["name"] for x in dataset.get_resources()]
                filename = "sdn_hpc_needs_api_2021.csv"
                check.equal(
                    resource_names,
                    [
                        "sdn_hpc_needs_2024.xlsx",
                        "sdn_hpc_needs_2023.xlsx",
                        "sdn_hpc_needs_2022.xlsx",
                        filename,
                        "sdn_hpc_needs_2021.xlsx",
                        "sdn_hpc_needs_2020.xlsx",
                        "sdn_hpc_needs_2019.xlsx",
                        "sdn_hpc_needs_2018.xlsx",
                        "sdn_hpc_needs_2017.xlsx",
                        "sdn_hpc_needs_2016.xlsx",
                        "sdn_hpc_needs_2015.xlsx",
                    ],
                )
                expected_file = join(fixtures_dir, filename)
                actual_file = join(tempdir, filename)
                assert_files_same(expected_file, actual_file)

                dataset_generator._year = 2024
                countries_with_data = ["AFG", "SDN"]
                global_rows = plan.get_global_rows()
                dataset, resource = dataset_generator.generate_global_dataset(
                    tempdir, global_rows, countries_with_data, highest_admin
                )
                check.equal(
                    dataset,
                    {
                        "data_update_frequency": "365",
                        "dataset_date": "[2024-01-01T00:00:00 TO 2024-12-31T23:59:59]",
                        "groups": [{"name": "afg"}, {"name": "sdn"}],
                        "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                        "name": "global-hpc-hno",
                        "owner_org": "49f12a06-1605-4f98-89f1-eaec37a0fdfe",
                        "subnational": "1",
                        "tags": [
                            {
                                "name": "hxl",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            }
                        ],
                        "title": "Global Humanitarian Programme Cycle, Humanitarian Needs",
                    },
                )
                resources = dataset.get_resources()
                check.equal(
                    resources,
                    [
                        {
                            "description": "This resource contains standardised subnational 2024 Humanitarian Needs Overview data taken from the OCHA HPC Tools system which is under active development.",
                            "format": "csv",
                            "name": "Global HPC HNO 2024",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        }
                    ],
                )
                filename = "hpc_hno_2024.csv"
                expected_file = join(fixtures_dir, filename)
                actual_file = join(tempdir, filename)
                assert_files_same(expected_file, actual_file)

                global_rows = hapi_output.get_global_rows()
                check.equal(len(global_rows), 2957)
                key_value_pairs = list(global_rows.items())
                key, value = key_value_pairs[0]
                check.equal(key, ("AFG", "", "", "", "", "", "", "all"))
                check.equal(
                    value,
                    {
                        "admin1_code": "",
                        "admin1_name": "",
                        "admin2_code": "",
                        "admin2_name": "",
                        "admin_level": 0,
                        "category": "",
                        "error": "",
                        "has_hrp": "Y",
                        "in_gho": "Y",
                        "location_code": "AFG",
                        "population": 44532600,
                        "population_status": "all",
                        "provider_admin1_name": "",
                        "provider_admin2_name": "",
                        "reference_period_end": "2024-12-31",
                        "reference_period_start": "2024-01-01",
                        "sector_code": "Intersectoral",
                        "sector_name": "Intersectoral",
                        "warning": "",
                    },
                )
                key, value = key_value_pairs[1000]
                check.equal(
                    key,
                    (
                        "AFG",
                        "Kabul",
                        "",
                        "AF01",
                        "",
                        "PRO",
                        "Children - Female - Border / EC",
                        "TGT",
                    ),
                )
                check.equal(
                    value,
                    {
                        "category": "Children - Female - Border / EC",
                        "warning": "",
                        "error": "",
                        "reference_period_start": "2024-01-01",
                        "reference_period_end": "2024-12-31",
                        "sector_code": "PRO",
                        "sector_name": "Protection",
                        "location_code": "AFG",
                        "has_hrp": "Y",
                        "in_gho": "Y",
                        "provider_admin1_name": "Kabul",
                        "provider_admin2_name": "",
                        "admin1_code": "AF01",
                        "admin1_name": "Kabul",
                        "admin2_code": "",
                        "admin2_name": "",
                        "admin_level": 1,
                        "population_status": "TGT",
                        "population": 3731,
                    },
                )
                key, value = key_value_pairs[2000]
                check.equal(
                    key,
                    (
                        "AFG",
                        "",
                        "Kabul",
                        "AF01",
                        "AF0101",
                        "PRO-CPN",
                        "Elderly",
                        "TGT",
                    ),
                )
                check.equal(
                    value,
                    {
                        "category": "Elderly",
                        "warning": "",
                        "error": "",
                        "reference_period_start": "2024-01-01",
                        "reference_period_end": "2024-12-31",
                        "sector_code": "PRO-CPN",
                        "sector_name": "Child Protection",
                        "location_code": "AFG",
                        "has_hrp": "Y",
                        "in_gho": "Y",
                        "provider_admin1_name": "",
                        "provider_admin2_name": "Kabul",
                        "admin1_code": "AF01",
                        "admin1_name": "Kabul",
                        "admin2_code": "AF0101",
                        "admin2_name": "Kabul",
                        "admin_level": 2,
                        "population_status": "TGT",
                        "population": 1327,
                    },
                )
                key, value = key_value_pairs[2956]
                check.equal(
                    key,
                    (
                        "SDN",
                        "",
                        "Jebel Awlia",
                        "SD01",
                        "SD01001",
                        "WSH",
                        "total",
                        "TGT",
                    ),
                )
                check.equal(
                    value,
                    {
                        "category": "total",
                        "warning": "",
                        "error": "",
                        "reference_period_start": "2024-01-01",
                        "reference_period_end": "2024-12-31",
                        "sector_code": "WSH",
                        "sector_name": "Water Sanitation Hygiene",
                        "location_code": "SDN",
                        "has_hrp": "Y",
                        "in_gho": "Y",
                        "provider_admin1_name": "",
                        "provider_admin2_name": "Jebel Awlia",
                        "admin1_code": "SD01",
                        "admin1_name": "Khartoum",
                        "admin2_code": "SD01001",
                        "admin2_name": "Jebel Awlia",
                        "admin_level": 2,
                        "population_status": "TGT",
                        "population": 210468,
                    },
                )

                hapi_dataset_generator = HAPIDatasetGenerator(
                    configuration,
                    year,
                    global_rows,
                    countries_with_data,
                )
                dataset = hapi_dataset_generator.generate_needs_dataset(
                    tempdir, countries_with_data, "1234", "5678", None
                )
                check.equal(
                    dataset,
                    {
                        "name": "hdx-hapi-humanitarian-needs",
                        "title": "HDX HAPI - Affected People: Humanitarian Needs",
                        "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                        "owner_org": "40d10ece-49de-4791-9aed-e164f1d16dd1",
                        "data_update_frequency": "30",
                        "tags": [
                            {
                                "name": "hxl",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            }
                        ],
                        "dataset_source": "OCHA Humanitarian Programme Cycle Tools (HPC Tools)",
                        "license_id": "cc-by-igo",
                        "subnational": "1",
                        "groups": [{"name": "afg"}, {"name": "sdn"}],
                        "dataset_date": "[2024-01-01T00:00:00 TO 2024-12-31T23:59:59]",
                        "dataset_preview": "no_preview",
                    },
                )
                check.equal(
                    dataset.get_resources(),
                    [
                        {
                            "name": "Global Affected People: Humanitarian Needs 2024",
                            "description": "Humanitarian needs data from HDX HAPI, please see [the documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/affected_people/#humanitarian-needs) for more information",
                            "format": "csv",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                            "dataset_preview_enabled": "False",
                        }
                    ],
                )
                filename = "hdx_hapi_humanitarian_needs_global_2024.csv"
                expected_file = join(fixtures_dir, filename)
                actual_file = join(tempdir, filename)
                assert_files_same(expected_file, actual_file)

                check.equal(error_handler.shared_errors["hdx_error"], {})
                check.equal(error_handler.shared_errors["error"], {})
                check.equal(
                    error_handler.shared_errors["warning"],
                    {
                        "HumanitarianNeeds - HPC": {
                            "HumanitarianNeeds - HPC - 16 population value(s) rounded in SDN. First 10 values: 2556222.092, 1980059.378, 4365198.53, 4539754.8, 4361725.2, 3293547.6, 267044.4, 2114.982659, 5269.306391, 203083.7109",
                            "HumanitarianNeeds - HPC - caseload Refugee Response no cluster for entity 7454 in SDN",
                        }
                    },
                )
