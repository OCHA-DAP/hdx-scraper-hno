import logging
from datetime import datetime, timezone
from os.path import join

import pytest
from pytest_check import check

from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.data.vocabulary import Vocabulary
from hdx.scraper.framework.utilities.reader import Read
from hdx.scraper.hno.dataset_generator import DatasetGenerator
from hdx.scraper.hno.monitor_json import MonitorJSON
from hdx.scraper.hno.plan import Plan
from hdx.scraper.hno.progress_json import ProgressJSON
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.path import script_dir_plus_file, temp_dir
from hdx.utilities.useragent import UserAgent

logger = logging.getLogger(__name__)


class TestHumanitarianNeeds:
    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=script_dir_plus_file(
                join("config", "project_configuration.yaml"), Plan
            ),
        )
        Locations.set_validlocations(
            [
                {"name": "afg", "title": "Afghanistan"},
                {"name": "sdn", "title": "Sudan"},
                {"name": "world", "title": "World"},
            ]
        )
        Vocabulary._approved_vocabulary = {
            "tags": [
                {"name": tag}
                for tag in (
                    "hxl",
                    "humanitarian needs overview - hno",
                    "people in need - pin",
                )
            ],
            "id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            "name": "approved",
        }
        return Configuration.read()

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
                    "AFG,SDN",
                    pcodes_to_process="AF01,AF0101,SD01,SD01001",
                )
                dataset_generator = DatasetGenerator(configuration, year)
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

                plan.setup_admins()
                monitor_json = MonitorJSON(input_dir, False)
                published, rows = plan.process("AFG", "1185", monitor_json)
                check.equal(
                    published, datetime(2024, 5, 17, 0, 0, tzinfo=timezone.utc)
                )
                check.equal(len(rows), 1217)
                highest_admin = plan.get_highest_admin("AFG")
                check.equal(highest_admin, 2)
                key_value_pairs = list(rows.items())
                key, value = key_value_pairs[0]
                check.equal(key, ("", "", ""))
                check.equal(
                    value,
                    {
                        "Admin 1 Name": "",
                        "Admin 1 PCode": "",
                        "Admin 2 Name": "",
                        "Admin 2 PCode": "",
                        "Admin 3 Name": "",
                        "Admin 3 PCode": "",
                        "Admin 4 Name": "",
                        "Admin 4 PCode": "",
                        "Admin 5 Name": "",
                        "Admin 5 PCode": "",
                        "Affected": "",
                        "Category": "total",
                        "In Need": 23666389,
                        "Original Sector": "ALL",
                        "Population": 44532600,
                        "Reached": 17327995,
                        "Sector": "Intersectoral",
                        "Targeted": 17327995,
                        "Valid Location": "Y",
                    },
                )
                key, value = key_value_pairs[256]
                check.equal(key, ("AF01", "FSC", "Children - Male"))
                check.equal(
                    value,
                    {
                        "Valid Location": "Y",
                        "Original Sector": "FSC",
                        "Category": "Children - Male",
                        "Sector": "FSC",
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
                        "In Need": 537481,
                        "Targeted": 537481,
                        "Affected": "",
                        "Reached": "",
                    },
                )
                key, value = key_value_pairs[381]
                check.equal(key, ("AF0101", "HEA", "Adult - Male"))
                check.equal(
                    value,
                    {
                        "Valid Location": "Y",
                        "Original Sector": "HEA",
                        "Category": "Adult - Male",
                        "Sector": "HEA",
                        "Admin 1 PCode": "AF01",
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
                        "In Need": 470052,
                        "Targeted": 302171,
                        "Affected": "",
                        "Reached": "",
                    },
                )
                key, value = key_value_pairs[557]
                check.equal(key, ("AF0101", "PRO", "People with Disabilities"))
                check.equal(
                    value,
                    {
                        "Valid Location": "Y",
                        "Original Sector": "",
                        "Category": "People with Disabilities",
                        "Sector": "PRO",
                        "Admin 1 PCode": "AF01",
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
                        "In Need": 316816,
                        "Targeted": 76564,
                        "Affected": "",
                        "Reached": "",
                    },
                )
                key, value = key_value_pairs[616]
                check.equal(
                    key,
                    (
                        "AF01",
                        "PRO-GBV",
                        "Children - Male - Pakistan (District of Return)",
                    ),
                )
                check.equal(
                    value,
                    {
                        "Valid Location": "Y",
                        "Original Sector": "",
                        "Category": "Children - Male - Pakistan (District of Return)",
                        "Sector": "PRO-GBV",
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
                        "In Need": 1552,
                        "Targeted": 109,
                        "Affected": "",
                        "Reached": "",
                    },
                )
                key, value = key_value_pairs[1215]
                check.equal(key, ("AF0101", "WSH", "Adult - Female"))
                check.equal(
                    value,
                    {
                        "Valid Location": "Y",
                        "Original Sector": "WSH",
                        "Category": "Adult - Female",
                        "Sector": "WSH",
                        "Admin 1 PCode": "AF01",
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
                        "In Need": 23852,
                        "Targeted": 15504,
                        "Affected": "",
                        "Reached": "",
                    },
                )

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
                    dataset, "AFG", rows, tempdir, 2024, highest_admin
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
                    dataset, "AFG", rows, tempdir, 2024, highest_admin
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

                published, rows = plan.process("SDN", "1188", monitor_json)
                check.equal(
                    published, datetime(2024, 5, 13, 0, 0, tzinfo=timezone.utc)
                )
                check.equal(len(rows), 222)
                highest_admin = plan.get_highest_admin("SDN")
                check.equal(highest_admin, 2)
                key_value_pairs = list(rows.items())
                key, value = key_value_pairs[0]
                check.equal(key, ("", "", ""))
                check.equal(
                    value,
                    {
                        "Valid Location": "Y",
                        "Original Sector": "ALL",
                        "Category": "total",
                        "Sector": "Intersectoral",
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
                        "Reached": "",
                    },
                )
                key, value = key_value_pairs[116]
                check.equal(key, ("SD01001", "PRO", "Female"))
                check.equal(
                    value,
                    {
                        "Valid Location": "Y",
                        "Original Sector": "",
                        "Category": "Female",
                        "Sector": "PRO",
                        "Admin 1 PCode": "SD01",
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
                        "In Need": 48657,
                        "Targeted": 29194,
                        "Affected": "",
                        "Reached": "",
                    },
                )
                key, value = key_value_pairs[205]
                check.equal(key, ("", "WSH", "Non-Displaced"))
                check.equal(
                    value,
                    {
                        "Valid Location": "Y",
                        "Original Sector": "WSH",
                        "Category": "Non-Displaced",
                        "Sector": "WSH",
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
                        "In Need": 9381655,
                        "Targeted": 4365199,
                        "Affected": "",
                        "Reached": "",
                    },
                )

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
                _ = dataset_generator.add_country_resource(
                    dataset, "SDN", rows, tempdir, 2021, highest_admin
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
                    dataset, "SDN", rows, tempdir, 2021, highest_admin
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

                global_rows = plan.get_global_rows()
                dataset = dataset_generator.generate_global_dataset(
                    tempdir, global_rows, ["AFG", "SDN"], 2024, highest_admin
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

                plan.add_negative_rounded_errors("AFG")
                plan.add_negative_rounded_errors("SDN")
                assert error_handler.shared_errors["hdx_error"] == {}
                assert error_handler.shared_errors["error"] == {}
                assert error_handler.shared_errors["warning"] == {
                    "HumanitarianNeeds - HPC": {
                        "HumanitarianNeeds - HPC - 16 population value(s) rounded in SDN. First 10 values: 2556222.092, 1980059.378, 4365198.53, 4539754.8, 4361725.2, 3293547.6, 267044.4, 2114.982659, 5269.306391, 203083.7109",
                        "HumanitarianNeeds - HPC - caseload Refugee Response (7454) unknown sector in SDN",
                    }
                }

                assert plan._used_sector_mapping == {
                    "ALL": "Intersectoral",
                    "CCM": "CCM",
                    "Child Protection": "PRO-CPN",
                    "EDU": "EDU",
                    "FSC": "FSC",
                    "Gender-Based Violence (GBV)": "PRO-GBV",
                    "HEA": "HEA",
                    "Housing, Land and Property": "PRO-HLP",
                    "MS": "Multi",
                    "Mine Action": "PRO-MIN",
                    "NUT": "NUT",
                    "Protection (overall)": "PRO",
                    "Refugee Response": "",
                    "SHL": "SHL",
                    "WSH": "WSH",
                }
