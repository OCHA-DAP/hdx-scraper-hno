import logging
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.scraper.hno.monitor_json import MonitorJSON
from hdx.scraper.hno.plan import Plan
from hdx.scraper.hno.progress_json import ProgressJSON
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import script_dir_plus_file, temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

logger = logging.getLogger(__name__)


class TestHAPIPipelineHNO:
    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=script_dir_plus_file(
                "project_configuration.yaml", Plan
            ),
        )
        Locations.set_validlocations(
            [
                {"name": "afg", "title": "Afghanistan"},
                {"name": "sdn", "title": "Sudan"},
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

    def test_plan(self, configuration, fixtures_dir, input_dir):
        with temp_dir(
            "TestHAPIHNO",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            year = 2024
            plan = Plan(
                configuration,
                year,
                "AFG,SDN",
                pcodes_to_process="AF01,AF0101,SD01,SD01001",
            )
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader,
                    tempdir,
                    input_dir,
                    tempdir,
                    save=False,
                    use_saved=True,
                )
                progress_json = ProgressJSON(year, input_dir, False)
                plan_ids_countries = plan.get_plan_ids_and_countries(
                    retriever, progress_json
                )
                assert plan_ids_countries == [
                    {"id": 1185, "iso3": "AFG"},
                    {"id": 1188, "iso3": "SDN"},
                ]

                monitor_json = MonitorJSON(input_dir, False)
                rows = plan.process(retriever, "AFG", "1185", monitor_json)
                assert len(rows) == 500
                key_value_pairs = list(rows.items())
                key, value = key_value_pairs[0]
                assert key == ("", "", "ALL", "", -1, -1, "", "")
                assert value == {
                    "Admin 1 PCode": "",
                    "Admin 2 PCode": "",
                    "Affected": "",
                    "Min Age": "",
                    "Max Age": "",
                    "Disabled": "",
                    "Gender": "",
                    "In Need": 23666389,
                    "Population": 44532600,
                    "Population Group": "",
                    "Reached": 17327995,
                    "Sector": "",
                    "Targeted": 17327995,
                }
                key, value = key_value_pairs[104]
                assert key == ("AF01", "", "FSC", "", -1, -1, "y", "")
                assert value == {
                    "Admin 1 PCode": "AF01",
                    "Admin 2 PCode": "",
                    "Affected": "",
                    "Min Age": "",
                    "Max Age": "",
                    "Disabled": "y",
                    "Gender": "",
                    "In Need": 188796,
                    "Population": "",
                    "Population Group": "",
                    "Reached": "",
                    "Sector": "FSC",
                    "Targeted": 188796,
                }
                key, value = key_value_pairs[149]
                assert key == ("AF01", "AF0101", "HEA", "", 65, -1, "", "")
                assert value == {
                    "Admin 1 PCode": "AF01",
                    "Admin 2 PCode": "AF0101",
                    "Affected": "",
                    "Min Age": 65,
                    "Max Age": "",
                    "Disabled": "",
                    "Gender": "",
                    "In Need": 57392,
                    "Population": "",
                    "Population Group": "",
                    "Reached": "",
                    "Sector": "HEA",
                    "Targeted": 36895,
                }
                key, value = key_value_pairs[220]
                assert key == (
                    "AF01",
                    "AF0101",
                    "PRO",
                    "m",
                    18,
                    64,
                    "",
                    "REF",
                )
                assert value == {
                    "Admin 1 PCode": "AF01",
                    "Admin 2 PCode": "AF0101",
                    "Affected": "",
                    "Min Age": 18,
                    "Max Age": 64,
                    "Disabled": "",
                    "Gender": "m",
                    "In Need": 81,
                    "Population": "",
                    "Population Group": "REF",
                    "Reached": "",
                    "Sector": "PRO",
                    "Targeted": 81,
                }
                key, value = key_value_pairs[250]
                assert key == ("", "", "PRO_GBV", "f", 18, 64, "", "")
                assert value == {
                    "Admin 1 PCode": "",
                    "Admin 2 PCode": "",
                    "Affected": "",
                    "Min Age": 18,
                    "Max Age": 64,
                    "Disabled": "",
                    "Gender": "f",
                    "In Need": 5695759,
                    "Population": "",
                    "Population Group": "",
                    "Reached": "",
                    "Sector": "PRO_GBV",
                    "Targeted": 1035118,
                }
                key, value = key_value_pairs[498]
                assert key == (
                    "AF01",
                    "AF0101",
                    "WSH",
                    "f",
                    18,
                    64,
                    "",
                    "",
                )
                assert value == {
                    "Admin 1 PCode": "AF01",
                    "Admin 2 PCode": "AF0101",
                    "Affected": "",
                    "Min Age": 18,
                    "Max Age": 64,
                    "Disabled": "",
                    "Gender": "f",
                    "In Need": 23852,
                    "Population": "",
                    "Population Group": "",
                    "Reached": "",
                    "Sector": "WSH",
                    "Targeted": 15504,
                }
                dataset = plan.generate_dataset("AFG", rows, tempdir)
                assert dataset == {
                    "data_update_frequency": "365",
                    "dataset_date": "[2024-01-01T00:00:00 TO 2024-12-31T23:59:59]",
                    "groups": [{"name": "afg"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "hno-data-for-afg",
                    "owner_org": "hdx",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        }
                    ],
                    "title": "Afghanistan - Humanitarian Needs Overview",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "HNO data with HXL tags",
                        "format": "csv",
                        "name": "HNO Data for AFG",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    }
                ]
                filename = "hno_data_afg.csv"
                expected_file = join(fixtures_dir, filename)
                actual_file = join(tempdir, filename)
                assert_files_same(expected_file, actual_file)

                rows = plan.process(retriever, "SDN", "1188", monitor_json)
                assert len(rows) == 200
                key_value_pairs = list(rows.items())
                key, value = key_value_pairs[0]
                assert key == ("", "", "ALL", "", -1, -1, "", "")
                assert value == {
                    "Admin 1 PCode": "",
                    "Admin 2 PCode": "",
                    "Affected": 28928870,
                    "Min Age": "",
                    "Max Age": "",
                    "Disabled": "",
                    "Gender": "",
                    "In Need": 24786370,
                    "Population": 50681990,
                    "Population Group": "",
                    "Reached": "",
                    "Sector": "",
                    "Targeted": 14657114,
                }
                key, value = key_value_pairs[99]
                assert key == ("", "", "PRO", "", -1, 17, "", "")
                assert value == {
                    "Admin 1 PCode": "",
                    "Admin 2 PCode": "",
                    "Affected": "",
                    "Min Age": "",
                    "Max Age": 17,
                    "Disabled": "",
                    "Gender": "",
                    "In Need": 4255433,
                    "Population": "",
                    "Population Group": "",
                    "Reached": "",
                    "Sector": "PRO",
                    "Targeted": 1985278,
                }
                key, value = key_value_pairs[199]
                assert key == ("SD01", "SD01001", "WSH", "", -1, -1, "", "")
                assert value == {
                    "Admin 1 PCode": "SD01",
                    "Admin 2 PCode": "SD01001",
                    "Affected": "",
                    "Min Age": "",
                    "Max Age": "",
                    "Disabled": "",
                    "Gender": "",
                    "In Need": 598658,
                    "Population": "",
                    "Population Group": "",
                    "Reached": "",
                    "Sector": "WSH",
                    "Targeted": 210468,
                }
                dataset = plan.generate_dataset("SDN", rows, tempdir)
                assert dataset == {
                    "data_update_frequency": "365",
                    "dataset_date": "[2024-01-01T00:00:00 TO 2024-12-31T23:59:59]",
                    "groups": [{"name": "sdn"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "hno-data-for-sdn",
                    "owner_org": "hdx",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        }
                    ],
                    "title": "Sudan - Humanitarian Needs Overview",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "HNO data with HXL tags",
                        "format": "csv",
                        "name": "HNO Data for SDN",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    }
                ]
                filename = "hno_data_sdn.csv"
                expected_file = join(fixtures_dir, filename)
                actual_file = join(tempdir, filename)
                assert_files_same(expected_file, actual_file)
