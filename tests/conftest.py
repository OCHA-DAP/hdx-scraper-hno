from os.path import join

import pytest

from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.scraper.hno.plan import Plan
from hdx.utilities.path import script_dir_plus_file
from hdx.utilities.useragent import UserAgent


@pytest.fixture(scope="session")
def configuration():
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
