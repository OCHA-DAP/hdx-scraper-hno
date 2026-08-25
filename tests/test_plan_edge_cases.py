"""Targeted unit tests for Plan.process()'s error/warning/edge-case branches
that the small real-fixture sample in test_main.py doesn't happen to exercise
(see the coverage gaps noted after the GraphQL migration). These use
monkeypatched graphql_reader calls with small synthetic payloads rather than
live/replayed fixtures, since the goal is exercising Plan's own branch logic,
not the transport layer.
"""

import pytest
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.utilities.base_downloader import DownloadError

from hdx.scraper.hno import graphql_reader
from hdx.scraper.hno.plan import Plan


def _fact(
    attachment_id,
    hpc_type,
    value,
    is_total=False,
    location=None,
    location_id=None,
    gender=None,
    age_group=None,
    population_status=None,
):
    return {
        "AttachmentId": attachment_id,
        "IsTotal": is_total,
        "ValueNum": value,
        "LocationId": location_id,
        "location": location,
        "gender": {"Name": gender} if gender else None,
        "ageGroup": {"Name": age_group} if age_group else None,
        "populationStatus": {"Name": population_status} if population_status else None,
        "settlementType": None,
        "disabilityStatus": None,
        "maternalStatus": None,
        "metricType": {"Name": hpc_type, "HPCType": hpc_type},
    }


@pytest.fixture
def error_handler():
    with HDXErrorHandler() as eh:
        yield eh


@pytest.fixture
def plan(configuration, error_handler):
    return Plan(configuration, 2025, error_handler)


def test_process_returns_none_on_download_error(monkeypatch, plan):
    def raise_download_error(*args, **kwargs):
        raise DownloadError("network boom")

    monkeypatch.setattr(graphql_reader, "execute_query", raise_download_error)
    assert plan.process("XXX", 1) == (None, None)


def test_process_returns_none_on_graphql_error(monkeypatch, plan):
    def raise_graphql_error(*args, **kwargs):
        raise graphql_reader.GraphQLError("boom")

    monkeypatch.setattr(graphql_reader, "execute_query", raise_graphql_error)
    assert plan.process("XXX", 1) == (None, None)


def test_process_returns_none_when_no_attachments(monkeypatch, plan):
    monkeypatch.setattr(
        graphql_reader,
        "execute_query",
        lambda *a, **k: {"attachments": {"items": []}},
    )
    monkeypatch.setattr(graphql_reader, "paginate", lambda *a, **k: [])
    assert plan.process("XXX", 1) == (None, None)


def test_process_no_cluster_warning(monkeypatch, plan, error_handler):
    attachments = [
        {
            "Id": 999,
            "Name": "Mystery Sector",
            "EntityMainType": "CoordinationEntity",
            "CustomReference": "5",  # no dash -> unresolvable cluster code
            "HasDisaggregatedData": False,
        }
    ]
    monkeypatch.setattr(
        graphql_reader,
        "execute_query",
        lambda *a, **k: {"attachments": {"items": attachments}},
    )
    monkeypatch.setattr(graphql_reader, "paginate", lambda *a, **k: [])

    published, rows = plan.process("XXX", 1)
    assert (
        rows[("", "", "Mystery Sector", "")]["Info"] == "No cluster for attachment 999"
    )
    assert (
        "HumanitarianNeeds - HPC - caseload Mystery Sector no cluster for attachment 999 in XXX"
        in error_handler.shared_errors["warning"]["HumanitarianNeeds - HPC"]
    )


def test_process_unknown_location_error(monkeypatch, plan, error_handler):
    attachments = [
        {
            "Id": 1,
            "Name": "Education",
            "EntityMainType": "CoordinationEntity",
            "CustomReference": "1-EDU",
            "HasDisaggregatedData": True,
        }
    ]
    facts = [_fact(1, "inNeed", 5.0, location=None, location_id=123)]
    monkeypatch.setattr(
        graphql_reader,
        "execute_query",
        lambda *a, **k: {"attachments": {"items": attachments}},
    )
    monkeypatch.setattr(graphql_reader, "paginate", lambda *a, **k: facts)

    published, rows = plan.process("XXX", 1)
    assert (
        "HumanitarianNeeds - HPC - unknown location 123 in XXX"
        in error_handler.shared_errors["error"]["HumanitarianNeeds - HPC"]
    )
    # only the national/total row should be present, no disaggregated row for
    # the unresolvable location
    assert len(rows) == 1


def test_process_max_admin_exceeded_raises(monkeypatch, plan):
    attachments = [
        {
            "Id": 1,
            "Name": "Education",
            "EntityMainType": "CoordinationEntity",
            "CustomReference": "1-EDU",
            "HasDisaggregatedData": True,
        }
    ]
    location = {"Name": "Too Deep", "AdminLevel": 6, "Pcode": "XX000000"}
    facts = [_fact(1, "inNeed", 5.0, location=location, location_id=1)]
    monkeypatch.setattr(
        graphql_reader,
        "execute_query",
        lambda *a, **k: {"attachments": {"items": attachments}},
    )
    monkeypatch.setattr(graphql_reader, "paginate", lambda *a, **k: facts)

    with pytest.raises(ValueError, match="Admin level: 6"):
        plan.process("XXX", 1)


def test_process_pcode_filter_skips_non_matching_rows(
    monkeypatch, configuration, error_handler
):
    plan = Plan(configuration, 2025, error_handler, pcodes_to_process=["AF01"])
    attachments = [
        {
            "Id": 1,
            "Name": "Education",
            "EntityMainType": "CoordinationEntity",
            "CustomReference": "1-EDU",
            "HasDisaggregatedData": True,
        }
    ]
    facts = [
        _fact(
            1,
            "inNeed",
            10.0,
            location={"Name": "Kabul", "AdminLevel": 1, "Pcode": "AF01"},
            location_id=1,
        ),
        _fact(
            1,
            "inNeed",
            20.0,
            location={"Name": "Other", "AdminLevel": 1, "Pcode": "AF02"},
            location_id=2,
        ),
    ]
    monkeypatch.setattr(
        graphql_reader,
        "execute_query",
        lambda *a, **k: {"attachments": {"items": attachments}},
    )
    monkeypatch.setattr(graphql_reader, "paginate", lambda *a, **k: facts)

    published, rows = plan.process("AFG", 1)
    pcodes_in_rows = {key[0] for key in rows if key[0]}
    assert pcodes_in_rows == {"AF01"}


def test_process_ignores_disaggregation_when_not_published(monkeypatch, plan):
    attachments = [
        {
            "Id": 1,
            "Name": "Education",
            "EntityMainType": "CoordinationEntity",
            "CustomReference": "1-EDU",
            "HasDisaggregatedData": False,
        }
    ]
    facts = [
        _fact(1, "inNeed", 100.0, is_total=True),
        _fact(
            1,
            "inNeed",
            10.0,
            location={"Name": "Kabul", "AdminLevel": 1, "Pcode": "AF01"},
            location_id=1,
        ),
    ]
    monkeypatch.setattr(
        graphql_reader,
        "execute_query",
        lambda *a, **k: {"attachments": {"items": attachments}},
    )
    monkeypatch.setattr(graphql_reader, "paginate", lambda *a, **k: facts)

    published, rows = plan.process("XXX", 1)
    assert len(rows) == 1
    national_row = rows[("", "EDU", "Education", "")]
    assert national_row["In Need"] == 100.0
