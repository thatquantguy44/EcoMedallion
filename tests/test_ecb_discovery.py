import json

import pytest

from fred_pipeline.catalogs.ecb_discovery import (
    ECBDiscoveryError,
    ECBMetadataClient,
    filter_dataflows,
    parse_dataflows_xml,
)

DATAFLOW_XML = """<?xml version='1.0' encoding='UTF-8'?>
<mes:Structure
  xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
  xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
  xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
  <mes:Structures>
    <str:Dataflows>
      <str:Dataflow
        agencyID="ECB"
        id="EXR"
        version="1.0"
        structureURL="https://data-api.ecb.europa.eu/service/dataflow/ECB/EXR/1.0">
        <com:Name xml:lang="en">Exchange Rates</com:Name>
      </str:Dataflow>
      <str:Dataflow agencyID="ECB" id="FM" version="1.0">
        <com:Name xml:lang="en">Financial market data</com:Name>
        <com:Description xml:lang="en">Money market and securities data</com:Description>
      </str:Dataflow>
    </str:Dataflows>
  </mes:Structures>
</mes:Structure>
"""


class _Response:
    status_code = 200
    text = DATAFLOW_XML


class _Session:
    def __init__(self):
        self.calls = []

    def get(self, url, params=None, timeout=None, headers=None):
        self.calls.append(
            {"url": url, "params": params, "timeout": timeout, "headers": headers}
        )
        return _Response()


def test_parse_dataflows_xml_maps_dataflow_fields():
    flows = parse_dataflows_xml(DATAFLOW_XML)
    assert [f.flow_id for f in flows] == ["EXR", "FM"]
    assert flows[0].agency_id == "ECB"
    assert flows[0].version == "1.0"
    assert flows[0].name == "Exchange Rates"
    assert flows[0].structure_url.endswith("/dataflow/ECB/EXR/1.0")
    assert flows[1].description == "Money market and securities data"


def test_parse_dataflows_xml_raises_clear_error_for_bad_xml():
    with pytest.raises(ECBDiscoveryError, match="Could not parse ECB dataflow XML"):
        parse_dataflows_xml("<not xml")


def test_filter_dataflows_searches_id_name_description_and_caps():
    flows = parse_dataflows_xml(DATAFLOW_XML)
    assert [f.flow_id for f in filter_dataflows(flows, search="exchange")] == ["EXR"]
    assert [f.flow_id for f in filter_dataflows(flows, search="money")] == ["FM"]
    assert [f.flow_id for f in filter_dataflows(flows, max_results=1)] == ["EXR"]


def test_metadata_client_fetches_dataflow_stubs_with_structure_accept_header():
    session = _Session()
    client = ECBMetadataClient(
        base_url="https://example.test/service",
        session=session,
        sleep=lambda _seconds: None,
    )
    flows = client.list_dataflows()
    assert [f.flow_id for f in flows] == ["EXR", "FM"]
    assert session.calls == [
        {
            "url": "https://example.test/service/dataflow",
            "params": {"detail": "allstubs"},
            "timeout": 30,
            "headers": {"Accept": "application/vnd.sdmx.structure+xml;version=2.1"},
        }
    ]


def test_discover_ecb_cli_lists_flows_as_json(monkeypatch, capsys):
    from fred_pipeline import cli

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def list_dataflows(self):
            return parse_dataflows_xml(DATAFLOW_XML)

    monkeypatch.setattr(
        "fred_pipeline.catalogs.ecb_discovery.ECBMetadataClient", FakeClient
    )
    rc = cli.main(["discover-ecb", "--list-flows", "--search", "exchange", "--json"])
    assert rc == 0
    rows = json.loads(capsys.readouterr().out)
    assert rows[0]["flow_id"] == "EXR"
    assert rows[0]["name"] == "Exchange Rates"


def test_discover_ecb_cli_requires_list_flows():
    from fred_pipeline.cli import build_parser

    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["discover-ecb"])
