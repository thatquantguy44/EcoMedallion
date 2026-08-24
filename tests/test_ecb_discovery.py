import json

import pytest

from fred_pipeline.catalogs.ecb_discovery import (
    ECBDiscoveryError,
    ECBMetadataClient,
    ecb_manifest_to_yaml,
    estimate_candidate_count,
    filter_dataflows,
    generate_ecb_candidate_specs,
    parse_dataflow_structure_xml,
    parse_dataflows_xml,
    parse_dimension_filters,
)
from fred_pipeline.manifest import Manifest

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

STRUCTURE_XML = """<?xml version='1.0' encoding='UTF-8'?>
<mes:Structure
  xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
  xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
  xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
  <mes:Structures>
    <str:Dataflows>
      <str:Dataflow agencyID="ECB" id="EXR" version="1.0">
        <com:Name xml:lang="en">Exchange Rates</com:Name>
        <str:Structure>
          <Ref package="datastructure" agencyID="ECB" id="ECB_EXR1" version="1.0" class="DataStructure"/>
        </str:Structure>
      </str:Dataflow>
    </str:Dataflows>
    <str:Codelists>
      <str:Codelist agencyID="ECB" id="CL_FREQ" version="1.0">
        <str:Code id="D"><com:Name xml:lang="en">Daily</com:Name></str:Code>
        <str:Code id="M"><com:Name xml:lang="en">Monthly</com:Name></str:Code>
      </str:Codelist>
      <str:Codelist agencyID="ECB" id="CL_CURRENCY" version="1.0">
        <str:Code id="EUR"><com:Name xml:lang="en">Euro</com:Name></str:Code>
        <str:Code id="NOK"><com:Name xml:lang="en">Norwegian krone</com:Name></str:Code>
        <str:Code id="USD"><com:Name xml:lang="en">US dollar</com:Name></str:Code>
      </str:Codelist>
      <str:Codelist agencyID="ECB" id="CL_EXR_TYPE" version="1.0">
        <str:Code id="SP00"><com:Name xml:lang="en">Spot</com:Name></str:Code>
      </str:Codelist>
      <str:Codelist agencyID="ECB" id="CL_EXR_SUFFIX" version="1.0">
        <str:Code id="A"><com:Name xml:lang="en">Average</com:Name></str:Code>
      </str:Codelist>
    </str:Codelists>
    <str:DataStructures>
      <str:DataStructure agencyID="ECB" id="ECB_EXR1" version="1.0">
        <str:DataStructureComponents>
          <str:DimensionList id="DimensionDescriptor">
            <str:Dimension id="FREQ" position="1">
              <str:LocalRepresentation><str:Enumeration>
                <Ref package="codelist" agencyID="ECB" id="CL_FREQ" version="1.0" class="Codelist"/>
              </str:Enumeration></str:LocalRepresentation>
            </str:Dimension>
            <str:Dimension id="CURRENCY" position="2">
              <str:LocalRepresentation><str:Enumeration>
                <Ref package="codelist" agencyID="ECB" id="CL_CURRENCY" version="1.0" class="Codelist"/>
              </str:Enumeration></str:LocalRepresentation>
            </str:Dimension>
            <str:Dimension id="CURRENCY_DENOM" position="3">
              <str:LocalRepresentation><str:Enumeration>
                <Ref package="codelist" agencyID="ECB" id="CL_CURRENCY" version="1.0" class="Codelist"/>
              </str:Enumeration></str:LocalRepresentation>
            </str:Dimension>
            <str:Dimension id="EXR_TYPE" position="4">
              <str:LocalRepresentation><str:Enumeration>
                <Ref package="codelist" agencyID="ECB" id="CL_EXR_TYPE" version="1.0" class="Codelist"/>
              </str:Enumeration></str:LocalRepresentation>
            </str:Dimension>
            <str:Dimension id="EXR_SUFFIX" position="5">
              <str:LocalRepresentation><str:Enumeration>
                <Ref package="codelist" agencyID="ECB" id="CL_EXR_SUFFIX" version="1.0" class="Codelist"/>
              </str:Enumeration></str:LocalRepresentation>
            </str:Dimension>
          </str:DimensionList>
        </str:DataStructureComponents>
      </str:DataStructure>
    </str:DataStructures>
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


def test_parse_dataflow_structure_xml_maps_dimensions_and_codes():
    structure = parse_dataflow_structure_xml(STRUCTURE_XML, flow_ref="EXR")
    assert structure.flow_id == "EXR"
    assert structure.structure_id == "ECB_EXR1"
    assert [dim.dimension_id for dim in structure.dimensions] == [
        "FREQ",
        "CURRENCY",
        "CURRENCY_DENOM",
        "EXR_TYPE",
        "EXR_SUFFIX",
    ]
    assert structure.dimensions[1].codelist_id == "CL_CURRENCY"
    assert [code.code_id for code in structure.dimensions[1].codes] == [
        "EUR",
        "NOK",
        "USD",
    ]


def test_generate_ecb_candidate_specs_filters_bounds_and_excludes_existing():
    structure = parse_dataflow_structure_xml(STRUCTURE_XML, flow_ref="EXR")
    filters = parse_dimension_filters(
        [
            "CURRENCY=USD,NOK",
            "CURRENCY_DENOM=EUR",
            "EXR_TYPE=SP00",
            "EXR_SUFFIX=A",
        ]
    )
    assert (
        estimate_candidate_count(
            structure, dimension_filters=filters, frequencies=["d"]
        )
        == 2
    )
    specs, skipped = generate_ecb_candidate_specs(
        structure,
        dimension_filters=filters,
        frequencies=["d"],
        max_cartesian=10,
        exclude_ids={"ECB:EXR:D.USD.EUR.SP00.A"},
    )
    assert [spec.series_id for spec in specs] == ["ECB:EXR:D.NOK.EUR.SP00.A"]
    assert specs[0].active is False
    assert specs[0].source == "ecb"
    assert specs[0].frequency == "d"
    assert skipped == [
        {
            "series_id": "ECB:EXR:D.USD.EUR.SP00.A",
            "reason": "already in manifest",
        }
    ]


def test_generate_ecb_candidate_specs_refuses_unsafe_cartesian_expansion():
    structure = parse_dataflow_structure_xml(STRUCTURE_XML, flow_ref="EXR")
    filters = parse_dimension_filters(
        ["CURRENCY_DENOM=EUR", "EXR_TYPE=SP00", "EXR_SUFFIX=A"]
    )
    with pytest.raises(ECBDiscoveryError, match="above --max-cartesian"):
        generate_ecb_candidate_specs(
            structure,
            dimension_filters=filters,
            frequencies=["d"],
            max_cartesian=1,
        )


def test_ecb_manifest_yaml_round_trips_through_manifest_validation():
    structure = parse_dataflow_structure_xml(STRUCTURE_XML, flow_ref="EXR")
    filters = parse_dimension_filters(
        ["CURRENCY=NOK", "CURRENCY_DENOM=EUR", "EXR_TYPE=SP00", "EXR_SUFFIX=A"]
    )
    specs, _skipped = generate_ecb_candidate_specs(
        structure,
        dimension_filters=filters,
        frequencies=["d"],
    )
    manifest = {
        "name": "ecb_exr_candidates",
        "description": "test",
        "version": 1,
        "series": [spec.to_dict() for spec in specs],
    }
    yaml_text = ecb_manifest_to_yaml(manifest)
    loaded = Manifest.from_dict(__import__("yaml").safe_load(yaml_text))
    assert loaded.series_ids(active_only=False) == ["ECB:EXR:D.NOK.EUR.SP00.A"]


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
    from fred_pipeline import cli

    assert cli.main(["discover-ecb"]) == 2


def test_discover_ecb_cli_inspects_flow_as_json(monkeypatch, capsys):
    from fred_pipeline import cli

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def list_dataflows(self):
            return parse_dataflows_xml(DATAFLOW_XML)

        def get_dataflow_structure(self, flow_ref, *, agency_id="ECB", version="1.0"):
            assert (flow_ref, agency_id, version) == ("EXR", "ECB", "1.0")
            return parse_dataflow_structure_xml(STRUCTURE_XML, flow_ref=flow_ref)

    monkeypatch.setattr(
        "fred_pipeline.catalogs.ecb_discovery.ECBMetadataClient", FakeClient
    )
    rc = cli.main(["discover-ecb", "--flow", "EXR", "--inspect", "--json"])
    assert rc == 0
    row = json.loads(capsys.readouterr().out)
    assert row["flow_id"] == "EXR"
    assert row["dimensions"][0]["dimension_id"] == "FREQ"
