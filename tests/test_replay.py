from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import Manifest, SeriesSpec
from fred_pipeline.pipeline import FredPipeline
from fred_pipeline.replay import replay_from_bronze


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


def _manifest(spec):
    return Manifest.from_dict({"name": "t", "series": [spec.to_dict()]})


def _payload():
    return {
        "observations": [
            {
                "date": "2024-01-01",
                "value": "1.0",
                "realtime_start": "2024-02-01",
                "realtime_end": "9999-12-31",
            },
            {
                "date": "2024-01-02",
                "value": "2.0",
                "realtime_start": "2024-02-01",
                "realtime_end": "9999-12-31",
            },
        ]
    }


class Client:
    def __init__(self, payload):
        self.payload = payload

    def get_observations(self, series_id, **kw):
        return self.payload


def test_replay_rebuilds_silver_from_bronze(tmp_path):
    cfg = _config()
    db = str(tmp_path / "f.db")
    spec = SeriesSpec(series_id="X", title="X", frequency="d")

    # 1. normal run populates bronze + silver
    wh = LocalWarehouse(cfg, db_path=db)
    FredPipeline(cfg, client=Client(_payload()), warehouse=wh).run([spec])
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 2
    bronze_before = wh.query("SELECT count(*) c FROM bronze_fred_api_response")[0]["c"]
    assert bronze_before == 1

    # 2. simulate a Silver loss (e.g., dropped for a transform fix)
    wh.conn.execute("DELETE FROM silver_fred_observation")
    wh.conn.commit()
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 0

    # 3. replay from bronze — no client involved
    result = replay_from_bronze(cfg, [_manifest(spec)], wh)
    assert result["bronze_payloads_replayed"] == 1
    assert result["gold_rebuilt"] is True
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 2
    replayed_capture = wh.query(
        "SELECT DISTINCT ingested_at FROM silver_fred_observation_history ORDER BY ingested_at"
    )
    original_capture = wh.query(
        "SELECT DISTINCT ingested_at FROM bronze_fred_api_response ORDER BY ingested_at"
    )
    assert replayed_capture == original_capture
    # gold rebuilt too
    assert wh.query("SELECT count(*) c FROM gold_fred_latest_observation")[0]["c"] == 2
    wh.close()


def test_replay_routes_by_source(tmp_path, fake_session_cls, fake_response_cls):
    """A BLS payload archived in Bronze must be re-normalized with the BLS
    normalizer on replay, not the FRED one."""
    from fred_pipeline.sources.bls import BLSClient

    cfg = _config()
    db = str(tmp_path / "f.db")
    spec = SeriesSpec(
        series_id="CUUR0000SA0",
        title="CPI",
        frequency="m",
        source="bls",
        vintage_enabled=False,
    )
    bls_payload = {
        "status": "REQUEST_SUCCEEDED",
        "message": [],
        "Results": {
            "series": [
                {
                    "seriesID": "CUUR0000SA0",
                    "data": [
                        {"year": "2024", "period": "M12", "value": "315.6"},
                        {"year": "2024", "period": "M11", "value": "314.4"},
                    ],
                }
            ]
        },
    }
    session = fake_session_cls([fake_response_cls(bls_payload)])
    bls = BLSClient(session=session, sleep=lambda _s: None)

    wh = LocalWarehouse(cfg, db_path=db)
    FredPipeline(cfg, clients={"bls": bls}, warehouse=wh).run(
        [spec], build_gold_layer=False
    )
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 2

    # drop silver, replay from the archived BLS bronze payload (no client)
    wh.conn.execute("DELETE FROM silver_fred_observation")
    wh.conn.commit()
    result = replay_from_bronze(cfg, [_manifest(spec)], wh, rebuild_gold=False)

    assert result["bronze_payloads_replayed"] == 1
    rows = wh.query(
        "SELECT source, count(*) c FROM silver_fred_observation GROUP BY source"
    )
    # re-derived correctly as BLS rows (FRED normalizer would have raised on the
    # missing 'observations' key and produced nothing)
    assert rows == [{"source": "bls", "c": 2}]
    wh.close()


def test_replay_is_idempotent_and_filters_series(tmp_path):
    cfg = _config()
    db = str(tmp_path / "f.db")
    specs = [
        SeriesSpec(series_id="A", title="A", frequency="d"),
        SeriesSpec(series_id="B", title="B", frequency="d"),
    ]
    man = Manifest.from_dict({"name": "t", "series": [s.to_dict() for s in specs]})

    wh = LocalWarehouse(cfg, db_path=db)
    FredPipeline(cfg, client=Client(_payload()), warehouse=wh).run(specs)

    # replay only A, twice -> still 2 rows for A, no growth (idempotent)
    replay_from_bronze(cfg, [man], wh, series_ids=["A"])
    replay_from_bronze(cfg, [man], wh, series_ids=["A"])
    a = wh.query("SELECT count(*) c FROM silver_fred_observation WHERE series_id='A'")
    assert a[0]["c"] == 2
    wh.close()
