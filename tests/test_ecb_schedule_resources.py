from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_ecb_timer_uses_frankfurt_weekday_retries():
    timer = (ROOT / "resources" / "ecb-eligible-assets.timer").read_text()
    calendars = [
        line.split("=", 1)[1]
        for line in timer.splitlines()
        if line.startswith("OnCalendar=")
    ]
    assert calendars == [
        "Mon..Fri *-*-* 17:45:00 Europe/Berlin",
        "Mon..Fri *-*-* 18:25:00 Europe/Berlin",
        "Mon..Fri *-*-* 21:20:00 Europe/Berlin",
    ]
    assert "Persistent=true" in timer


def test_ecb_service_runs_dedicated_production_poll():
    service = (ROOT / "resources" / "ecb-eligible-assets.service").read_text()
    assert "EnvironmentFile=/etc/fred-pipeline/ecb-assets.env" in service
    assert "-m fred_pipeline ecb-assets poll --env prod" in service
    assert "Type=oneshot" in service


def test_ecb_schedule_warehouse_uses_secret_dsn_env():
    path = ROOT / "resources" / "ecb-assets-warehouse.yml.example"
    config = yaml.safe_load(path.read_text())
    settings = config["default"]
    assert settings["primary_backend"] == "postgres"
    postgres = settings["backends"]["postgres"]
    assert postgres == {
        "target": "service",
        "dsn_env": "FRED_POSTGRES_SERVICE_DSN",
    }
    assert "dsn" not in postgres
