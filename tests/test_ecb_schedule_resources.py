from pathlib import Path

import yaml

from fred_pipeline.governance.alerting_config import load_alerting_config


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
    assert settings["ecb_assets_authoritative_backend"] == "postgres"
    postgres = settings["backends"]["postgres"]
    assert postgres == {
        "target": "service",
        "dsn_env": "FRED_POSTGRES_SERVICE_DSN",
    }
    assert "dsn" not in postgres


def test_ecb_schedule_env_pins_authoritative_backend_to_postgres():
    env = (ROOT / "resources" / "ecb-assets.env.example").read_text()
    assert "FRED_ECB_ASSETS_AUTHORITATIVE_BACKEND=postgres" in env


def test_free_brevo_smtp_alert_template_is_loadable_and_secret_safe():
    path = ROOT / "resources" / "ecb-alerting.brevo-smtp.yml.example"
    config = load_alerting_config(str(path))
    assert config.enabled is True
    assert config.transport.kind == "smtp"
    assert config.transport.host == "smtp-relay.brevo.com"
    assert config.transport.port == 587
    assert config.transport.use_starttls is True
    assert config.transport.password_env == "FRED_SMTP_PASSWORD"


def test_outlook_graph_alert_template_is_loadable_and_secret_safe():
    path = ROOT / "resources" / "ecb-alerting.outlook-graph.yml.example"
    config = load_alerting_config(str(path))
    assert config.enabled is True
    assert config.transport.kind == "microsoft_graph"
    assert config.transport.client_secret_env == "FRED_GRAPH_CLIENT_SECRET"
    assert config.route_for("failure").has_recipients
