"""Profile selection must apply consistently to run and ECB alerts."""

import pytest

from fred_pipeline.governance.alerting import build_message
from fred_pipeline.governance.alerting_config import (
    AlertingConfigError,
    load_alerting_config,
)
from fred_pipeline.governance.ecb_alerting import (
    ECBAssetsAssessment,
    _build_message,
    send_ecb_assets_alert,
)
from fred_pipeline.governance.email_report import RunSummary
from fred_pipeline.governance.email_transport import FileEmailTransport, build_transport


def _load(tmp_path, extra=""):
    path = tmp_path / "alerts.yml"
    path.write_text(
        """enabled: true
policy: always
default_profile: preview
profiles:
  preview:
    from_address: preview@example.com
    transport:
      kind: console
  oncall:
    from_address: alerts@example.com
    from_name: Oncall
    transport:
      kind: file
      output_dir: /tmp/alerts
routes:
  failure:
    profile: oncall
    to: ops@example.com
  success:
    to: team@example.com
"""
        + extra
    )
    return load_alerting_config(str(path))


def test_profile_routes_select_sender_and_transport_for_both_alert_types(tmp_path):
    config = _load(tmp_path)
    summary = RunSummary(
        run_id="r", environment="dev", verdict="failure", reason="test", stages=[]
    )
    run_message = build_message(summary, config)
    ecb_message = _build_message(
        ECBAssetsAssessment("failure", "test", {}), config, environment="dev"
    )
    assert run_message.from_address == ecb_message.from_address == "alerts@example.com"
    assert run_message.from_name == ecb_message.from_name == "Oncall"
    assert isinstance(build_transport(config, verdict="failure"), FileEmailTransport)
    assert config.profile_for("success").from_address == "preview@example.com"


@pytest.mark.parametrize(
    "extra",
    [
        "transport: {kind: console}\n",
        "from_address: root@example.com\n",
        "default_profile: missing\n",
    ],
)
def test_mixed_or_unknown_profiles_rejected(tmp_path, extra):
    with pytest.raises(AlertingConfigError):
        _load(tmp_path, extra)


def test_unknown_route_profile_rejected(tmp_path):
    path = tmp_path / "alerts.yml"
    path.write_text("""enabled: true
default_profile: a
profiles:
  a:
    from_address: a@example.com
    transport: {kind: console}
routes:
  failure: {to: ops@example.com, profile: missing}
""")
    with pytest.raises(AlertingConfigError, match="profile"):
        load_alerting_config(str(path))


def test_ecb_alert_uses_verdict_profile_in_actual_delivery(tmp_path):
    config = _load(tmp_path)
    from dataclasses import replace

    profile = config.profiles["oncall"]
    config.profiles["oncall"] = replace(
        profile,
        transport=replace(profile.transport, output_dir=str(tmp_path / "alerts")),
    )
    assert send_ecb_assets_alert(
        action="poll", error="failure", config=config, environment="dev"
    )
    assert list((tmp_path / "alerts").glob("*.html"))
