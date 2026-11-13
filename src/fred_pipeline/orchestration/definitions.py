"""Load with ``dagster dev -m fred_pipeline.orchestration.definitions``.

Adding a job means adding its validated config, runtime method and registry row.
Import is intentionally inert: resources resolve protected settings at execution.
"""

from dataclasses import dataclass

from dagster import (
    AssetKey,
    AssetMaterialization,
    AssetObservation,
    ConfigMapping,
    Definitions,
    Failure,
    RetryPolicy,
    ScheduleDefinition,
    DefaultScheduleStatus,
    graph,
    op,
    resource,
    Output,
)

from fred_pipeline.orchestration.config import (
    MacroPipelineConfig,
    ECBEligibleAssetsConfig,
    ECBHealthConfig,
    WarehouseHealthConfig,
    GoldRebuildConfig,
    MetadataReconciliationConfig,
    FOMCRefreshConfig,
)


@resource
def runtime_resource(_context):
    from fred_pipeline.job_runtime import JobRuntime

    return JobRuntime()


@dataclass(frozen=True)
class JobSpec:
    name: str
    config: type
    handler: str
    description: str
    pool: str | None = None


JOB_SPECS = (
    JobSpec(
        "macro_pipeline_job",
        MacroPipelineConfig,
        "macro_pipeline",
        "Manifest-driven Bronze/Silver/Gold pipeline; separate ECB ownership.",
        "warehouse_writer",
    ),
    JobSpec(
        "ecb_eligible_assets_job",
        ECBEligibleAssetsConfig,
        "ecb_eligible_assets",
        "Poll and verify signed ECB security-level eligible-assets releases.",
        "ecb_writer",
    ),
    JobSpec(
        "ecb_eligible_assets_health_job",
        ECBHealthConfig,
        "ecb_eligible_assets_health",
        "Read-only ECB accepted-state and publication health.",
    ),
    JobSpec(
        "warehouse_health_job",
        WarehouseHealthConfig,
        "warehouse_health",
        "Read-only all-source freshness, capture, DQ and Gold evidence.",
    ),
    JobSpec(
        "gold_rebuild_job",
        GoldRebuildConfig,
        "gold_rebuild",
        "Rebuild Gold from stored inputs with offline FOMC validation.",
        "warehouse_writer",
    ),
    JobSpec(
        "metadata_reconciliation_job",
        MetadataReconciliationConfig,
        "metadata_reconciliation",
        "FRED catalog drift and all-source stored staleness.",
        "warehouse_writer",
    ),
    JobSpec(
        "fomc_refresh_job",
        FOMCRefreshConfig,
        "fomc_refresh",
        "Add newly published FOMC meetings and validate calendar runway.",
        "warehouse_writer",
    ),
)


def build_job(spec: JobSpec):
    schema = spec.config.to_config_schema()
    op_name = spec.handler

    @op(
        name=op_name,
        config_schema=schema,
        required_resource_keys={"runtime"},
        pool=spec.pool,
        retry_policy=RetryPolicy(max_retries=0),
    )
    def execute(context):
        try:
            config = spec.config(**context.op_config)
            report = getattr(context.resources.runtime, spec.handler)(config)
        except Exception:
            # Provider exceptions can contain DSNs, tokens or signed URLs.
            raise Failure(
                description=f"{spec.name} failed during workload execution; inspect protected worker logs.",
                allow_retries=False,
            ) from None
        metadata = {
            "verdict": report.verdict,
            "report": report.details,
            "dagster_run_id": context.run_id,
        }
        key = AssetKey(report.asset_key or ("operations", spec.handler))
        for materialization in report.materializations:
            yield AssetMaterialization(asset_key=key, metadata=materialization)
        if not report.materializations:
            yield AssetObservation(asset_key=key, metadata=metadata)
        if report.verdict != "success":
            raise Failure(
                description=f"{spec.name} domain verdict: {report.verdict}",
                metadata=metadata,
                allow_retries=False,
            )
        yield Output(report.details, metadata=metadata)

    @graph(name=spec.name)
    def workload():
        execute()

    def map_config(cfg):
        validated = spec.config(**cfg).model_dump(exclude_none=True)
        return {"ops": {op_name: {"config": validated}}}

    return workload.to_job(
        resource_defs={"runtime": runtime_resource},
        description=spec.description,
        config=ConfigMapping(config_schema=schema, config_fn=map_config),
        tags={"dagster/max_retries": "0"},
    )


JOBS = {spec.name: build_job(spec) for spec in JOB_SPECS}
macro_pipeline_job = JOBS["macro_pipeline_job"]
ecb_eligible_assets_job = JOBS["ecb_eligible_assets_job"]
ecb_eligible_assets_health_job = JOBS["ecb_eligible_assets_health_job"]
warehouse_health_job = JOBS["warehouse_health_job"]
gold_rebuild_job = JOBS["gold_rebuild_job"]
metadata_reconciliation_job = JOBS["metadata_reconciliation_job"]
fomc_refresh_job = JOBS["fomc_refresh_job"]

# Production activation and missed-tick recovery remain deployment gates.
ecb_schedules = [
    ScheduleDefinition(
        name=f"ecb_assets_{hour:02d}{minute:02d}",
        job=ecb_eligible_assets_job,
        cron_schedule=f"{minute} {hour} * * 1-5",
        execution_timezone="Europe/Berlin",
        default_status=DefaultScheduleStatus.STOPPED,
    )
    for hour, minute in ((17, 45), (18, 25), (21, 20))
]
defs = Definitions(jobs=list(JOBS.values()), schedules=ecb_schedules)
