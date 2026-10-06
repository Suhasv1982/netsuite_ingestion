"""Per-environment names the tools read. Names only: hosts, ids and emails are resolved at runtime and never stored."""

from __future__ import annotations

from dataclasses import dataclass, field

DEFAULT_LIMIT = 50
HARD_LIMIT = 200


@dataclass(frozen=True)
class EnvConfig:
    catalog: str
    jobs: dict[str, str]                 # tool alias -> job name
    pipelines: dict[str, str]            # alias -> pipeline name
    source_endpoint: str
    metadata_endpoint: str
    deploy_workflows: tuple[str, ...]    # GitHub Actions workflow files that deploy this env
    business_keys: dict[str, str] = field(default_factory=dict)   # incremental table -> key column
    full_load_tables: tuple[str, ...] = ()

    @property
    def tables(self) -> tuple[str, ...]:
        return self.full_load_tables + tuple(self.business_keys)


BUSINESS_KEYS = {
    "netsuite_memberships": "membership_internal_id",
    "netsuite_certifications": "certification_internal_id",
    "netsuite_transactions": "transaction_internal_id",
    "netsuite_transaction_lines": "transaction_line_id",
}

ENVS: dict[str, EnvConfig] = {
    "dev": EnvConfig(
        catalog="workspace",
        jobs={
            "generator": "[dev ci_dev] netsuite_daily_generator",
            "ingestion": "[dev ci_dev] netsuite_ingestion_daily",
            "canary": "[dev ci_dev] guard_canary_check",
        },
        pipelines={
            "ingestion": "[dev ci_dev] netsuite_ingestion_poc",
            "canary": "[dev ci_dev] guard_canary",
        },
        source_endpoint="projects/netsuite-sample/branches/production/endpoints/primary",
        metadata_endpoint="projects/aidq-metadata/branches/dev/endpoints/primary",
        deploy_workflows=("deploy-dev.yml",),
        business_keys=BUSINESS_KEYS,
        full_load_tables=("netsuite_customers",),
    ),
}


class ConfigError(ValueError):
    """A caller argument outside what this server allows (returned to the client as status: error)."""


def env_config(env: str) -> EnvConfig:
    if env == "prod":
        raise ConfigError("prod not enabled in phase 3")
    if env not in ENVS:
        raise ConfigError(f"unknown env {env!r}; allowed: {sorted(ENVS)}")
    return ENVS[env]


def check_table(cfg: EnvConfig, table: str | None) -> tuple[str, ...]:
    """The tables a call covers: all of them, or the one named (must be in the allowlist)."""
    if table is None:
        return cfg.tables
    if table not in cfg.tables:
        raise ConfigError(f"unknown table {table!r}; allowed: {list(cfg.tables)}")
    return (table,)


def check_alias(kind: str, aliases: dict[str, str], alias: str | None) -> dict[str, str]:
    if alias is None:
        return dict(aliases)
    if alias not in aliases:
        raise ConfigError(f"unknown {kind} {alias!r}; allowed: {sorted(aliases)}")
    return {alias: aliases[alias]}


def clamp(name: str, value: int | None, default: int, maximum: int, minimum: int = 1) -> int:
    """A bounded integer argument: default when None, ConfigError outside [minimum, maximum]."""
    if value is None:
        return default
    if not isinstance(value, int) or isinstance(value, bool) or not minimum <= value <= maximum:
        raise ConfigError(f"{name} must be an integer from {minimum} to {maximum}")
    return value
