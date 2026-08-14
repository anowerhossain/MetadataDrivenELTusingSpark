"""
Configuration Management Module (Schema & Parser).
Provides dataclass schemas and TOML loading with environment variable resolution.
"""

import os
import re
import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, List, Union

try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib  # type: ignore
    except ImportError:
        tomllib = None  # type: ignore


class ConfigError(ValueError):
    """Raised when configuration loading or validation fails."""
    pass


class SourceType(str, Enum):
    ORACLE = "oracle"
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    POSTGRES = "postgres"
    SQLSERVER = "sqlserver"
    MSSQL = "mssql"
    SFTP = "sftp"

    @classmethod
    def from_string(cls, val: Any) -> "SourceType":
        if not isinstance(val, str):
            raise ConfigError(f"Field 'type' in section '[source]' must be a string, got {type(val).__name__}.")
        normalized = val.strip().lower()
        for member in cls:
            if member.value == normalized:
                return member
        valid_values = [m.value for m in cls]
        raise ConfigError(f"Invalid source.type '{val}'. Allowed values: {valid_values}")


class TargetType(str, Enum):
    ICEBERG = "iceberg"

    @classmethod
    def from_string(cls, val: Any) -> "TargetType":
        if not isinstance(val, str):
            raise ConfigError(f"Field 'type' in section '[target]' must be a string, got {type(val).__name__}.")
        normalized = val.strip().lower()
        for member in cls:
            if member.value == normalized:
                return member
        valid_values = [m.value for m in cls]
        raise ConfigError(f"Invalid target.type '{val}'. Allowed values: {valid_values}")


class LoadType(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    UPSERT = "upsert"

    @classmethod
    def from_string(cls, val: Any) -> "LoadType":
        if not isinstance(val, str):
            raise ConfigError(f"Field 'type' in section '[load]' must be a string, got {type(val).__name__}.")
        normalized = val.strip().lower()
        for member in cls:
            if member.value == normalized:
                return member
        valid_values = [m.value for m in cls]
        raise ConfigError(f"Invalid load.type '{val}'. Allowed values: {valid_values}")


@dataclass(frozen=True)
class JobSection:
    job_id: str
    job_name: str
    enabled: bool
    description: Optional[str] = ""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobSection":
        if not isinstance(data, dict):
            raise ConfigError("Section '[job]' must be a dictionary.")

        for req_field in ("job_id", "job_name", "enabled"):
            if req_field not in data:
                raise ConfigError(f"Missing required field '{req_field}' in section '[job]'.")

        job_id = data["job_id"]
        if not isinstance(job_id, str) or not job_id.strip():
            raise ConfigError("Field 'job_id' in section '[job]' must be a non-empty string.")

        job_name = data["job_name"]
        if not isinstance(job_name, str) or not job_name.strip():
            raise ConfigError("Field 'job_name' in section '[job]' must be a non-empty string.")

        enabled = data["enabled"]
        if not isinstance(enabled, bool):
            raise ConfigError("Field 'enabled' in section '[job]' must be a boolean (true/false).")

        description = str(data.get("description", "")).strip()

        return cls(
            job_id=job_id.strip(),
            job_name=job_name.strip(),
            enabled=enabled,
            description=description,
        )


@dataclass(frozen=True)
class SourceExtractionSection:
    columns: Optional[List[str]] = None
    exclude_columns: Optional[List[str]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SourceExtractionSection":
        if not isinstance(data, dict):
            raise ConfigError("Sub-section '[source.extraction]' must be a dictionary.")
        
        raw_cols = data.get("columns")
        cols = None
        if raw_cols is not None:
            if isinstance(raw_cols, list):
                cols = [str(c).strip() for c in raw_cols if str(c).strip()]
            elif isinstance(raw_cols, str) and raw_cols.strip():
                cols = [raw_cols.strip()]

        raw_excl = data.get("exclude_columns") or data.get("exclude")
        excl = None
        if raw_excl is not None:
            if isinstance(raw_excl, list):
                excl = [str(c).strip() for c in raw_excl if str(c).strip()]
            elif isinstance(raw_excl, str) and raw_excl.strip():
                excl = [raw_excl.strip()]

        return cls(columns=cols, exclude_columns=excl)


@dataclass(frozen=True)
class JDBCSection:
    partition_column: Optional[str] = None
    lower_bound: Optional[int] = None
    upper_bound: Optional[int] = None
    num_partitions: int = 4
    fetch_size: int = 10000

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JDBCSection":
        if not isinstance(data, dict):
            raise ConfigError("Sub-section '[source.jdbc]' must be a dictionary.")

        part_col = data.get("partition_column") or data.get("split_column")
        if part_col and isinstance(part_col, str):
            part_col = part_col.strip()
            if not part_col:
                part_col = None

        num_parts = data.get("num_partitions") or data.get("partitions", 4)
        try:
            num_parts = int(num_parts)
            if num_parts <= 0:
                num_parts = 4
        except (ValueError, TypeError):
            num_parts = 4

        fetch_size = data.get("fetch_size") or data.get("fetchsize", 10000)
        try:
            fetch_size = int(fetch_size)
            if fetch_size <= 0:
                fetch_size = 10000
        except (ValueError, TypeError):
            fetch_size = 10000

        lower_b = data.get("lower_bound")
        if lower_b is not None:
            try:
                lower_b = int(lower_b)
            except (ValueError, TypeError):
                lower_b = None

        upper_b = data.get("upper_bound")
        if upper_b is not None:
            try:
                upper_b = int(upper_b)
            except (ValueError, TypeError):
                upper_b = None

        return cls(
            partition_column=part_col,
            lower_bound=lower_b,
            upper_bound=upper_b,
            num_partitions=num_parts,
            fetch_size=fetch_size,
        )


@dataclass(frozen=True)
class SourceSFTPSection:
    path: str = "/remote/incoming/"
    file_pattern: str = "*.csv"
    file_format: str = "csv"
    delimiter: str = ","
    header: bool = True
    encoding: str = "utf-8"
    sheet_name: Union[str, int] = "0"
    header_row: int = 0
    audit_table: str = "hive.etl_audit.sftp_file_audit"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SourceSFTPSection":
        if not isinstance(data, dict):
            raise ConfigError("Sub-section '[source.sftp]' must be a dictionary.")

        path = str(data.get("path") or data.get("directory", "/remote/incoming/")).strip()
        pattern = str(data.get("file_pattern") or data.get("pattern", "*.csv")).strip()
        fmt = str(data.get("file_format") or data.get("format", "csv")).strip().lower()
        delimiter = str(data.get("delimiter", ",")).strip() or ","
        header = bool(data.get("header", True))
        encoding = str(data.get("encoding", "utf-8")).strip() or "utf-8"
        sheet = data.get("sheet_name", "0")
        header_row = int(data.get("header_row", 0))
        audit_tbl = str(data.get("audit_table", "hive.etl_audit.sftp_file_audit")).strip()

        return cls(
            path=path,
            file_pattern=pattern,
            file_format=fmt,
            delimiter=delimiter,
            header=header,
            encoding=encoding,
            sheet_name=sheet,
            header_row=header_row,
            audit_table=audit_tbl,
        )


@dataclass(frozen=True)
class SourceSection:
    type: SourceType
    connection: str
    schema: str
    table: str
    extraction: SourceExtractionSection = field(default_factory=SourceExtractionSection)
    jdbc: JDBCSection = field(default_factory=JDBCSection)
    sftp: SourceSFTPSection = field(default_factory=SourceSFTPSection)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SourceSection":
        if not isinstance(data, dict):
            raise ConfigError("Section '[source]' must be a dictionary.")

        for req_field in ("type", "connection", "schema", "table"):
            if req_field not in data:
                raise ConfigError(f"Missing required field '{req_field}' in section '[source]'.")

        source_type = SourceType.from_string(data["type"])

        connection = data["connection"]
        if not isinstance(connection, str) or not connection.strip():
            raise ConfigError("Field 'connection' in section '[source]' must be a non-empty string.")

        schema = data["schema"]
        if not isinstance(schema, str) or not schema.strip():
            raise ConfigError("Field 'schema' in section '[source]' must be a non-empty string.")

        table = data["table"]
        if not isinstance(table, str) or not table.strip():
            raise ConfigError("Field 'table' in section '[source]' must be a non-empty string.")

        extraction = SourceExtractionSection.from_dict(data["extraction"]) if "extraction" in data else SourceExtractionSection()
        jdbc = JDBCSection.from_dict(data["jdbc"]) if "jdbc" in data else JDBCSection()
        sftp = SourceSFTPSection.from_dict(data["sftp"]) if "sftp" in data else SourceSFTPSection()

        return cls(
            type=source_type,
            connection=connection.strip(),
            schema=schema.strip(),
            table=table.strip(),
            extraction=extraction,
            jdbc=jdbc,
            sftp=sftp,
        )


@dataclass(frozen=True)
class IncrementalLoadSection:
    column: Optional[str] = None
    watermark_type: str = "timestamp"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IncrementalLoadSection":
        if not isinstance(data, dict):
            raise ConfigError("Sub-section '[load.incremental]' must be a dictionary.")

        column = data.get("column") or data.get("watermark_column")
        if column and isinstance(column, str):
            column = column.strip()
            if not column:
                column = None

        watermark_type = str(data.get("watermark_type", "timestamp")).strip().lower()
        if watermark_type not in ("timestamp", "numeric", "date"):
            watermark_type = "timestamp"

        return cls(column=column, watermark_type=watermark_type)


@dataclass(frozen=True)
class LoadSection:
    type: LoadType
    watermark_column: Optional[str] = None
    incremental: Optional[IncrementalLoadSection] = None
    merge_keys: Optional[List[str]] = None
    options: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LoadSection":
        if not isinstance(data, dict):
            raise ConfigError("Section '[load]' must be a dictionary.")

        if "type" not in data:
            raise ConfigError("Missing required field 'type' in section '[load]'.")

        load_type = LoadType.from_string(data["type"])

        inc_section = IncrementalLoadSection.from_dict(data["incremental"]) if "incremental" in data else None

        wm_col = data.get("watermark_column")
        if inc_section and inc_section.column:
            wm_col = inc_section.column

        if wm_col and isinstance(wm_col, str):
            wm_col = wm_col.strip()
            if not wm_col:
                wm_col = None

        if load_type in (LoadType.INCREMENTAL, LoadType.UPSERT) and not wm_col:
            raise ConfigError(f"Load type '{load_type.value}' requires 'load.incremental.column'.")

        raw_keys = data.get("merge_keys") or data.get("merge_key")
        merge_keys = None
        if raw_keys is not None:
            if isinstance(raw_keys, list):
                merge_keys = [str(k).strip() for k in raw_keys if str(k).strip()]
            elif isinstance(raw_keys, str) and raw_keys.strip():
                merge_keys = [raw_keys.strip()]

        if load_type == LoadType.UPSERT and not merge_keys:
            raise ConfigError("Load type 'upsert' requires 'merge_keys' to perform Iceberg MERGE INTO.")

        options = data.get("options", {})
        if not isinstance(options, dict):
            options = {}

        return cls(
            type=load_type,
            watermark_column=wm_col,
            incremental=inc_section,
            merge_keys=merge_keys,
            options=options,
        )


@dataclass(frozen=True)
class TargetPartitionSection:
    type: Optional[str] = None
    column: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TargetPartitionSection":
        if not isinstance(data, dict):
            raise ConfigError("Sub-section '[target.partition]' must be a dictionary.")

        part_type = data.get("type")
        if part_type and isinstance(part_type, str):
            part_type = part_type.strip().lower()

        part_col = data.get("column")
        if part_col and isinstance(part_col, str):
            part_col = part_col.strip()

        return cls(type=part_type, column=part_col)


@dataclass(frozen=True)
class TargetMaintenanceSection:
    enabled: bool = True
    compact_small_files: bool = True
    target_file_size_mb: int = 128
    rewrite_manifests: bool = True

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TargetMaintenanceSection":
        if not isinstance(data, dict):
            raise ConfigError("Sub-section '[target.maintenance]' must be a dictionary.")

        enabled = bool(data.get("enabled", True))
        compact = bool(data.get("compact_small_files", True))
        
        file_size = data.get("target_file_size_mb", 128)
        try:
            file_size = int(file_size)
        except (ValueError, TypeError):
            file_size = 128

        manifests = bool(data.get("rewrite_manifests", True))

        return cls(
            enabled=enabled,
            compact_small_files=compact,
            target_file_size_mb=file_size,
            rewrite_manifests=manifests,
        )


@dataclass(frozen=True)
class TargetSection:
    catalog: str
    database: str
    table: str
    type: TargetType = TargetType.ICEBERG
    partition: TargetPartitionSection = field(default_factory=TargetPartitionSection)
    maintenance: TargetMaintenanceSection = field(default_factory=TargetMaintenanceSection)
    options: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TargetSection":
        if not isinstance(data, dict):
            raise ConfigError("Section '[target]' must be a dictionary.")

        for req_field in ("catalog", "database", "table"):
            if req_field not in data:
                raise ConfigError(f"Missing required field '{req_field}' in section '[target]'.")

        catalog = data["catalog"]
        if not isinstance(catalog, str) or not catalog.strip():
            raise ConfigError("Field 'catalog' in section '[target]' must be a non-empty string.")

        database = data["database"]
        if not isinstance(database, str) or not database.strip():
            raise ConfigError("Field 'database' in section '[target]' must be a non-empty string.")

        table = data["table"]
        if not isinstance(table, str) or not table.strip():
            raise ConfigError("Field 'table' in section '[target]' must be a non-empty string.")

        target_type = TargetType.ICEBERG
        if "type" in data:
            target_type = TargetType.from_string(data["type"])

        partition = TargetPartitionSection.from_dict(data["partition"]) if "partition" in data else TargetPartitionSection()
        maintenance = TargetMaintenanceSection.from_dict(data["maintenance"]) if "maintenance" in data else TargetMaintenanceSection()

        options = data.get("options", {})
        if not isinstance(options, dict):
            options = {}

        return cls(
            catalog=catalog.strip(),
            database=database.strip(),
            table=table.strip(),
            type=target_type,
            partition=partition,
            maintenance=maintenance,
            options=options,
        )


@dataclass(frozen=True)
class KeysSection:
    primary_key: Optional[List[str]] = None
    merge_keys: Optional[List[str]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "KeysSection":
        if not isinstance(data, dict):
            raise ConfigError("Section '[keys]' must be a dictionary.")

        raw_pk = data.get("primary_key") or data.get("primary_keys")
        pk_list = None
        if raw_pk is not None:
            if isinstance(raw_pk, list):
                pk_list = [str(k).strip() for k in raw_pk if str(k).strip()]
            elif isinstance(raw_pk, str) and raw_pk.strip():
                pk_list = [raw_pk.strip()]

        raw_mk = data.get("merge_keys") or data.get("merge_key")
        mk_list = None
        if raw_mk is not None:
            if isinstance(raw_mk, list):
                mk_list = [str(k).strip() for k in raw_mk if str(k).strip()]
            elif isinstance(raw_mk, str) and raw_mk.strip():
                mk_list = [raw_mk.strip()]

        return cls(primary_key=pk_list, merge_keys=mk_list)


@dataclass(frozen=True)
class TransformSection:
    rename: Dict[str, str] = field(default_factory=dict)
    cast: Dict[str, str] = field(default_factory=dict)
    derived: Dict[str, str] = field(default_factory=dict)
    exclude: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TransformSection":
        if not isinstance(data, dict):
            raise ConfigError("Section '[transform]' must be a dictionary.")

        rename_map = data.get("rename", {})
        if not isinstance(rename_map, dict):
            rename_map = {}
        rename_map = {str(k).strip(): str(v).strip() for k, v in rename_map.items()}

        cast_map = data.get("cast", {})
        if not isinstance(cast_map, dict):
            cast_map = {}
        cast_map = {str(k).strip(): str(v).strip() for k, v in cast_map.items()}

        derived_map = data.get("derived", {})
        if not isinstance(derived_map, dict):
            derived_map = {}
        derived_map = {str(k).strip(): str(v).strip() for k, v in derived_map.items()}

        raw_excl = data.get("exclude", [])
        exclude_list = []
        if isinstance(raw_excl, list):
            exclude_list = [str(x).strip() for x in raw_excl if str(x).strip()]
        elif isinstance(raw_excl, str) and raw_excl.strip():
            exclude_list = [raw_excl.strip()]

        return cls(
            rename=rename_map,
            cast=cast_map,
            derived=derived_map,
            exclude=exclude_list,
        )


@dataclass(frozen=True)
class AuditColumnsSection:
    enabled: bool = True
    insert_ts_column: str = "dwh_insert_ts"
    updated_ts_column: str = "dwh_updated_ts"
    job_id_column: str = "dwh_etl_run_id"
    source_system_column: str = "dwh_job_user"
    run_id_column: str = "dwh_etl_run_id"
    job_user_column: str = "dwh_job_user"
    timezone: str = "Asia/Dhaka"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AuditColumnsSection":
        if not isinstance(data, dict):
            return cls()

        enabled = bool(data.get("enabled", True))
        ins_ts = str(data.get("insert_ts_column", data.get("insert_timestamp", "dwh_insert_ts"))).strip()
        upd_ts = str(data.get("updated_ts_column", data.get("updated_timestamp", "dwh_updated_ts"))).strip()
        job_id_col = str(data.get("job_id_column", data.get("run_id_column", data.get("etl_run_id", "dwh_etl_run_id")))).strip()
        source_sys_col = str(data.get("source_system_column", data.get("job_user_column", data.get("source_system", "dwh_job_user")))).strip()
        run_id_col = str(data.get("run_id_column", job_id_col)).strip()
        job_usr_col = str(data.get("job_user_column", source_sys_col)).strip()
        tz = str(data.get("timezone", "Asia/Dhaka")).strip()

        return cls(
            enabled=enabled,
            insert_ts_column=ins_ts,
            updated_ts_column=upd_ts,
            job_id_column=job_id_col,
            source_system_column=source_sys_col,
            run_id_column=run_id_col,
            job_user_column=job_usr_col,
            timezone=tz,
        )


@dataclass(frozen=True)
class SchemaSection:
    evolution: bool = True
    add_columns: bool = True
    allow_nullable: bool = True

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SchemaSection":
        if not isinstance(data, dict):
            raise ConfigError("Section '[schema]' must be a dictionary.")

        evolution = bool(data.get("evolution", True))
        add_cols = bool(data.get("add_columns", True))
        nullable = bool(data.get("allow_nullable", True))

        return cls(evolution=evolution, add_columns=add_cols, allow_nullable=nullable)


@dataclass(frozen=True)
class PreloadSection:
    enabled: bool = True
    operations: List[str] = field(default_factory=lambda: ["validate_source", "validate_target"])

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PreloadSection":
        if not isinstance(data, dict):
            raise ConfigError("Section '[preload]' must be a dictionary.")

        enabled = bool(data.get("enabled", True))
        ops_raw = data.get("operations", ["validate_source", "validate_target"])
        ops_list = []
        if isinstance(ops_raw, list):
            ops_list = [str(o).strip() for o in ops_raw if str(o).strip()]

        return cls(enabled=enabled, operations=ops_list)


@dataclass(frozen=True)
class PostloadSection:
    enabled: bool = True
    operations: List[str] = field(default_factory=lambda: ["update_watermark", "compact_table"])

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PostloadSection":
        if not isinstance(data, dict):
            raise ConfigError("Section '[postload]' must be a dictionary.")

        enabled = bool(data.get("enabled", True))
        ops_raw = data.get("operations", ["update_watermark", "compact_table"])
        ops_list = []
        if isinstance(ops_raw, list):
            ops_list = [str(o).strip() for o in ops_raw if str(o).strip()]

        return cls(enabled=enabled, operations=ops_list)


@dataclass(frozen=True)
class QualitySection:
    enabled: bool = True
    null_check: List[str] = field(default_factory=list)
    unique_check: List[str] = field(default_factory=list)
    minimum_rows: int = 0
    check_null_keys: bool = False
    check_duplicate_keys: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QualitySection":
        if not isinstance(data, dict):
            return cls()

        enabled = bool(data.get("enabled", True))
        null_check = [str(k).strip() for k in (data.get("null_check") or []) if str(k).strip()]
        unique_check = [str(k).strip() for k in (data.get("unique_check") or []) if str(k).strip()]

        try:
            minimum_rows = int(data.get("minimum_rows", 0))
        except (ValueError, TypeError):
            minimum_rows = 0

        check_null_keys = bool(data.get("check_null_keys", False))
        check_duplicate_keys = bool(data.get("check_duplicate_keys", False))

        return cls(
            enabled=enabled,
            null_check=null_check,
            unique_check=unique_check,
            minimum_rows=minimum_rows,
            check_null_keys=check_null_keys,
            check_duplicate_keys=check_duplicate_keys,
        )


# Backward compatibility alias
LoadIncrementalSection = IncrementalLoadSection


@dataclass(frozen=True)
class MaintenanceSection:
    enabled: bool = True
    target_file_size_mb: int = 128
    rewrite_manifests: bool = True
    orphan_file_retention_days: int = 3

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MaintenanceSection":
        if not isinstance(data, dict):
            return cls()
        return cls(
            enabled=bool(data.get("enabled", True)),
            target_file_size_mb=int(data.get("target_file_size_mb", 128)),
            rewrite_manifests=bool(data.get("rewrite_manifests", True)),
            orphan_file_retention_days=int(data.get("orphan_file_retention_days", 3)),
        )


@dataclass(frozen=True)
class ExecutionSection:
    retries: int = 3
    retry_delay_seconds: float = 30.0
    backoff_multiplier: float = 2.0
    exponential_backoff: bool = True

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutionSection":
        if not isinstance(data, dict):
            raise ConfigError("Section '[execution]' must be a dictionary.")

        retries = data.get("retries", 3)
        try:
            retries = int(retries)
        except (ValueError, TypeError):
            retries = 3

        delay = data.get("retry_delay_seconds", 30.0)
        try:
            delay = float(delay)
        except (ValueError, TypeError):
            delay = 30.0

        mult = data.get("backoff_multiplier", 2.0)
        try:
            mult = float(mult)
        except (ValueError, TypeError):
            mult = 2.0

        exp_backoff = bool(data.get("exponential_backoff", True))

        return cls(
            retries=retries,
            retry_delay_seconds=delay,
            backoff_multiplier=mult,
            exponential_backoff=exp_backoff,
        )


@dataclass(frozen=True)
class RetrySection:
    max_attempts: int = 3
    delay_seconds: float = 30.0
    backoff_multiplier: float = 2.0
    exponential_backoff: bool = True

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RetrySection":
        if not isinstance(data, dict):
            raise ConfigError("Section '[retry]' must be a dictionary.")

        attempts = data.get("max_attempts") or data.get("retries", 3)
        try:
            attempts = int(attempts)
        except (ValueError, TypeError):
            attempts = 3

        delay = data.get("delay_seconds") or data.get("retry_delay_seconds", 30.0)
        try:
            delay = float(delay)
        except (ValueError, TypeError):
            delay = 30.0

        mult = data.get("backoff_multiplier", 2.0)
        try:
            mult = float(mult)
        except (ValueError, TypeError):
            mult = 2.0

        exp = bool(data.get("exponential_backoff", True))

        return cls(
            max_attempts=attempts,
            delay_seconds=delay,
            backoff_multiplier=mult,
            exponential_backoff=exp,
        )


@dataclass(frozen=True)
class ResourceSection:
    profile: Optional[str] = None
    executor_memory: Optional[str] = None
    shuffle_partitions: Optional[int] = None
    custom_spark_options: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ResourceSection":
        if not isinstance(data, dict):
            return cls()

        profile = str(data.get("profile", "auto")).strip().lower()

        exec_mem = data.get("executor_memory")
        if exec_mem and isinstance(exec_mem, str):
            exec_mem = exec_mem.strip()

        shuffle = data.get("shuffle_partitions")
        if shuffle is not None:
            try:
                shuffle = int(shuffle)
            except (ValueError, TypeError):
                shuffle = None

        custom_opts = data.get("custom_spark_options", {})
        if not isinstance(custom_opts, dict):
            custom_opts = {}

        return cls(
            profile=profile,
            executor_memory=exec_mem if exec_mem else None,
            shuffle_partitions=shuffle,
            custom_spark_options=custom_opts,
        )


@dataclass(frozen=True)
class EmailEventSection:
    event: str = "on_failure"
    enabled: bool = True
    sender: str = "noreply@company.com"
    to: List[str] = field(default_factory=list)
    cc: List[str] = field(default_factory=list)
    bcc: List[str] = field(default_factory=list)
    subject_prefix: str = "[ETL ALERT]"
    template: str = "job_failed"
    subject: Optional[str] = None
    body_template: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any], default_sender: str = "noreply@company.com") -> "EmailEventSection":
        if not isinstance(data, dict):
            return cls()

        def parse_list(val: Any) -> List[str]:
            if isinstance(val, str):
                return [v.strip() for v in val.split(",") if v.strip()]
            elif isinstance(val, list):
                return [str(v).strip() for v in val if str(v).strip()]
            return []

        event = str(data.get("event") or "on_failure").strip().lower()
        enabled = bool(data.get("enabled", True))
        sender = str(data.get("from") or data.get("sender") or default_sender).strip()
        to_list = parse_list(data.get("to"))
        cc_list = parse_list(data.get("cc"))
        bcc_list = parse_list(data.get("bcc"))

        # Default template based on event type
        default_tmpl = "job_failed"
        if event in ("on_success", "success"):
            default_tmpl = "job_success"
        elif event in ("on_quality_failure", "data_quality_failed"):
            default_tmpl = "data_quality_failed"
        elif event in ("on_sla_breach", "sla_breached"):
            default_tmpl = "sla_breached"
        elif event in ("on_missing_file", "missing_file"):
            default_tmpl = "missing_file"

        template = str(data.get("template") or default_tmpl).strip().lower()
        subject_prefix = str(data.get("subject_prefix") or "[ETL ALERT]").strip()
        subject_pat = str(data.get("subject")).strip() if data.get("subject") else None
        body_tmpl = str(data.get("body_template")).strip() if data.get("body_template") else None

        return cls(
            event=event,
            enabled=enabled,
            sender=sender,
            to=to_list,
            cc=cc_list,
            bcc=bcc_list,
            subject_prefix=subject_prefix,
            template=template,
            subject=subject_pat,
            body_template=body_tmpl,
        )


@dataclass(frozen=True)
class EmailNotificationSection:
    enabled: bool = False
    sender: str = "noreply@company.com"
    to: List[str] = field(default_factory=list)
    cc: List[str] = field(default_factory=list)
    bcc: List[str] = field(default_factory=list)
    subject_prefix: str = "[ETL JOB FAILURE]"
    template: str = "job_failed"
    subject: Optional[str] = None
    body_template: Optional[str] = None
    events: List[EmailEventSection] = field(default_factory=list)

    def get_event_config(self, event_or_status: str) -> Optional[EmailEventSection]:
        """
        Resolves matching EmailEventSection for event name (e.g. 'on_failure', 'on_success')
        or pipeline status ('FAILED', 'SUCCESS', 'DATA_QUALITY_FAILED', 'SLA_BREACHED', 'MISSING_FILE').
        """
        if not self.enabled:
            return None

        # Map status to event name
        status_map = {
            "failed": "on_failure",
            "failure": "on_failure",
            "on_failure": "on_failure",
            "success": "on_success",
            "on_success": "on_success",
            "data_quality_failed": "on_quality_failure",
            "quality_failed": "on_quality_failure",
            "on_quality_failure": "on_quality_failure",
            "sla_breached": "on_sla_breach",
            "on_sla_breach": "on_sla_breach",
            "missing_file": "on_missing_file",
            "on_missing_file": "on_missing_file",
            "data_anomaly": "on_anomaly",
            "on_anomaly": "on_anomaly",
        }
        target_event = status_map.get(str(event_or_status).strip().lower(), str(event_or_status).strip().lower())

        # Match specific event from self.events list
        for ev in self.events:
            if ev.event.strip().lower() == target_event and ev.enabled:
                return ev

        # Fallback to single top-level event configuration
        default_ev = EmailEventSection(
            event=target_event,
            enabled=self.enabled,
            sender=self.sender,
            to=self.to,
            cc=self.cc,
            bcc=self.bcc,
            subject_prefix=self.subject_prefix,
            template=self.template,
            subject=self.subject,
            body_template=self.body_template,
        )

        return default_ev if default_ev.to else None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EmailNotificationSection":
        if not isinstance(data, dict):
            return cls()

        enabled = bool(data.get("enabled", False))
        sender = str(data.get("from") or data.get("sender") or "noreply@company.com").strip()

        def parse_list(val: Any) -> List[str]:
            if isinstance(val, str):
                return [v.strip() for v in val.split(",") if v.strip()]
            elif isinstance(val, list):
                return [str(v).strip() for v in val if str(v).strip()]
            return []

        to_list = parse_list(data.get("to"))
        cc_list = parse_list(data.get("cc"))
        bcc_list = parse_list(data.get("bcc"))
        subject_prefix = str(data.get("subject_prefix") or "[ETL JOB FAILURE]").strip()
        template = str(data.get("template") or "job_failed").strip().lower()
        subject_pat = str(data.get("subject")).strip() if data.get("subject") else None
        body_tmpl = str(data.get("body_template")).strip() if data.get("body_template") else None

        # Parse event sub-tables [[email_notification.events]] or [email_notification.events]
        events_data = data.get("events")
        parsed_events: List[EmailEventSection] = []
        if isinstance(events_data, list):
            for ed in events_data:
                if isinstance(ed, dict):
                    parsed_events.append(EmailEventSection.from_dict(ed, default_sender=sender))
        elif isinstance(events_data, dict):
            for ev_name, ed in events_data.items():
                if isinstance(ed, dict):
                    ed_copy = dict(ed)
                    ed_copy["event"] = ev_name
                    parsed_events.append(EmailEventSection.from_dict(ed_copy, default_sender=sender))

        return cls(
            enabled=enabled,
            sender=sender,
            to=to_list,
            cc=cc_list,
            bcc=bcc_list,
            subject_prefix=subject_prefix,
            template=template,
            subject=subject_pat,
            body_template=body_tmpl,
            events=parsed_events,
        )


@dataclass(frozen=True)
class JobConfig:
    job: JobSection
    source: SourceSection
    load: LoadSection
    target: TargetSection
    keys: KeysSection = field(default_factory=KeysSection)
    transform: TransformSection = field(default_factory=TransformSection)
    audit_columns: AuditColumnsSection = field(default_factory=AuditColumnsSection)
    schema_config: SchemaSection = field(default_factory=SchemaSection)
    preload: PreloadSection = field(default_factory=PreloadSection)
    postload: PostloadSection = field(default_factory=PostloadSection)
    quality: QualitySection = field(default_factory=QualitySection)
    execution: ExecutionSection = field(default_factory=ExecutionSection)
    retry: RetrySection = field(default_factory=RetrySection)
    resources: ResourceSection = field(default_factory=ResourceSection)
    email_notification: EmailNotificationSection = field(default_factory=EmailNotificationSection)
    spark: Dict[str, Any] = field(default_factory=dict)
    jdbc: Optional[JDBCSection] = None

    @property
    def resource(self) -> ResourceSection:
        return self.resources

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobConfig":
        if not isinstance(data, dict):
            raise ConfigError("TOML configuration root must be a dictionary.")

        for req_sec in ("job", "source", "load", "target"):
            if req_sec not in data:
                raise ConfigError(f"Missing required section '[{req_sec}]' in TOML configuration.")

        job_sec = JobSection.from_dict(data["job"])
        source_sec = SourceSection.from_dict(data["source"])
        load_sec = LoadSection.from_dict(data["load"])
        target_sec = TargetSection.from_dict(data["target"])

        keys_sec = KeysSection.from_dict(data["keys"]) if "keys" in data else KeysSection()
        transform_sec = TransformSection.from_dict(data["transform"]) if "transform" in data else TransformSection()

        audit_data = data.get("audit_columns") or data.get("audit")
        audit_sec = AuditColumnsSection.from_dict(audit_data) if audit_data else AuditColumnsSection()

        schema_sec = SchemaSection.from_dict(data["schema"]) if "schema" in data else SchemaSection()
        preload_sec = PreloadSection.from_dict(data["preload"]) if "preload" in data else PreloadSection()
        postload_sec = PostloadSection.from_dict(data["postload"]) if "postload" in data else PostloadSection()

        quality_data = data.get("data_quality") or data.get("quality")
        quality_sec = QualitySection.from_dict(quality_data) if quality_data else QualitySection()

        exec_sec = ExecutionSection.from_dict(data["execution"]) if "execution" in data else ExecutionSection()
        retry_sec = RetrySection.from_dict(data["retry"]) if "retry" in data else RetrySection()
        resource_sec = ResourceSection.from_dict(data["resources"]) if "resources" in data else ResourceSection()

        email_data = data.get("email_notification") or data.get("email")
        email_sec = EmailNotificationSection.from_dict(email_data) if email_data else EmailNotificationSection()

        spark_dict = data.get("spark", {})
        jdbc_sec = source_sec.jdbc

        if keys_sec.merge_keys and not load_sec.merge_keys:
            load_sec = LoadSection(
                type=load_sec.type,
                watermark_column=load_sec.watermark_column,
                incremental=load_sec.incremental,
                merge_keys=keys_sec.merge_keys,
            )

        return cls(
            job=job_sec,
            source=source_sec,
            load=load_sec,
            target=target_sec,
            keys=keys_sec,
            transform=transform_sec,
            audit_columns=audit_sec,
            schema_config=schema_sec,
            preload=preload_sec,
            postload=postload_sec,
            quality=quality_sec,
            execution=exec_sec,
            retry=retry_sec,
            resources=resource_sec,
            email_notification=email_sec,
            spark=spark_dict,
            jdbc=jdbc_sec,
        )

    @property
    def data_quality(self) -> QualitySection:
        """Convenience alias for self.quality."""
        return self.quality


class ConfigParser:
    """Parses and validates TOML configuration files with environment variable substitution."""

    ENV_PATTERN = re.compile(r"\$\{([A-Za-z0-9_]+)(?::([^}]+))?\}")

    @classmethod
    def resolve_env_vars(cls, val: Any) -> Any:
        """Recursively substitutes environment variable placeholders in configuration values."""
        if isinstance(val, str):
            def replace_match(match_obj: re.Match) -> str:
                env_var = match_obj.group(1)
                default_val = match_obj.group(2)
                return os.environ.get(env_var, default_val if default_val is not None else "")
            return cls.ENV_PATTERN.sub(replace_match, val)
        elif isinstance(val, dict):
            return {k: cls.resolve_env_vars(v) for k, v in val.items()}
        elif isinstance(val, list):
            return [cls.resolve_env_vars(v) for v in val]
        return val

    @classmethod
    def parse_dict(cls, data: Dict[str, Any]) -> JobConfig:
        """Resolves env vars and validates configuration dictionary into a JobConfig object."""
        resolved_data = cls.resolve_env_vars(data)
        return JobConfig.from_dict(resolved_data)

    @classmethod
    def load_toml(cls, config_path: Union[str, Path]) -> JobConfig:
        """Loads, parses, resolves env vars, and validates a TOML configuration file into a JobConfig object."""
        file_path = Path(config_path)
        if not file_path.is_file():
            raise ConfigError(f"Configuration file not found at: {config_path}")

        if tomllib is None:
            raise ConfigError(
                "TOML parser library is unavailable. On Python < 3.11, please ensure 'tomli' is installed."
            )

        try:
            with open(file_path, "rb") as f:
                raw_data = tomllib.load(f)
        except Exception as err:
            if isinstance(err, ConfigError):
                raise err
            raise ConfigError(f"Failed to parse TOML file '{config_path}': {err}") from err

        return cls.parse_dict(raw_data)
