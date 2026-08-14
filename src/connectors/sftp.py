"""
SFTP Connection & File Ingestion Connector Module.
Connects to remote SFTP servers (or local staging paths), lists files matching path and file_pattern,
extracts exact file metadata, prevents duplicate loading of unchanged files, supports CSV and Excel (.xlsx) parsing,
and logs execution telemetry into an Apache Iceberg audit table.
"""

import os
import glob
import fnmatch
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import paramiko
except ImportError:
    paramiko = None

from src.helpers.logger import setup_logger
from src.core.config import SourceSection, SourceSFTPSection, ConfigError
from src.core.state import WatermarkManager

logger = setup_logger("SFTPConnector")


@dataclass
class SFTPConnectionConfig:
    """Holds validated SFTP connection properties safely without exposing secrets in logs."""
    connection_name: str
    host: str
    port: int = 22
    username: str = ""
    password: Optional[str] = field(default=None, repr=False)
    key_path: Optional[str] = field(default=None)

    def __repr__(self) -> str:
        return (
            f"SFTPConnectionConfig(connection_name='{self.connection_name}', "
            f"host='{self.host}', port={self.port}, username='{self.username}', "
            f"key_path='{self.key_path}')"
        )


class SFTPConnectionResolver:
    """Resolves SFTP connection credentials from environment variables."""

    @classmethod
    def resolve(cls, source_config: SourceSection) -> SFTPConnectionConfig:
        connection_name = source_config.connection
        if not connection_name:
            raise ConfigError("Source section is missing 'connection' name.")

        prefix = connection_name.strip().upper().replace("-", "_")
        if not prefix.startswith("SFTP_"):
            prefix = f"SFTP_{prefix}"

        host = os.getenv(f"{prefix}_HOST", os.getenv(f"{prefix}_SERVER", "localhost"))
        port_str = os.getenv(f"{prefix}_PORT", "22")
        username = os.getenv(f"{prefix}_USERNAME", os.getenv(f"{prefix}_USER", ""))
        password = os.getenv(f"{prefix}_PASSWORD", os.getenv(f"{prefix}_PASS", None))
        key_path = os.getenv(f"{prefix}_KEY_PATH", os.getenv(f"{prefix}_KEY_FILE", None))

        try:
            port = int(port_str)
        except (ValueError, TypeError):
            port = 22

        conn_obj = SFTPConnectionConfig(
            connection_name=connection_name,
            host=host,
            port=port,
            username=username,
            password=password,
            key_path=key_path,
        )
        logger.info(f"Resolved SFTP connection successfully: {conn_obj}")
        return conn_obj


@dataclass
class SFTPFileMetadata:
    """Captured file metadata strictly according to requirements."""
    file_name: str
    file_path: str
    file_size: int
    last_modified: str
    file_owner: Optional[str] = None  # None if unavailable, DO NOT guess


class SFTPAuditLogger:
    """Logs SFTP ingestion file metadata telemetry into an Apache Iceberg audit table."""

    @classmethod
    def log_audit(
        cls,
        spark: Any = None,
        audit_table: str = "",
        metadata: Optional[SFTPFileMetadata] = None,
        target_table: str = "",
        record_count: int = 0,
        status: str = "SUCCESS",
        error_message: Optional[str] = None,
        spark_session: Any = None
    ) -> None:
        effective_spark = spark if spark is not None else spark_session
        load_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        file_name = metadata.file_name if metadata else ""
        file_path = metadata.file_path if metadata else ""
        file_size = metadata.file_size if metadata else 0
        last_modified = metadata.last_modified if metadata else ""
        file_owner = metadata.file_owner if metadata else None

        audit_row = {
            "file_name": file_name,
            "file_path": file_path,
            "file_size_bytes": file_size,
            "last_modified_ts": last_modified,
            "file_owner": file_owner,
            "target_table": target_table,
            "load_timestamp": load_time,
            "record_count": record_count,
            "status": status,
            "error_message": error_message,
        }

        if effective_spark is not None:
            try:
                audit_df = effective_spark.createDataFrame([audit_row])
                effective_spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {audit_table} ("
                    f"file_name STRING, file_path STRING, file_size_bytes BIGINT, "
                    f"last_modified_ts STRING, file_owner STRING, target_table STRING, "
                    f"load_timestamp STRING, record_count BIGINT, status STRING, error_message STRING) "
                    f"USING iceberg"
                )
                audit_df.write.format("iceberg").mode("append").saveAsTable(audit_table)
                logger.info(f"Logged SFTP file audit record into Iceberg table '{audit_table}'.")
            except Exception as err:
                logger.warning(f"Could not log SFTP audit telemetry into Iceberg table '{audit_table}': {err}")
        else:
            logger.info(
                f"[SFTP Audit Stub] target='{target_table}', file='{file_name}', "
                f"size={file_size}, rows={record_count}, status='{status}'"
            )


class SFTPReader:
    """
    SFTP File Ingestion Connector for CSV and Excel (.xlsx) files.
    Discovers matching files, tracks watermark fingerprints to avoid duplicate loads,
    parses data, and writes file audit logs into Iceberg.
    """

    def __init__(self, spark_session: Any, source_config: SourceSection):
        if not isinstance(source_config, SourceSection):
            raise ConfigError(f"Expected SourceSection configuration, got {type(source_config).__name__}.")
        self.spark = spark_session
        self.source_config = source_config
        self.sftp_config = source_config.sftp
        self.conn_config = SFTPConnectionResolver.resolve(source_config)
        self.watermark_mgr = WatermarkManager(self.spark)

    def find_files(self) -> List[Tuple[str, SFTPFileMetadata]]:
        """
        Discovers files matching path + file_pattern on remote SFTP server or local staging path.
        Extracts exact file metadata: file_name, file_size, last_modified, file_path, file_owner.
        """
        path_setting = self.sftp_config.path
        pattern = self.sftp_config.file_pattern
        matched_files: List[Tuple[str, SFTPFileMetadata]] = []

        # Check if remote SFTP client is configured and paramiko is available
        if paramiko is not None and self.conn_config.host not in ("localhost", "127.0.0.1", ""):
            try:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                if self.conn_config.key_path and os.path.exists(self.conn_config.key_path):
                    ssh.connect(
                        self.conn_config.host,
                        port=self.conn_config.port,
                        username=self.conn_config.username,
                        key_filename=self.conn_config.key_path,
                        timeout=10,
                    )
                else:
                    ssh.connect(
                        self.conn_config.host,
                        port=self.conn_config.port,
                        username=self.conn_config.username,
                        password=self.conn_config.password,
                        timeout=10,
                    )
                sftp = ssh.open_sftp()
                logger.info(f"Connected to remote SFTP host '{self.conn_config.host}:{self.conn_config.port}'.")

                file_attr_list = sftp.listdir_attr(path_setting)
                for attr in file_attr_list:
                    if fnmatch.fnmatch(attr.filename, pattern):
                        full_remote_path = os.path.join(path_setting, attr.filename).replace("\\", "/")
                        mtime_str = datetime.fromtimestamp(attr.st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

                        # Extract owner string if available from SFTP stat
                        owner_str = None
                        if hasattr(attr, "longname") and attr.longname:
                            parts = attr.longname.split()
                            if len(parts) >= 4:
                                owner_str = parts[2]
                        elif hasattr(attr, "st_uid") and attr.st_uid:
                            owner_str = str(attr.st_uid)

                        meta = SFTPFileMetadata(
                            file_name=attr.filename,
                            file_path=full_remote_path,
                            file_size=attr.st_size,
                            last_modified=mtime_str,
                            file_owner=owner_str
                        )
                        matched_files.append((full_remote_path, meta))
                sftp.close()
                ssh.close()
                return matched_files
            except Exception as err:
                logger.warning(f"Remote SFTP connection failed ({err}). Falling back to local path discovery.")

        # Local filesystem fallback (for local dev, testing, or staging directories)
        search_dir = path_setting
        if not os.path.exists(search_dir) and os.path.dirname(path_setting):
            search_dir = os.path.dirname(path_setting)
        if not search_dir or not os.path.exists(search_dir):
            search_dir = "."

        logger.info(f"Searching local staging directory '{search_dir}' with pattern '{pattern}'...")
        for root, _, files in os.walk(search_dir):
            for filename in files:
                if fnmatch.fnmatch(filename, pattern):
                    full_local_path = os.path.abspath(os.path.join(root, filename))
                    st = os.stat(full_local_path)
                    mtime_str = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

                    owner_str = None
                    try:
                        import pwd
                        owner_str = pwd.getpwuid(st.st_uid).pw_name
                    except Exception:
                        owner_str = None  # Do NOT guess if unavailable

                    meta = SFTPFileMetadata(
                        file_name=filename,
                        file_path=full_local_path,
                        file_size=st.st_size,
                        last_modified=mtime_str,
                        file_owner=owner_str
                    )
                    matched_files.append((full_local_path, meta))

        return matched_files

    def read(self, load_config: Any = None, last_watermark: Optional[str] = None) -> Any:
        """
        Scans SFTP source for files, checks deduplication state, parses CSV or Excel files,
        records Iceberg audit entries, and returns combined PySpark DataFrame.
        """
        matched_files = self.find_files()
        if not matched_files:
            logger.warning(
                f"No files found matching path '{self.sftp_config.path}' "
                f"with pattern '{self.sftp_config.file_pattern}'."
            )
            return None

        watermark_mgr = WatermarkManager(self.spark) if self.spark is not None else None

        files_to_process: List[Tuple[str, SFTPFileMetadata]] = []

        for filepath, meta in matched_files:
            state_key = f"sftp_file::{meta.file_path}"
            fingerprint = f"{meta.file_size}_{meta.last_modified}"

            previous_fingerprint = watermark_mgr.get_last_watermark(state_key) if watermark_mgr else None
            if previous_fingerprint == fingerprint:
                logger.info(f"Skipping already processed file '{meta.file_name}' (Size & MTime unchanged).")
                continue

            logger.info(f"Queuing file for processing: '{meta.file_name}' (size={meta.file_size} bytes, modified='{meta.last_modified}', owner={meta.file_owner})")
            files_to_process.append((filepath, meta))

        if not files_to_process:
            logger.info("All files in SFTP source are already processed. Returning empty dataset.")
            return None

        dataframes = []
        target_table = f"{self.source_config.schema}.{self.source_config.table}"
        file_fmt = self.sftp_config.file_format.lower()

        for filepath, meta in files_to_process:
            try:
                if file_fmt in ("excel", "xlsx"):
                    if pd is None:
                        raise ConfigError("pandas and openpyxl are required for Excel file parsing.")
                    sheet = self.sftp_config.sheet_name
                    header_row = self.sftp_config.header_row
                    logger.info(f"Parsing Excel file '{meta.file_name}' (sheet='{sheet}', header_row={header_row})...")
                    
                    # Extract openpyxl workbook document properties if file_owner is missing
                    try:
                        import openpyxl
                        wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
                        if meta.file_owner is None and wb.properties:
                            excel_author = wb.properties.lastModifiedBy or wb.properties.creator
                            if excel_author:
                                meta.file_owner = str(excel_author).strip()
                        wb.close()
                    except Exception as xl_err:
                        logger.debug(f"Could not read Excel openpyxl metadata properties for '{meta.file_name}': {xl_err}")

                    pdf = pd.read_excel(filepath, sheet_name=sheet, header=header_row)
                    pdf.columns = [str(c).strip() for c in pdf.columns]
                    
                    if self.spark is not None:
                        df = self.spark.createDataFrame(pdf)
                    else:
                        df = pdf
                else:  # CSV default
                    logger.info(
                        f"Parsing CSV file '{meta.file_name}' "
                        f"(delimiter='{self.sftp_config.delimiter}', header={self.sftp_config.header}, encoding='{self.sftp_config.encoding}')..."
                    )
                    if self.spark is not None:
                        reader_builder = (
                            self.spark.read.format("csv")
                            .option("header", str(self.sftp_config.header).lower())
                            .option("delimiter", self.sftp_config.delimiter)
                            .option("encoding", self.sftp_config.encoding)
                            .option("inferSchema", "true")
                        )
                        df = reader_builder.load(filepath)
                    else:
                        if pd is None:
                            raise ConfigError("pandas is required for local CSV testing without PySpark.")
                        pdf = pd.read_csv(
                            filepath,
                            delimiter=self.sftp_config.delimiter,
                            header=0 if self.sftp_config.header else None,
                            encoding=self.sftp_config.encoding,
                        )
                        df = pdf

                # Calculate rows in file
                rec_count = 0
                try:
                    rec_count = df.count() if hasattr(df, "count") else len(df)
                except Exception:
                    rec_count = 0

                # Record Iceberg Audit Telemetry
                SFTPAuditLogger.log_audit(
                    spark=self.spark,
                    audit_table=self.sftp_config.audit_table,
                    metadata=meta,
                    target_table=target_table,
                    record_count=rec_count,
                    status="SUCCESS",
                )

                # Update watermark state for deduplication
                if watermark_mgr is not None:
                    fingerprint = f"{meta.file_size}_{meta.last_modified}"
                    watermark_mgr.update_watermark(f"sftp_file::{meta.file_path}", fingerprint)

                dataframes.append(df)

            except Exception as err:
                logger.error(f"Failed to parse SFTP file '{meta.file_name}': {err}")
                SFTPAuditLogger.log_audit(
                    spark=self.spark,
                    audit_table=self.sftp_config.audit_table,
                    metadata=meta,
                    target_table=target_table,
                    record_count=0,
                    status="FAILED",
                    error_message=str(err),
                )
                raise err

        if not dataframes:
            return None

        if len(dataframes) == 1:
            return dataframes[0]

        # Union multiple DataFrames
        combined_df = dataframes[0]
        for next_df in dataframes[1:]:
            if hasattr(combined_df, "unionByName"):
                combined_df = combined_df.unionByName(next_df, allowMissingColumns=True)
            elif hasattr(combined_df, "append") and pd is not None:
                combined_df = pd.concat([combined_df, next_df], ignore_index=True)

        return combined_df
