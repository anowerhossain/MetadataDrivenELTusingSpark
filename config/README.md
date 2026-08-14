# `config` - Configuration Directory Root

This directory contains metadata configuration files that govern the extraction, transformation, quality validation, and loading behavior of the ETL framework.

## Structure

```text
config/
└── jobs/       # TOML job configuration files (e.g. customer.toml, mysql_orders.toml)
```

## Configuration Concept

The framework is **100% metadata-driven**. No Python code changes are required to add new ingestion pipelines or database sources. Adding a new table ingestion job is as simple as creating a new `.toml` file inside `config/jobs/`.

---

## Sub-Directories

- [`config/jobs/`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/config/jobs/README.md): Contains individual TOML configuration files defining specific data pipeline pipelines.
