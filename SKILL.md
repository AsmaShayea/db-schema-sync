# db-schema-sync

A developer tool for comparing and synchronizing database schemas across environments.

## Description

`db-schema-sync` helps development teams keep database schemas consistent across development, staging, and production environments. It compares table structures, indexes, constraints, and generates migration scripts to resolve differences.

## Features

- **Schema Comparison**: Compare schemas between two database connections
- **Diff Generation**: Generate human-readable diffs of schema differences
- **Migration Scripts**: Auto-generate SQL migration scripts
- **Multi-Database Support**: PostgreSQL, MySQL, SQLite
- **CI/CD Integration**: Exit codes for pipeline integration

## Usage

### Compare Two Databases

```bash
python schema_sync.py compare \
  --source "postgresql://localhost/dev_db" \
  --target "postgresql://localhost/staging_db"
```

### Generate Migration Script

```bash
python schema_sync.py migrate \
  --source "postgresql://localhost/dev_db" \
  --target "postgresql://localhost/staging_db" \
  --output migrations/
```

### Validate Schema Consistency

```bash
python schema_sync.py validate \
  --config schema_config.yaml
```

## Configuration

Create a `schema_config.yaml` file:

```yaml
environments:
  dev:
    url: ${DEV_DATABASE_URL}
  staging:
    url: ${STAGING_DATABASE_URL}
  production:
    url: ${PROD_DATABASE_URL}

ignore_tables:
  - schema_migrations
  - ar_internal_metadata

compare_options:
  check_indexes: true
  check_constraints: true
  check_defaults: false
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Schemas match |
| 1 | Schema differences found |
| 2 | Connection error |
| 3 | Configuration error |

## Requirements

- Python 3.8+
- psycopg2 (PostgreSQL)
- mysql-connector-python (MySQL)
- PyYAML

## Installation

```bash
pip install -r requirements.txt
```

## License

MIT License
