# db-schema-sync

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

A developer tool for comparing and synchronizing database schemas across environments.

## Why db-schema-sync?

Managing database schemas across multiple environments (dev, staging, production) is a common pain point:

- **Schema drift**: Changes made in one environment don't make it to others
- **Migration gaps**: Missing or incomplete migration scripts
- **CI/CD failures**: Deployments fail due to schema mismatches

`db-schema-sync` solves these problems by providing automated schema comparison and migration generation.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Compare two databases
python schema_sync.py compare \
  --source "postgresql://localhost/dev_db" \
  --target "postgresql://localhost/staging_db"

# Generate migration script
python schema_sync.py migrate \
  --source "postgresql://localhost/dev_db" \
  --target "postgresql://localhost/staging_db" \
  --output migrations/
```

## Features

### Schema Comparison

Compare table structures, columns, indexes, and constraints between any two databases:

```bash
python schema_sync.py compare -s $DEV_DB_URL -t $STAGING_DB_URL
```

Output:
```
📗 Tables to add:
  + user_preferences
  + audit_logs

📙 Tables to modify:
  ~ users
      + column: last_login_at
      ~ column: email (type changed)
```

### Migration Generation

Automatically generate SQL migration scripts:

```bash
python schema_sync.py migrate -s $DEV_DB_URL -t $STAGING_DB_URL -o migrations/
```

### Multi-Environment Validation

Validate schema consistency across all environments using a config file:

```yaml
# schema_config.yaml
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
```

```bash
python schema_sync.py validate --config schema_config.yaml
```

## Supported Databases

| Database | Status |
|----------|--------|
| PostgreSQL | ✅ Full support |
| MySQL | ✅ Full support |
| SQLite | 🚧 Coming soon |

## CI/CD Integration

Use exit codes to integrate with your CI/CD pipeline:

| Code | Meaning |
|------|---------|
| 0 | Schemas match |
| 1 | Schema differences found |
| 2 | Connection error |
| 3 | Configuration error |

Example GitHub Actions workflow:

```yaml
- name: Check schema consistency
  run: |
    python schema_sync.py compare \
      --source ${{ secrets.DEV_DB_URL }} \
      --target ${{ secrets.STAGING_DB_URL }}
```

## Installation

```bash
git clone https://github.com/AsmaShayea/db-schema-sync.git
cd db-schema-sync
pip install -r requirements.txt
```

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details.

## License

MIT License - see [LICENSE](LICENSE) for details.
