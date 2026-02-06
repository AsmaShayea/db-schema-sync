#!/usr/bin/env python3
"""
db-schema-sync: Database schema comparison and synchronization tool.

Compares database schemas across environments and generates migration scripts.
"""

import argparse
import sys
import os
import yaml
from urllib.parse import urlparse
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


class DatabaseType(Enum):
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"


@dataclass
class Column:
    name: str
    data_type: str
    nullable: bool
    default: Optional[str]
    
    def __eq__(self, other):
        if not isinstance(other, Column):
            return False
        return (self.name == other.name and 
                self.data_type == other.data_type and
                self.nullable == other.nullable)


@dataclass
class Index:
    name: str
    columns: List[str]
    unique: bool


@dataclass
class Table:
    name: str
    columns: List[Column]
    indexes: List[Index]
    primary_key: List[str]


@dataclass
class SchemaDiff:
    tables_added: List[str]
    tables_removed: List[str]
    tables_modified: Dict[str, Dict]


def parse_database_url(url: str) -> Tuple[DatabaseType, dict]:
    """Parse database URL into connection parameters."""
    parsed = urlparse(url)
    
    if parsed.scheme in ('postgresql', 'postgres'):
        db_type = DatabaseType.POSTGRESQL
    elif parsed.scheme == 'mysql':
        db_type = DatabaseType.MYSQL
    elif parsed.scheme == 'sqlite':
        db_type = DatabaseType.SQLITE
    else:
        raise ValueError(f"Unsupported database type: {parsed.scheme}")
    
    params = {
        'host': parsed.hostname or 'localhost',
        'port': parsed.port,
        'database': parsed.path.lstrip('/'),
        'user': parsed.username,
        'password': parsed.password
    }
    
    return db_type, params


def get_postgresql_schema(params: dict) -> Dict[str, Table]:
    """Extract schema from PostgreSQL database."""
    try:
        import psycopg2
    except ImportError:
        print("Error: psycopg2 not installed. Run: pip install psycopg2-binary")
        sys.exit(3)
    
    conn = psycopg2.connect(
        host=params['host'],
        port=params['port'] or 5432,
        dbname=params['database'],
        user=params['user'],
        password=params['password']
    )
    
    tables = {}
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
    """)
    
    for (table_name,) in cursor.fetchall():
        # Get columns
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        
        columns = [
            Column(
                name=row[0],
                data_type=row[1],
                nullable=row[2] == 'YES',
                default=row[3]
            )
            for row in cursor.fetchall()
        ]
        
        # Get indexes
        cursor.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public' AND tablename = %s
        """, (table_name,))
        
        indexes = []
        for idx_name, idx_def in cursor.fetchall():
            unique = 'UNIQUE' in idx_def.upper()
            # Extract column names from index definition
            cols_start = idx_def.find('(') + 1
            cols_end = idx_def.find(')')
            cols = [c.strip() for c in idx_def[cols_start:cols_end].split(',')]
            indexes.append(Index(name=idx_name, columns=cols, unique=unique))
        
        # Get primary key
        cursor.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
        """, (table_name,))
        
        primary_key = [row[0] for row in cursor.fetchall()]
        
        tables[table_name] = Table(
            name=table_name,
            columns=columns,
            indexes=indexes,
            primary_key=primary_key
        )
    
    cursor.close()
    conn.close()
    
    return tables


def get_mysql_schema(params: dict) -> Dict[str, Table]:
    """Extract schema from MySQL database."""
    try:
        import mysql.connector
    except ImportError:
        print("Error: mysql-connector-python not installed. Run: pip install mysql-connector-python")
        sys.exit(3)
    
    conn = mysql.connector.connect(
        host=params['host'],
        port=params['port'] or 3306,
        database=params['database'],
        user=params['user'],
        password=params['password']
    )
    
    tables = {}
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
    """, (params['database'],))
    
    for (table_name,) in cursor.fetchall():
        # Get columns
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (params['database'], table_name))
        
        columns = [
            Column(
                name=row[0],
                data_type=row[1],
                nullable=row[2] == 'YES',
                default=row[3]
            )
            for row in cursor.fetchall()
        ]
        
        # Get indexes
        cursor.execute("""
            SELECT index_name, non_unique, column_name
            FROM information_schema.statistics
            WHERE table_schema = %s AND table_name = %s
            ORDER BY index_name, seq_in_index
        """, (params['database'], table_name))
        
        index_map = {}
        for idx_name, non_unique, col_name in cursor.fetchall():
            if idx_name not in index_map:
                index_map[idx_name] = {'unique': not non_unique, 'columns': []}
            index_map[idx_name]['columns'].append(col_name)
        
        indexes = [
            Index(name=name, columns=data['columns'], unique=data['unique'])
            for name, data in index_map.items()
        ]
        
        # Get primary key
        cursor.execute("""
            SELECT column_name
            FROM information_schema.key_column_usage
            WHERE table_schema = %s AND table_name = %s AND constraint_name = 'PRIMARY'
            ORDER BY ordinal_position
        """, (params['database'], table_name))
        
        primary_key = [row[0] for row in cursor.fetchall()]
        
        tables[table_name] = Table(
            name=table_name,
            columns=columns,
            indexes=indexes,
            primary_key=primary_key
        )
    
    cursor.close()
    conn.close()
    
    return tables


def get_schema(url: str) -> Dict[str, Table]:
    """Get schema from database URL."""
    db_type, params = parse_database_url(url)
    
    if db_type == DatabaseType.POSTGRESQL:
        return get_postgresql_schema(params)
    elif db_type == DatabaseType.MYSQL:
        return get_mysql_schema(params)
    else:
        raise NotImplementedError(f"Schema extraction not implemented for {db_type}")


def compare_schemas(source: Dict[str, Table], target: Dict[str, Table], 
                   ignore_tables: List[str] = None) -> SchemaDiff:
    """Compare two database schemas."""
    ignore_tables = ignore_tables or []
    
    source_tables = set(source.keys()) - set(ignore_tables)
    target_tables = set(target.keys()) - set(ignore_tables)
    
    tables_added = list(source_tables - target_tables)
    tables_removed = list(target_tables - source_tables)
    
    tables_modified = {}
    for table_name in source_tables & target_tables:
        source_table = source[table_name]
        target_table = target[table_name]
        
        modifications = {}
        
        # Compare columns
        source_cols = {c.name: c for c in source_table.columns}
        target_cols = {c.name: c for c in target_table.columns}
        
        cols_added = set(source_cols.keys()) - set(target_cols.keys())
        cols_removed = set(target_cols.keys()) - set(source_cols.keys())
        cols_modified = []
        
        for col_name in source_cols.keys() & target_cols.keys():
            if source_cols[col_name] != target_cols[col_name]:
                cols_modified.append(col_name)
        
        if cols_added or cols_removed or cols_modified:
            modifications['columns'] = {
                'added': list(cols_added),
                'removed': list(cols_removed),
                'modified': cols_modified
            }
        
        # Compare indexes
        source_idx = {i.name: i for i in source_table.indexes}
        target_idx = {i.name: i for i in target_table.indexes}
        
        idx_added = set(source_idx.keys()) - set(target_idx.keys())
        idx_removed = set(target_idx.keys()) - set(source_idx.keys())
        
        if idx_added or idx_removed:
            modifications['indexes'] = {
                'added': list(idx_added),
                'removed': list(idx_removed)
            }
        
        if modifications:
            tables_modified[table_name] = modifications
    
    return SchemaDiff(
        tables_added=tables_added,
        tables_removed=tables_removed,
        tables_modified=tables_modified
    )


def generate_migration_sql(diff: SchemaDiff, source: Dict[str, Table], 
                          target: Dict[str, Table]) -> str:
    """Generate SQL migration script from schema diff."""
    lines = []
    lines.append("-- Auto-generated migration script")
    lines.append("-- Generated by db-schema-sync")
    lines.append("")
    
    # Create new tables
    for table_name in diff.tables_added:
        table = source[table_name]
        lines.append(f"-- Create table: {table_name}")
        lines.append(f"CREATE TABLE {table_name} (")
        
        col_defs = []
        for col in table.columns:
            nullable = "" if col.nullable else " NOT NULL"
            default = f" DEFAULT {col.default}" if col.default else ""
            col_defs.append(f"    {col.name} {col.data_type}{nullable}{default}")
        
        if table.primary_key:
            col_defs.append(f"    PRIMARY KEY ({', '.join(table.primary_key)})")
        
        lines.append(",\n".join(col_defs))
        lines.append(");")
        lines.append("")
    
    # Drop removed tables
    for table_name in diff.tables_removed:
        lines.append(f"-- Drop table: {table_name}")
        lines.append(f"DROP TABLE IF EXISTS {table_name};")
        lines.append("")
    
    # Modify existing tables
    for table_name, mods in diff.tables_modified.items():
        lines.append(f"-- Modify table: {table_name}")
        
        if 'columns' in mods:
            for col_name in mods['columns'].get('added', []):
                col = next(c for c in source[table_name].columns if c.name == col_name)
                nullable = "" if col.nullable else " NOT NULL"
                lines.append(f"ALTER TABLE {table_name} ADD COLUMN {col.name} {col.data_type}{nullable};")
            
            for col_name in mods['columns'].get('removed', []):
                lines.append(f"ALTER TABLE {table_name} DROP COLUMN {col_name};")
        
        if 'indexes' in mods:
            for idx_name in mods['indexes'].get('added', []):
                idx = next(i for i in source[table_name].indexes if i.name == idx_name)
                unique = "UNIQUE " if idx.unique else ""
                lines.append(f"CREATE {unique}INDEX {idx_name} ON {table_name} ({', '.join(idx.columns)});")
            
            for idx_name in mods['indexes'].get('removed', []):
                lines.append(f"DROP INDEX IF EXISTS {idx_name};")
        
        lines.append("")
    
    return "\n".join(lines)


def print_diff(diff: SchemaDiff):
    """Print schema diff in human-readable format."""
    if diff.tables_added:
        print("\n📗 Tables to add:")
        for t in diff.tables_added:
            print(f"  + {t}")
    
    if diff.tables_removed:
        print("\n📕 Tables to remove:")
        for t in diff.tables_removed:
            print(f"  - {t}")
    
    if diff.tables_modified:
        print("\n📙 Tables to modify:")
        for table_name, mods in diff.tables_modified.items():
            print(f"  ~ {table_name}")
            if 'columns' in mods:
                for col in mods['columns'].get('added', []):
                    print(f"      + column: {col}")
                for col in mods['columns'].get('removed', []):
                    print(f"      - column: {col}")
                for col in mods['columns'].get('modified', []):
                    print(f"      ~ column: {col}")
            if 'indexes' in mods:
                for idx in mods['indexes'].get('added', []):
                    print(f"      + index: {idx}")
                for idx in mods['indexes'].get('removed', []):
                    print(f"      - index: {idx}")
    
    if not diff.tables_added and not diff.tables_removed and not diff.tables_modified:
        print("\n✅ Schemas are identical")


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Expand environment variables in URLs
    for env_name, env_config in config.get('environments', {}).items():
        url = env_config.get('url', '')
        if url.startswith('${') and url.endswith('}'):
            env_var = url[2:-1]
            env_config['url'] = os.environ.get(env_var, '')
    
    return config


def cmd_compare(args):
    """Handle compare command."""
    try:
        print(f"Connecting to source: {args.source.split('@')[0]}...")
        source_schema = get_schema(args.source)
        print(f"  Found {len(source_schema)} tables")
        
        print(f"Connecting to target: {args.target.split('@')[0]}...")
        target_schema = get_schema(args.target)
        print(f"  Found {len(target_schema)} tables")
        
        ignore = args.ignore.split(',') if args.ignore else []
        diff = compare_schemas(source_schema, target_schema, ignore)
        
        print_diff(diff)
        
        if diff.tables_added or diff.tables_removed or diff.tables_modified:
            return 1
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        return 2


def cmd_migrate(args):
    """Handle migrate command."""
    try:
        source_schema = get_schema(args.source)
        target_schema = get_schema(args.target)
        
        ignore = args.ignore.split(',') if args.ignore else []
        diff = compare_schemas(source_schema, target_schema, ignore)
        
        sql = generate_migration_sql(diff, source_schema, target_schema)
        
        if args.output:
            os.makedirs(args.output, exist_ok=True)
            output_file = os.path.join(args.output, 'migration.sql')
            with open(output_file, 'w') as f:
                f.write(sql)
            print(f"Migration script written to: {output_file}")
        else:
            print(sql)
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        return 2


def cmd_validate(args):
    """Handle validate command."""
    try:
        config = load_config(args.config)
        environments = config.get('environments', {})
        ignore = config.get('ignore_tables', [])
        
        if len(environments) < 2:
            print("Error: Need at least 2 environments to validate")
            return 3
        
        env_names = list(environments.keys())
        schemas = {}
        
        for env_name in env_names:
            url = environments[env_name].get('url')
            if not url:
                print(f"Warning: No URL for environment '{env_name}'")
                continue
            print(f"Connecting to {env_name}...")
            schemas[env_name] = get_schema(url)
        
        # Compare each pair
        all_match = True
        for i, env1 in enumerate(env_names):
            for env2 in env_names[i+1:]:
                if env1 not in schemas or env2 not in schemas:
                    continue
                print(f"\nComparing {env1} vs {env2}:")
                diff = compare_schemas(schemas[env1], schemas[env2], ignore)
                print_diff(diff)
                if diff.tables_added or diff.tables_removed or diff.tables_modified:
                    all_match = False
        
        return 0 if all_match else 1
        
    except FileNotFoundError:
        print(f"Error: Config file not found: {args.config}")
        return 3
    except Exception as e:
        print(f"Error: {e}")
        return 2


def main():
    parser = argparse.ArgumentParser(
        description='Database schema comparison and synchronization tool'
    )
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Compare command
    compare_parser = subparsers.add_parser('compare', help='Compare two database schemas')
    compare_parser.add_argument('--source', '-s', required=True, help='Source database URL')
    compare_parser.add_argument('--target', '-t', required=True, help='Target database URL')
    compare_parser.add_argument('--ignore', '-i', help='Comma-separated list of tables to ignore')
    
    # Migrate command
    migrate_parser = subparsers.add_parser('migrate', help='Generate migration script')
    migrate_parser.add_argument('--source', '-s', required=True, help='Source database URL')
    migrate_parser.add_argument('--target', '-t', required=True, help='Target database URL')
    migrate_parser.add_argument('--output', '-o', help='Output directory for migration script')
    migrate_parser.add_argument('--ignore', '-i', help='Comma-separated list of tables to ignore')
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate schema consistency')
    validate_parser.add_argument('--config', '-c', required=True, help='Path to config file')
    
    args = parser.parse_args()
    
    if args.command == 'compare':
        sys.exit(cmd_compare(args))
    elif args.command == 'migrate':
        sys.exit(cmd_migrate(args))
    elif args.command == 'validate':
        sys.exit(cmd_validate(args))
    else:
        parser.print_help()
        sys.exit(0)


if __name__ == '__main__':
    main()
