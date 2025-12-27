"""
Interactive DuckDB Query Tool
Run SQL queries on the Polymarket database
"""

import duckdb
import sys
from pathlib import Path
from create_database import DEFAULT_DB_PATH


def run_query(db_path: str, query: str, show_schema: bool = False):
    """
    Execute a SQL query and display results.
    
    Args:
        db_path: Path to DuckDB database
        query: SQL query to execute
        show_schema: Also show table schemas
    """
    conn = duckdb.connect(db_path)
    
    if show_schema:
        print("="*60)
        print("DATABASE SCHEMA")
        print("="*60)
        tables = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main'
            ORDER BY table_name
        """).fetchall()
        
        for (table_name,) in tables:
            print(f"\n{table_name}:")
            columns = conn.execute(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """).fetchall()
            for col_name, data_type, nullable in columns:
                null_str = "NULL" if nullable == "YES" else "NOT NULL"
                print(f"  {col_name:30s} {data_type:20s} {null_str}")
        print("\n" + "="*60 + "\n")
    
    try:
        result = conn.execute(query)
        
        # Get column names
        if result.description:
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            
            if not rows:
                print("No results returned.")
                return
            
            # Calculate column widths
            widths = [len(col) for col in columns]
            for row in rows:
                for i, val in enumerate(row):
                    widths[i] = max(widths[i], len(str(val)))
            
            # Print header
            header = " | ".join(f"{col:{w}}" for col, w in zip(columns, widths))
            print(header)
            print("-" * len(header))
            
            # Print rows
            for row in rows:
                print(" | ".join(f"{str(val):{w}}" for val, w in zip(row, widths)))
            
            print(f"\n({len(rows)} row{'s' if len(rows) != 1 else ''})")
        else:
            print("Query executed successfully (no results to display).")
    
    except Exception as e:
        print(f"Error executing query: {e}")
    finally:
        conn.close()


def interactive_mode(db_path: str):
    """
    Run in interactive mode - accept queries from stdin.
    """
    print("="*60)
    print("POLYMARKET DATABASE QUERY TOOL")
    print(f"Database: {db_path}")
    print("="*60)
    print("\nCommands:")
    print("  \\q or \\quit     - Exit")
    print("  \\schema         - Show database schema")
    print("  \\tables         - List all tables")
    print("  \\d <table>      - Describe table")
    print("  \\stats          - Show table row counts")
    print("  Any SQL query   - Execute query")
    print("\nEnter SQL query (end with semicolon):")
    print("-"*60)
    
    conn = duckdb.connect(db_path)
    
    query_buffer = []
    
    while True:
        try:
            if not query_buffer:
                prompt = "sql> "
            else:
                prompt = "...> "
            
            line = input(prompt).strip()
            
            # Handle commands
            if line.lower() in ['\\q', '\\quit', 'exit', 'quit']:
                print("Goodbye!")
                break
            
            elif line.lower() == '\\schema':
                run_query(db_path, "SELECT 1", show_schema=True)
                continue
            
            elif line.lower() == '\\tables':
                result = conn.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'main'
                    ORDER BY table_name
                """).fetchall()
                print("\nTables:")
                for (table,) in result:
                    print(f"  - {table}")
                print()
                continue
            
            elif line.lower().startswith('\\d '):
                table_name = line[3:].strip()
                try:
                    columns = conn.execute(f"""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_name = '{table_name}'
                        ORDER BY ordinal_position
                    """).fetchall()
                    
                    if columns:
                        print(f"\nTable: {table_name}")
                        print("-" * 60)
                        for col_name, data_type, nullable in columns:
                            null_str = "NULL" if nullable == "YES" else "NOT NULL"
                            print(f"{col_name:30s} {data_type:20s} {null_str}")
                        print()
                    else:
                        print(f"Table '{table_name}' not found.\n")
                except Exception as e:
                    print(f"Error: {e}\n")
                continue
            
            elif line.lower() == '\\stats':
                tables = conn.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'main'
                    ORDER BY table_name
                """).fetchall()
                
                print("\nTable Statistics:")
                print("-" * 60)
                for (table,) in tables:
                    try:
                        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                        print(f"{table:30s} {count:>15,} rows")
                    except duckdb.CatalogException:
                        print(f"{table:30s} {'Table not found':>15}")
                    except Exception as e:
                        print(f"{table:30s} {'Error: ' + str(e)[:20]:>15}")
                print()
                continue
            
            # Build multi-line queries
            if line:
                query_buffer.append(line)
            
            # Execute when semicolon found
            if line.endswith(';'):
                query = ' '.join(query_buffer)
                query_buffer = []
                
                try:
                    result = conn.execute(query)
                    
                    if result.description:
                        columns = [desc[0] for desc in result.description]
                        rows = result.fetchall()
                        
                        if not rows:
                            print("No results returned.\n")
                            continue
                        
                        # Calculate column widths (no truncation for raw output)
                        widths = [len(col) for col in columns]
                        for row in rows:
                            for i, val in enumerate(row):
                                widths[i] = max(widths[i], len(str(val)))
                        
                        # Print header
                        header = " | ".join(f"{col:{w}}" for col, w in zip(columns, widths))
                        print("\n" + header)
                        print("-" * len(header))
                        
                        # Print rows (limit display to 100 rows but show full content)
                        display_rows = rows[:100]
                        for row in display_rows:
                            formatted = []
                            for val, w in zip(row, widths):
                                s = str(val)
                                # For very wide columns (like JSON), print on separate line
                                if w > 120:
                                    formatted.append(f"\n{s}\n")
                                else:
                                    formatted.append(f"{s:{w}}")
                            print(" | ".join(formatted))
                        
                        if len(rows) > 100:
                            print(f"... ({len(rows) - 100} more rows)")
                        
                        print(f"\n({len(rows)} row{'s' if len(rows) != 1 else ''})\n")
                    else:
                        print("Query executed successfully.\n")
                
                except Exception as e:
                    print(f"Error: {e}\n")
        
        except KeyboardInterrupt:
            print("\nUse \\q to quit")
            query_buffer = []
        except EOFError:
            print("\nGoodbye!")
            break
    
    conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Query Polymarket DuckDB database")
    parser.add_argument("--db", default=None, help=f"Database path (default: {DEFAULT_DB_PATH})")
    parser.add_argument("--query", "-q", help="SQL query to execute (non-interactive)")
    parser.add_argument("--schema", action="store_true", help="Show database schema")
    
    args = parser.parse_args()
    
    db_path = args.db or DEFAULT_DB_PATH
    
    if not Path(db_path).exists():
        print(f"Error: Database not found at {db_path}")
        sys.exit(1)
    
    if args.query:
        # Single query mode
        run_query(db_path, args.query, show_schema=args.schema)
    else:
        # Interactive mode
        interactive_mode(db_path)
