from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Optional

import duckdb
import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(add_completion=False, no_args_is_help=True)
console = Console()

DEFAULT_DB = Path(".tdb.duckdb")

def _connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))

def _fmt_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    x = float(n)
    for u in units:
        if x < 1024 or u == units[-1]:
            return f"{x:.2f} {u}"
        x /= 1024
    return f"{n} B"

def _safe_table_name(name: str) -> str:
    name = Path(name).stem
    name = re.sub(r"[^0-9A-Za-z_]+", "_", name.strip())
    name = re.sub(r"_+", "_", name).strip("_")
    return name.lower() or "data"

def _csv_opts_sql(delim: str, header: str, sample: int) -> str:
    opts = [f"sample_size={int(sample)}"]
    # auto = do not override (let DuckDB sniff dialect/header)
    if delim != "auto":
        opts.append(f"delim='{delim}'")
    if header != "auto":
        opts.append(f"header={header.lower()}")
    return ", ".join(opts)

@app.command()
def init(db: Path = typer.Option(DEFAULT_DB, "--db", help="Path to DuckDB database file")):
    """Create/open database."""
    con = _connect(db)
    con.close()
    console.print(f"✅ DB ready: [bold]{db}[/bold]")

@app.command("import-csv")
def import_csv(
    csv: Path = typer.Argument(..., exists=True, dir_okay=False, help="Path to CSV file"),
    table: str = typer.Option("data", "--table", help="Table name"),
    db: Path = typer.Option(DEFAULT_DB, "--db", help="Path to DuckDB database file"),
    delim: str = typer.Option("auto", "--delim", help="Delimiter, e.g. ';' or 'auto'"),
    header: str = typer.Option("auto", "--header", help="auto|true|false"),
    sample: int = typer.Option(20480, "--sample", help="Rows to sample for detection"),
    as_json: bool = typer.Option(False, "--json", help="Machine-readable JSON output (for GUI)"),
):
    """Import one CSV into DuckDB."""
    con = _connect(db)
    opts = _csv_opts_sql(delim, header, sample)

    t0 = time.perf_counter()
    con.execute(f'DROP TABLE IF EXISTS "{table}"')
    con.execute(
        f"""
        CREATE TABLE "{table}" AS
        SELECT * FROM read_csv('{csv.as_posix()}', {opts})
        """
    )
    dt = time.perf_counter() - t0

    rows = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    db_size = db.stat().st_size if db.exists() else 0
    con.close()

    payload = {
        "db": str(db),
        "table": table,
        "csv": str(csv),
        "rows": int(rows),
        "seconds": float(dt),
        "rows_per_sec": float(rows / dt) if dt > 0 else None,
        "db_bytes": int(db_size),
    }

    if as_json:
        print(json.dumps(payload, ensure_ascii=False))
        return

    console.print(f"✅ Imported [bold]{rows}[/bold] rows into [bold]{table}[/bold]")
    console.print(f"⏱  {dt:.3f}s  |  {rows/dt:,.0f} rows/sec")
    console.print(f"💾 DB file: {_fmt_bytes(db_size)}  ({db})")

@app.command("import-folder")
def import_folder(
    folder: Path = typer.Argument(..., exists=True, file_okay=False, help="Folder with CSV files"),
    pattern: str = typer.Option("*.csv", "--pattern", help="Glob pattern"),
    db: Path = typer.Option(DEFAULT_DB, "--db", help="Path to DuckDB database file"),
    delim: str = typer.Option("auto", "--delim", help="Delimiter or auto"),
    header: str = typer.Option("auto", "--header", help="auto|true|false"),
    sample: int = typer.Option(20480, "--sample", help="Rows to sample for detection"),
):
    """Import all CSV files from a folder into separate tables."""
    files = sorted(folder.glob(pattern))
    if not files:
        raise typer.BadParameter(f"No files match {pattern} in {folder}")

    total_rows = 0
    total_sec = 0.0
    for f in files:
        table = _safe_table_name(f.name)
        con = _connect(db)
        opts = _csv_opts_sql(delim, header, sample)

        t0 = time.perf_counter()
        con.execute(f'DROP TABLE IF EXISTS "{table}"')
        con.execute(
            f"""
            CREATE TABLE "{table}" AS
            SELECT * FROM read_csv('{f.as_posix()}', {opts})
            """
        )
        dt = time.perf_counter() - t0
        rows = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        con.close()

        total_rows += int(rows)
        total_sec += float(dt)
        console.print(f"✅ {f.name}  →  [bold]{table}[/bold]  ({rows} rows, {dt:.3f}s)")

    db_size = db.stat().st_size if db.exists() else 0
    console.print(f"\n📦 Total: [bold]{total_rows}[/bold] rows | ⏱ {total_sec:.3f}s | 💾 {_fmt_bytes(db_size)}")

@app.command()
def tables(
    db: Path = typer.Option(DEFAULT_DB, "--db", help="Path to DuckDB database file"),
):
    """List tables."""
    con = _connect(db)
    names = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    tbl = Table(title=f"Tables in {db}")
    tbl.add_column("table")
    tbl.add_column("rows", justify="right")
    for name in names:
        n = con.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
        tbl.add_row(name, str(n))
    console.print(tbl)
    con.close()

@app.command()
def describe(
    table: str = typer.Option("data", "--table", help="Table name"),
    db: Path = typer.Option(DEFAULT_DB, "--db", help="Path to DuckDB database file"),
):
    """Describe table schema."""
    con = _connect(db)
    df = con.execute(f'DESCRIBE "{table}"').df()
    tbl = Table(title=f"Schema: {table}")
    for c in df.columns:
        tbl.add_column(str(c))
    for _, row in df.iterrows():
        tbl.add_row(*[str(x) for x in row.tolist()])
    console.print(tbl)
    con.close()

@app.command()
def head(
    table: str = typer.Option("data", "--table", help="Table name"),
    n: int = typer.Option(10, "-n", help="Rows to show"),
    db: Path = typer.Option(DEFAULT_DB, "--db", help="Path to DuckDB database file"),
    as_json: bool = typer.Option(False, "--json", help="JSON output (for GUI)"),
):
    """Show first N rows."""
    con = _connect(db)
    df = con.execute(f'SELECT * FROM "{table}" LIMIT {int(n)}').df()
    con.close()

    if as_json:
        print(json.dumps(df.to_dict(orient="records"), ensure_ascii=False))
        return

    rich_table = Table(title=f"{table} (first {n})", show_lines=False)
    for col in df.columns:
        rich_table.add_column(str(col), overflow="fold")
    for _, row in df.iterrows():
        rich_table.add_row(*[("" if v is None else str(v)) for v in row.tolist()])
    console.print(rich_table)

@app.command()
def sql(
    query: str = typer.Argument(..., help='SQL query, e.g. "SELECT COUNT(*) FROM data"'),
    db: Path = typer.Option(DEFAULT_DB, "--db", help="Path to DuckDB database file"),
    as_json: bool = typer.Option(False, "--json", help="JSON output (for GUI)"),
):
    """Run an arbitrary SQL query."""
    con = _connect(db)
    t0 = time.perf_counter()
    res = con.execute(query)
    dt = time.perf_counter() - t0
    cols = [d[0] for d in res.description] if res.description else []
    rows = res.fetchall()
    con.close()

    if as_json:
        out = {"ms": dt * 1000.0, "columns": cols, "rows": rows}
        print(json.dumps(out, ensure_ascii=False))
        return

    tbl = Table(title=f"SQL ({dt*1000:.1f} ms)")
    for c in cols:
        tbl.add_column(str(c))
    for r in rows[:200]:
        tbl.add_row(*[("" if v is None else str(v)) for v in r])
    console.print(tbl)
    if len(rows) > 200:
        console.print(f"… показано 200 из {len(rows)} строк")

@app.command()
def sniff(
    csv: Path = typer.Argument(..., exists=True, dir_okay=False, help="Path to CSV file"),
    db: Path = typer.Option(DEFAULT_DB, "--db", help="Path to DuckDB database file"),
):
    """Show what DuckDB detects about the CSV (dialect/types/header)."""
    con = _connect(db)
    df = con.execute(f"SELECT * FROM sniff_csv('{csv.as_posix()}')").df()
    con.close()

    tbl = Table(title=f"sniff_csv: {csv.name}")
    for c in df.columns:
        tbl.add_column(str(c), overflow="fold")
    for _, row in df.iterrows():
        tbl.add_row(*[("" if v is None else str(v)) for v in row.tolist()])
    console.print(tbl)
