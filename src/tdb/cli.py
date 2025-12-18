from __future__ import annotations

import json
import re
import time
from pathlib import Path

import duckdb
import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(add_completion=False, no_args_is_help=True)
console = Console()

DEFAULT_DB = Path(".tdb.duckdb")
DEFAULT_PROFILE = Path(".tdb_profile.json")


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


def _qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _csv_opts_sql(delim: str, header: str, sample: int) -> str:
    opts = [f"sample_size={int(sample)}"]
    if delim != "auto":
        opts.append(f"delim='{delim}'")
    if header != "auto":
        opts.append(f"header={header.lower()}")
    return ", ".join(opts)


def _pk_metrics_sql(table: str, cols: list[str]) -> str:
    t = _qident(table)
    cols_q = [_qident(c) for c in cols]
    n_expr = "COUNT(*) AS n"
    distinct_expr = (
        f"COUNT(DISTINCT ({', '.join(cols_q)})) AS distinct_n"
        if len(cols_q) > 1
        else f"COUNT(DISTINCT {cols_q[0]}) AS distinct_n"
    )
    null_cond = " OR ".join([f"{c} IS NULL" for c in cols_q])
    null_expr = f"SUM({null_cond}) AS null_n"
    dup_expr = "COUNT(*) - " + (
        f"COUNT(DISTINCT ({', '.join(cols_q)})) AS dup_n" if len(cols_q) > 1 else f"COUNT(DISTINCT {cols_q[0]}) AS dup_n"
    )
    return f"SELECT {n_expr}, {distinct_expr}, {null_expr}, {dup_expr} FROM {t};"


def _fk_orphans_sql(src_table: str, src_cols: list[str], ref_table: str, ref_cols: list[str], label: str) -> str:
    s = _qident(src_table)
    r = _qident(ref_table)
    s_cols = [_qident(c) for c in src_cols]
    r_cols = [_qident(c) for c in ref_cols]
    on = " AND ".join([f"r.{rc} = s.{sc}" for sc, rc in zip(s_cols, r_cols)])
    where = f"r.{r_cols[0]} IS NULL"
    return f"SELECT COUNT(*) AS {_qident(label)} FROM {s} s LEFT JOIN {r} r ON {on} WHERE {where};"


@app.command()
def init(db: Path = typer.Option(DEFAULT_DB, "--db", help="Path to DuckDB database file")):
    con = _connect(db)
    con.close()
    console.print(f"✅ DB ready: [bold]{db}[/bold]")


@app.command("import-csv")
def import_csv(
    csv: Path = typer.Argument(..., exists=True, dir_okay=False),
    table: str = typer.Option("data", "--table"),
    db: Path = typer.Option(DEFAULT_DB, "--db"),
    delim: str = typer.Option("auto", "--delim"),
    header: str = typer.Option("auto", "--header"),
    sample: int = typer.Option(20480, "--sample"),
    as_json: bool = typer.Option(False, "--json"),
):
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
    con.close()

    db_size = db.stat().st_size if db.exists() else 0
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


@app.command()
def describe(table: str = typer.Option("data", "--table"), db: Path = typer.Option(DEFAULT_DB, "--db")):
    con = _connect(db)
    df = con.execute(f'DESCRIBE "{table}"').df()
    con.close()

    tbl = Table(title=f"Schema: {table}")
    for c in df.columns:
        tbl.add_column(str(c))
    for _, row in df.iterrows():
        tbl.add_row(*[str(x) for x in row.tolist()])
    console.print(tbl)


@app.command()
def head(
    table: str = typer.Option("data", "--table"),
    n: int = typer.Option(10, "-n"),
    db: Path = typer.Option(DEFAULT_DB, "--db"),
    as_json: bool = typer.Option(False, "--json"),
):
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
def sql(query: str = typer.Argument(...), db: Path = typer.Option(DEFAULT_DB, "--db"), as_json: bool = typer.Option(False, "--json")):
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
def validate(
    db: Path = typer.Option(DEFAULT_DB, "--db"),
    profile: Path = typer.Option(DEFAULT_PROFILE, "--profile"),
):
    prof = json.load(open(profile, "r", encoding="utf-8"))
    con = _connect(db)

    pk_tbl = Table(title="PK checks")
    pk_tbl.add_column("table")
    pk_tbl.add_column("pk")
    pk_tbl.add_column("n", justify="right")
    pk_tbl.add_column("distinct", justify="right")
    pk_tbl.add_column("null", justify="right")
    pk_tbl.add_column("dup", justify="right")

    fk_tbl = Table(title="FK orphan checks")
    fk_tbl.add_column("fk")
    fk_tbl.add_column("orphans", justify="right")

    for tname, spec in prof["tables"].items():
        pk = spec.get("pk", [])
        if pk:
            q = _pk_metrics_sql(tname, pk)
            n, distinct_n, null_n, dup_n = con.execute(q).fetchone()
            pk_tbl.add_row(tname, ",".join(pk), str(n), str(distinct_n), str(null_n), str(dup_n))

        for i, fk in enumerate(spec.get("fks", []), start=1):
            label = f"{tname}.fk{i}"
            q = _fk_orphans_sql(tname, fk["cols"], fk["ref_table"], fk["ref_cols"], label)
            orphans = con.execute(q).fetchone()[0]
            fk_tbl.add_row(f'{tname}({",".join(fk["cols"])}) → {fk["ref_table"]}({",".join(fk["ref_cols"])})', str(orphans))

    con.close()
    db_size = db.stat().st_size if db.exists() else 0

    console.print(pk_tbl)
    console.print(fk_tbl)
    console.print(f"💾 DB file: {_fmt_bytes(db_size)}  ({db})")
