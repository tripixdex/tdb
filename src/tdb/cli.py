from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import pandas as pd
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


def _json_safe(v):
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return str(v)


def _csv_opts_sql(sample: int) -> str:
    return f"sample_size={int(sample)}"


@dataclass
class Col:
    name: str
    typ: str


def _table_cols(con: duckdb.DuckDBPyConnection, table: str) -> list[Col]:
    rows = con.execute(f"PRAGMA table_info({_qident(table)})").fetchall()
    return [Col(name=r[1], typ=r[2]) for r in rows]


def _toposort_tables(profile: dict) -> list[str]:
    tables = list(profile["tables"].keys())
    deps: dict[str, set[str]] = {t: set() for t in tables}
    for t, spec in profile["tables"].items():
        for fk in spec.get("fks", []):
            deps[t].add(fk["ref_table"])

    ordered: list[str] = []
    remaining = set(tables)
    while remaining:
        ready = sorted([t for t in remaining if deps[t].issubset(set(ordered))])
        if not ready:
            ordered.extend(sorted(list(remaining)))
            break
        ordered.extend(ready)
        remaining -= set(ready)
    return ordered


def _pk_metrics(con: duckdb.DuckDBPyConnection, table: str, cols: list[str]) -> dict:
    t = _qident(table)
    cols_q = [_qident(c) for c in cols]

    distinct_expr = (
        f"COUNT(DISTINCT ({', '.join(cols_q)}))"
        if len(cols_q) > 1
        else f"COUNT(DISTINCT {cols_q[0]})"
    )

    null_cond = " OR ".join([f"{c} IS NULL" for c in cols_q])

    q = f"""
    SELECT
      COUNT(*) AS n,
      {distinct_expr} AS distinct_n,
      SUM({null_cond}) AS null_n,
      COUNT(*) - {distinct_expr} AS dup_n
    FROM {t};
    """
    n, distinct_n, null_n, dup_n = con.execute(q).fetchone()
    return {
        "table": table,
        "pk": cols,
        "n": int(n),
        "distinct": int(distinct_n),
        "null": int(null_n),
        "dup": int(dup_n),
    }


def _fk_orphans(con: duckdb.DuckDBPyConnection, src_table: str, src_cols: list[str], ref_table: str, ref_cols: list[str]) -> dict:
    s = _qident(src_table)
    r = _qident(ref_table)
    s_cols = [_qident(c) for c in src_cols]
    r_cols = [_qident(c) for c in ref_cols]
    on = " AND ".join([f"r.{rc} = s.{sc}" for sc, rc in zip(s_cols, r_cols)])
    q = f"SELECT COUNT(*) FROM {s} s LEFT JOIN {r} r ON {on} WHERE r.{r_cols[0]} IS NULL;"
    n = con.execute(q).fetchone()[0]
    return {
        "fk": f'{src_table}({",".join(src_cols)}) -> {ref_table}({",".join(ref_cols)})',
        "orphans": int(n),
    }


@app.command()
def init(db: Path = typer.Option(DEFAULT_DB, "--db")):
    con = _connect(db)
    con.close()
    console.print(f"✅ DB ready: [bold]{db}[/bold]")


@app.command("import-csv")
def import_csv(
    csv: Path = typer.Argument(..., exists=True, dir_okay=False),
    table: str = typer.Option("data", "--table"),
    db: Path = typer.Option(DEFAULT_DB, "--db"),
    sample: int = typer.Option(20480, "--sample"),
    as_json: bool = typer.Option(False, "--json"),
):
    con = _connect(db)
    opts = _csv_opts_sql(sample)

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
def tables(db: Path = typer.Option(DEFAULT_DB, "--db"), as_json: bool = typer.Option(False, "--json")):
    con = _connect(db)
    names = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    con.close()

    if as_json:
        print(json.dumps(names, ensure_ascii=False))
        return

    tbl = Table(title=f"Tables in {db}")
    tbl.add_column("table")
    for n in names:
        tbl.add_row(n)
    console.print(tbl)


@app.command()
def describe(
    table: str = typer.Option("data", "--table"),
    db: Path = typer.Option(DEFAULT_DB, "--db"),
    as_json: bool = typer.Option(False, "--json"),
):
    con = _connect(db)
    df = con.execute(f'DESCRIBE "{table}"').df()
    con.close()

    if as_json:
        records = [{k: _json_safe(v) for k, v in row.items()} for row in df.to_dict(orient="records")]
        print(json.dumps(records, ensure_ascii=False))
        return

    tbl = Table(title=f"Schema: {table}")
    for c in df.columns:
        tbl.add_column(str(c))
    for _, row in df.iterrows():
        tbl.add_row(*[str(x) for x in row.tolist()])
    console.print(tbl)


@app.command()
def head(
    table: str = typer.Option("data", "--table"),
    n: int = typer.Option(50, "-n"),
    db: Path = typer.Option(DEFAULT_DB, "--db"),
    as_json: bool = typer.Option(False, "--json"),
):
    con = _connect(db)
    df = con.execute(f'SELECT * FROM "{table}" LIMIT {int(n)}').df()
    con.close()

    if as_json:
        records = [{k: _json_safe(v) for k, v in row.items()} for row in df.to_dict(orient="records")]
        print(json.dumps(records, ensure_ascii=False))
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
        rows_safe = [[_json_safe(v) for v in r] for r in rows]
        print(json.dumps({"ms": dt * 1000.0, "columns": cols, "rows": rows_safe}, ensure_ascii=False))
        return

    tbl = Table(title=f"SQL ({dt*1000:.1f} ms)")
    for c in cols:
        tbl.add_column(str(c))
    for r in rows[:200]:
        tbl.add_row(*[("" if v is None else str(v)) for v in r])
    console.print(tbl)


@app.command()
def validate(
    db: Path = typer.Option(DEFAULT_DB, "--db"),
    profile: Path = typer.Option(DEFAULT_PROFILE, "--profile"),
    as_json: bool = typer.Option(False, "--json"),
):
    prof = json.load(open(profile, "r", encoding="utf-8"))
    con = _connect(db)

    pk_rows: list[dict] = []
    fk_rows: list[dict] = []

    for tname, spec in prof["tables"].items():
        pk = spec.get("pk", [])
        if pk:
            pk_rows.append(_pk_metrics(con, tname, pk))
        for fk in spec.get("fks", []):
            fk_rows.append(_fk_orphans(con, tname, fk["cols"], fk["ref_table"], fk["ref_cols"]))

    con.close()
    db_size = db.stat().st_size if db.exists() else 0

    if as_json:
        print(json.dumps({"db": str(db), "db_bytes": int(db_size), "pk": pk_rows, "fk": fk_rows}, ensure_ascii=False))
        return

    pk_tbl = Table(title="PK checks")
    pk_tbl.add_column("table")
    pk_tbl.add_column("pk")
    pk_tbl.add_column("n", justify="right")
    pk_tbl.add_column("distinct", justify="right")
    pk_tbl.add_column("null", justify="right")
    pk_tbl.add_column("dup", justify="right")
    for r in pk_rows:
        pk_tbl.add_row(r["table"], ",".join(r["pk"]), str(r["n"]), str(r["distinct"]), str(r["null"]), str(r["dup"]))

    fk_tbl = Table(title="FK orphan checks")
    fk_tbl.add_column("fk")
    fk_tbl.add_column("orphans", justify="right")
    for r in fk_rows:
        fk_tbl.add_row(r["fk"].replace("->", "→"), str(r["orphans"]))

    console.print(pk_tbl)
    console.print(fk_tbl)
    console.print(f"💾 DB file: {_fmt_bytes(db_size)}  ({db})")


@app.command()
def build(
    folder: Path = typer.Argument(..., exists=True, file_okay=False),
    db: Path = typer.Option(Path("build/school.duckdb"), "--db"),
    profile: Path = typer.Option(DEFAULT_PROFILE, "--profile"),
    sample: int = typer.Option(20480, "--sample"),
    as_json: bool = typer.Option(False, "--json"),
):
    prof = json.load(open(profile, "r", encoding="utf-8"))
    order = _toposort_tables(prof)

    db.parent.mkdir(parents=True, exist_ok=True)
    db.unlink(missing_ok=True)

    con = _connect(db)
    metrics_rows: list[dict] = []

    # staging import
    for t in order:
        csv_path = folder / f"{t}.csv"
        stg = f"stg_{t}"

        t0 = time.perf_counter()
        con.execute(f'DROP TABLE IF EXISTS "{stg}"')
        con.execute(
            f"""
            CREATE TABLE "{stg}" AS
            SELECT * FROM read_csv('{csv_path.as_posix()}', sample_size={int(sample)})
            """
        )
        dt = time.perf_counter() - t0
        rows = con.execute(f'SELECT COUNT(*) FROM "{stg}"').fetchone()[0]
        metrics_rows.append({"table": stg, "rows": int(rows), "seconds": float(dt)})

    # constrained tables + load
    for t in order:
        stg = f"stg_{t}"
        cols = _table_cols(con, stg)

        pk = prof["tables"][t].get("pk", [])
        fks = prof["tables"][t].get("fks", [])

        col_ddl = ",\n  ".join([f"{_qident(c.name)} {c.typ}" for c in cols])

        pk_ddl = ""
        if pk:
            pk_ddl = ",\n  PRIMARY KEY (" + ", ".join(_qident(c) for c in pk) + ")"

        fk_ddl = ""
        if fks:
            fk_lines = []
            for fk in fks:
                fk_lines.append(
                    "FOREIGN KEY ("
                    + ", ".join(_qident(c) for c in fk["cols"])
                    + ") REFERENCES "
                    + _qident(fk["ref_table"])
                    + " ("
                    + ", ".join(_qident(c) for c in fk["ref_cols"])
                    + ")"
                )
            fk_ddl = ",\n  " + ",\n  ".join(fk_lines)

        t0 = time.perf_counter()
        con.execute(f'DROP TABLE IF EXISTS "{t}"')
        con.execute(f'CREATE TABLE "{t}" (\n  {col_ddl}{pk_ddl}{fk_ddl}\n)')
        con.execute(f'INSERT INTO "{t}" SELECT * FROM "{stg}"')
        con.execute(f'DROP TABLE "{stg}"')
        dt = time.perf_counter() - t0
        rows = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        metrics_rows.append({"table": t, "rows": int(rows), "seconds": float(dt)})

    con.close()
    db_size = db.stat().st_size if db.exists() else 0

    if as_json:
        print(json.dumps({"db": str(db), "db_bytes": int(db_size), "metrics": metrics_rows}, ensure_ascii=False))
        return

    tbl = Table(title="Build metrics")
    tbl.add_column("table")
    tbl.add_column("rows", justify="right")
    tbl.add_column("seconds", justify="right")
    for r in metrics_rows:
        tbl.add_row(r["table"], str(r["rows"]), f'{r["seconds"]:.3f}')
    console.print(tbl)
    console.print(f"💾 DB file: {_fmt_bytes(db_size)}  ({db})")
