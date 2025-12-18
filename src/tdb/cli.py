from __future__ import annotations

import json
import time
from dataclasses import dataclass
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
    distinct_expr = (
        f"COUNT(DISTINCT ({', '.join(cols_q)})) AS distinct_n"
        if len(cols_q) > 1
        else f"COUNT(DISTINCT {cols_q[0]}) AS distinct_n"
    )
    null_cond = " OR ".join([f"{c} IS NULL" for c in cols_q])
    return f"""
    SELECT
      COUNT(*) AS n,
      {distinct_expr},
      SUM({null_cond}) AS null_n,
      COUNT(*) - {distinct_expr.replace(" AS distinct_n", "")} AS dup_n
    FROM {t};
    """


def _fk_orphans_sql(src_table: str, src_cols: list[str], ref_table: str, ref_cols: list[str], label: str) -> str:
    s = _qident(src_table)
    r = _qident(ref_table)
    s_cols = [_qident(c) for c in src_cols]
    r_cols = [_qident(c) for c in ref_cols]
    on = " AND ".join([f"r.{rc} = s.{sc}" for sc, rc in zip(s_cols, r_cols)])
    where = f"r.{r_cols[0]} IS NULL"
    return f"SELECT COUNT(*) AS {_qident(label)} FROM {s} s LEFT JOIN {r} r ON {on} WHERE {where};"


@dataclass
class Col:
    name: str
    typ: str


def _table_cols(con: duckdb.DuckDBPyConnection, table: str) -> list[Col]:
    # PRAGMA table_info('t') returns: cid, name, type, notnull, dflt_value, pk
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
            # cycle or missing table in profile
            ordered.extend(sorted(list(remaining)))
            break
        ordered.extend(ready)
        remaining -= set(ready)
    return ordered


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
    payload = {"db": str(db), "table": table, "csv": str(csv), "rows": int(rows), "seconds": float(dt), "db_bytes": int(db_size)}

    if as_json:
        print(json.dumps(payload, ensure_ascii=False))
        return

    console.print(f"✅ Imported [bold]{rows}[/bold] rows into [bold]{table}[/bold]")
    console.print(f"⏱  {dt:.3f}s  |  {rows/dt:,.0f} rows/sec")
    console.print(f"💾 DB file: {_fmt_bytes(db_size)}  ({db})")


@app.command("import-folder")
def import_folder(
    folder: Path = typer.Argument(..., exists=True, file_okay=False),
    db: Path = typer.Option(DEFAULT_DB, "--db"),
):
    files = sorted(folder.glob("*.csv"))
    for f in files:
        t = f.stem
        import_csv.callback(csv=f, table=t, db=db, delim="auto", header="auto", sample=20480, as_json=False)  # type: ignore


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
def sql(query: str = typer.Argument(...), db: Path = typer.Option(DEFAULT_DB, "--db"), as_json: bool = typer.Option(False, "--json")):
    con = _connect(db)
    t0 = time.perf_counter()
    res = con.execute(query)
    dt = time.perf_counter() - t0
    cols = [d[0] for d in res.description] if res.description else []
    rows = res.fetchall()
    con.close()

    if as_json:
        print(json.dumps({"ms": dt * 1000.0, "columns": cols, "rows": rows}, ensure_ascii=False))
        return

    tbl = Table(title=f"SQL ({dt*1000:.1f} ms)")
    for c in cols:
        tbl.add_column(str(c))
    for r in rows[:200]:
        tbl.add_row(*[("" if v is None else str(v)) for v in r])
    console.print(tbl)


@app.command()
def validate(db: Path = typer.Option(DEFAULT_DB, "--db"), profile: Path = typer.Option(DEFAULT_PROFILE, "--profile")):
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
            n, distinct_n, null_n, dup_n = con.execute(_pk_metrics_sql(tname, pk)).fetchone()
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


@app.command()
def build(
    folder: Path = typer.Argument(..., exists=True, file_okay=False),
    db: Path = typer.Option(Path("build/school.duckdb"), "--db"),
    profile: Path = typer.Option(DEFAULT_PROFILE, "--profile"),
):
    prof = json.load(open(profile, "r", encoding="utf-8"))
    order = _toposort_tables(prof)

    db.parent.mkdir(parents=True, exist_ok=True)
    db.unlink(missing_ok=True)

    con = _connect(db)

    metrics = Table(title="Build metrics")
    metrics.add_column("table")
    metrics.add_column("rows", justify="right")
    metrics.add_column("seconds", justify="right")

    # 1) staging import (dialect autodetect)
    for t in order:
        csv_path = folder / f"{t}.csv"
        stg = f"stg_{t}"

        t0 = time.perf_counter()
        con.execute(f'DROP TABLE IF EXISTS "{stg}"')
        con.execute(
            f"""
            CREATE TABLE "{stg}" AS
            SELECT * FROM read_csv('{csv_path.as_posix()}', sample_size=20480)
            """
        )
        dt = time.perf_counter() - t0
        rows = con.execute(f'SELECT COUNT(*) FROM "{stg}"').fetchone()[0]
        metrics.add_row(f"stg_{t}", str(rows), f"{dt:.3f}")

    # 2) constrained tables + load
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
        metrics.add_row(t, str(rows), f"{dt:.3f}")

    con.close()
    db_size = db.stat().st_size if db.exists() else 0
    console.print(metrics)
    console.print(f"💾 DB file: {_fmt_bytes(db_size)}  ({db})")
