#!/usr/bin/env python3
"""
Local SQLite-backed indexer for order conf files.

The index maps order_id -> all matching conf files and their batch folders. It
is intentionally standard-library only so it can run unchanged on Linux, WSL
Ubuntu, and simple cluster login nodes.
"""

import argparse
import glob
import os
from pathlib import Path
import re
import sqlite3
import time
from datetime import datetime


CONF_RE = re.compile(r"^(?P<order_id>B\d+)(?P<suffix>.*)\.conf$")


def log(level, message):
    print("[{0}] {1}".format(level, message), flush=True)


def info(message):
    log("INFO", message)


def warn(message):
    log("WARN", message)


def error(message):
    log("ERROR", message)


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def normalize_path(path):
    """Expand ~ and return an absolute Path without requiring it to exist."""
    return Path(path).expanduser().resolve()


def init_db(db_path):
    """
    Create SQLite database and tables if they do not exist, and enable WAL mode.
    """
    db = normalize_path(db_path)
    if db.parent:
        db.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(str(db)) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders_latest (
                order_id TEXT PRIMARY KEY,
                conf_path TEXT NOT NULL,
                batch TEXT,
                retry INTEGER,
                mtime REAL,
                indexed_at TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT,
                conf_path TEXT UNIQUE,
                batch TEXT,
                retry INTEGER,
                mtime REAL,
                indexed_at TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS order_conf_index (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL,
                conf_name TEXT NOT NULL,
                suffix TEXT,
                conf_path TEXT NOT NULL UNIQUE,
                batch TEXT NOT NULL,
                batch_path TEXT NOT NULL,
                mtime REAL,
                indexed_at TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_order_id
            ON order_conf_index(order_id);
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_batch
            ON order_conf_index(batch);
            """
        )
        conn.commit()


def parse_conf_path(conf_path):
    """
    Parse a conf file path into indexable metadata.

    Returns None if the filename does not match B...*.conf, the path is not a
    file directly inside a Conf directory, the batch directory does not start
    with 20, or the file cannot be stat'ed.
    """
    path = normalize_path(conf_path)
    match = CONF_RE.match(path.name)
    if not match:
        return None

    if path.parent.name != "Conf":
        return None

    batch_dir = path.parent.parent
    if not batch_dir.name.startswith("20"):
        return None

    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None

    return {
        "order_id": match.group("order_id"),
        "conf_name": path.name,
        "suffix": match.group("suffix"),
        "conf_path": str(path),
        "batch": batch_dir.name,
        "batch_path": str(batch_dir),
        "mtime": mtime,
    }


def index_single_conf(conf_path, db_path):
    """
    Parse and index a single conf file.

    Returns the parsed metadata dict when indexed, otherwise None.
    """
    init_db(db_path)
    record = parse_conf_path(conf_path)
    if record is None:
        error("Failed to parse file: {0}".format(conf_path))
        return None

    indexed_at = now_iso()
    db = normalize_path(db_path)

    with sqlite3.connect(str(db)) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            INSERT OR IGNORE INTO order_conf_index
                (
                    order_id,
                    conf_name,
                    suffix,
                    conf_path,
                    batch,
                    batch_path,
                    mtime,
                    indexed_at
                )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                record["order_id"],
                record["conf_name"],
                record["suffix"],
                record["conf_path"],
                record["batch"],
                record["batch_path"],
                record["mtime"],
                indexed_at,
            ),
        )
        conn.execute(
            """
            UPDATE order_conf_index
            SET order_id = ?,
                conf_name = ?,
                suffix = ?,
                batch = ?,
                batch_path = ?,
                mtime = ?,
                indexed_at = ?
            WHERE conf_path = ?;
            """,
            (
                record["order_id"],
                record["conf_name"],
                record["suffix"],
                record["batch"],
                record["batch_path"],
                record["mtime"],
                indexed_at,
                record["conf_path"],
            ),
        )
        conn.commit()

    info("Indexed conf: {0}".format(record["conf_path"]))
    record["indexed_at"] = indexed_at
    return record


def discover_batch_dirs(root):
    """
    Find batch directories under root.

    A batch directory is any directory whose name starts with 20 and contains a
    direct Conf child directory. Results are sorted descending by directory name.
    """
    root_path = normalize_path(root)
    if not root_path.exists():
        warn("Root does not exist: {0}".format(root_path))
        return []

    batch_dirs = []
    for current_root, dirnames, _filenames in os.walk(str(root_path)):
        current = Path(current_root)
        if current.name.startswith("20") and (current / "Conf").is_dir():
            batch_dirs.append(current)
            dirnames[:] = []

    return sorted(batch_dirs, key=lambda path: path.name, reverse=True)


def incremental_scan(root, db_path, limit=50):
    """
    Scan only the latest N discovered batch directories and index Conf/*.conf.
    """
    init_db(db_path)
    root_path = normalize_path(root)
    info("Starting incremental scan")

    indexed = 0
    for batch_dir in discover_batch_dirs(root_path)[: int(limit)]:
        conf_dir = batch_dir / "Conf"
        for conf_path in sorted(conf_dir.glob("*.conf")):
            if index_single_conf(conf_path, db_path) is not None:
                indexed += 1

    info("Incremental scan complete, indexed {0} conf files".format(indexed))
    return indexed


def iter_all_conf_files(root):
    """
    Yield conf files matching the required */20*/Conf/*.conf shape under root.
    """
    root_path = normalize_path(root)
    pattern = str(root_path / "**" / "20*" / "Conf" / "*.conf")
    for conf_name in glob.glob(pattern, recursive=True):
        conf_path = normalize_path(conf_name)
        try:
            conf_path.relative_to(root_path)
        except ValueError:
            continue
        yield conf_path


def full_scan(root, db_path):
    """
    Recursively scan all */20*/Conf/*.conf under root and index every file.
    """
    init_db(db_path)
    root_path = normalize_path(root)
    info("Starting full scan")

    if not root_path.exists():
        warn("Root does not exist: {0}".format(root_path))
        return 0

    indexed = 0
    for conf_path in sorted(iter_all_conf_files(root_path)):
        if index_single_conf(conf_path, db_path) is not None:
            indexed += 1

    info("Full scan complete, indexed {0} conf files".format(indexed))
    return indexed


def row_to_mapping(row):
    return {
        "order_id": row[0],
        "conf_name": row[1],
        "suffix": row[2],
        "conf_path": row[3],
        "batch": row[4],
        "batch_path": row[5],
        "mtime": row[6],
        "indexed_at": row[7],
    }


def query_order_records(order_id, db_path):
    init_db(db_path)
    db = normalize_path(db_path)
    with sqlite3.connect(str(db)) as conn:
        rows = conn.execute(
            """
            SELECT
                order_id,
                conf_name,
                suffix,
                conf_path,
                batch,
                batch_path,
                mtime,
                indexed_at
            FROM order_conf_index
            WHERE order_id = ?
            ORDER BY batch DESC, conf_name ASC;
            """,
            (order_id,),
        ).fetchall()

    return [row_to_mapping(row) for row in rows]


def fallback_search(root, order_id, db_path):
    """
    Search filesystem directly for */20*/Conf/{order_id}*.conf.

    Any found files are indexed, then all indexed mappings for the order are
    returned.
    """
    init_db(db_path)
    root_path = normalize_path(root)
    info("Fallback search started for order: {0}".format(order_id))

    if not root_path.exists():
        warn("Root does not exist: {0}".format(root_path))
        return []

    pattern = str(root_path / "**" / "20*" / "Conf" / (order_id + "*.conf"))
    found = 0
    for conf_name in sorted(glob.glob(pattern, recursive=True)):
        conf_path = normalize_path(conf_name)
        try:
            conf_path.relative_to(root_path)
        except ValueError:
            continue
        record = index_single_conf(conf_path, db_path)
        if record is not None and record["order_id"] == order_id:
            found += 1

    if found == 0:
        warn("Fallback search found no conf files for order: {0}".format(order_id))
        return []

    info("Fallback search indexed {0} conf file(s)".format(found))
    return query_order_records(order_id, db_path)


def get_order_mappings(order_id, root, db_path):
    """
    Query all conf mappings for an order with SQLite-first lookup and
    filesystem validation.
    """
    init_db(db_path)
    records = query_order_records(order_id, db_path)

    if not records:
        info("No index records found, fallback search triggered")
        return fallback_search(root, order_id, db_path)

    valid_records = []
    missing_count = 0
    refreshed = False

    for record in records:
        conf_path = normalize_path(record["conf_path"])
        if not conf_path.exists():
            missing_count += 1
            warn("Indexed path missing: {0}".format(record["conf_path"]))
            continue

        current_mtime = conf_path.stat().st_mtime
        stored_mtime = record.get("mtime") or 0.0
        if current_mtime != stored_mtime:
            info("Indexed file mtime changed, refreshing record: {0}".format(conf_path))
            index_single_conf(conf_path, db_path)
            refreshed = True

        valid_records.append(record)

    if missing_count:
        fallback_search(root, order_id, db_path)
        records = query_order_records(order_id, db_path)
        valid_records = []
        for record in records:
            if normalize_path(record["conf_path"]).exists():
                valid_records.append(record)

    if refreshed:
        valid_records = [
            record
            for record in query_order_records(order_id, db_path)
            if normalize_path(record["conf_path"]).exists()
        ]

    return valid_records


def get_conf(order_id, root, db_path):
    """
    Backward-compatible helper: return mappings instead of a single latest path.
    """
    return get_order_mappings(order_id, root, db_path)


def print_order_mappings(order_id, records):
    print("Order: {0}".format(order_id))
    print("Found {0} record(s)".format(len(records)))

    for index, record in enumerate(records, start=1):
        print("")
        print("{0}.".format(index))
        print("  order_id: {0}".format(record["order_id"]))
        print("  conf_name: {0}".format(record["conf_name"]))
        print("  suffix: {0}".format(record["suffix"]))
        print("  batch: {0}".format(record["batch"]))
        print("  batch_path: {0}".format(record["batch_path"]))
        print("  conf_path: {0}".format(record["conf_path"]))
        print("  mtime: {0}".format(record["mtime"]))
        print("  indexed_at: {0}".format(record["indexed_at"]))


def run_loop(root, db_path):
    """
    Long-running scan loop:
    - incremental scan immediately and every 15 minutes
    - full scan every 24 hours
    """
    incremental_interval = 15 * 60
    full_interval = 24 * 60 * 60
    last_incremental = 0.0
    last_full = time.time()

    info("Starting indexer loop")
    try:
        incremental_scan(root, db_path, limit=50)
        last_incremental = time.time()

        while True:
            now = time.time()

            if now - last_incremental >= incremental_interval:
                incremental_scan(root, db_path, limit=50)
                last_incremental = now

            if now - last_full >= full_interval:
                full_scan(root, db_path)
                last_full = now

            time.sleep(5)
    except KeyboardInterrupt:
        info("Indexer loop stopped by user")


def build_parser():
    parser = argparse.ArgumentParser(
        description="SQLite indexer for order conf files."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Initialize database")
    init_parser.add_argument("--db", required=True, help="Path to SQLite DB")

    incremental_parser = subparsers.add_parser(
        "incremental", help="Run incremental scan"
    )
    incremental_parser.add_argument("--root", required=True, help="FTGS root path")
    incremental_parser.add_argument("--db", required=True, help="Path to SQLite DB")
    incremental_parser.add_argument("--limit", type=int, default=50)

    full_parser = subparsers.add_parser("full", help="Run full scan")
    full_parser.add_argument("--root", required=True, help="FTGS root path")
    full_parser.add_argument("--db", required=True, help="Path to SQLite DB")

    query_parser = subparsers.add_parser("query", help="Query an order")
    query_parser.add_argument("--root", required=True, help="FTGS root path")
    query_parser.add_argument("--db", required=True, help="Path to SQLite DB")
    query_parser.add_argument("--order", required=True, help="Order ID")

    loop_parser = subparsers.add_parser("loop", help="Run indexer loop")
    loop_parser.add_argument("--root", required=True, help="FTGS root path")
    loop_parser.add_argument("--db", required=True, help="Path to SQLite DB")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "init":
        init_db(args.db)
        info("Initialized database: {0}".format(normalize_path(args.db)))
    elif args.command == "incremental":
        incremental_scan(args.root, args.db, limit=args.limit)
    elif args.command == "full":
        full_scan(args.root, args.db)
    elif args.command == "query":
        records = get_order_mappings(args.order, args.root, args.db)
        print_order_mappings(args.order, records)
    elif args.command == "loop":
        run_loop(args.root, args.db)


if __name__ == "__main__":
    main()
