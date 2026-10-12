"""Private, immutable source snapshots. No source writes or restore command."""
import hashlib
import json
import os
import re
import subprocess
import tempfile
import shutil
from collections import Counter
from pathlib import Path

SOURCES = (
    ("openbkn", "t_operation_audit", "bkn-backend", "audit"),
    ("openbkn", "t_vega_operation_audit", "vega", "audit"),
    ("openbkn", "t_execution_factory_operation_audit", "execution-factory", "audit"),
    ("openbkn", "t_model_manager_operation_audit", "model-manager", "audit"),
    ("safe", "audit_logs", "bkn-safe-admin", "audit"),
    ("safe", "access_logs", "bkn-safe-access", "audit"),
    ("openbkn", "bkn_backend_trace_outbox", "bkn-backend", "evidence"),
    ("openbkn", "ontology_query_trace_outbox", "ontology-query", "evidence"),
)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def strict_loads(text):
    def pairs(entries):
        result = {}
        for key, value in entries:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def reject_constant(value):
        raise ValueError("nonfinite JSON number")

    value = json.loads(text, object_pairs_hook=pairs, parse_constant=reject_constant)
    canonical(value)
    return value


def digest(data):
    return hashlib.sha256(data).hexdigest()


def identifier(value):
    if not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise ValueError("invalid SQL identifier")
    return "`" + value + "`"


def literal(value):
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
        raise ValueError("invalid source identifier")
    return "'" + value + "'"


def readonly(query):
    return "SET SESSION time_zone='+00:00'; SET SESSION TRANSACTION READ ONLY; START TRANSACTION WITH CONSISTENT SNAPSHOT; " + query + "; COMMIT;"


def export_query(database, table, columns, source_id, kind):
    pairs = []
    for name, datatype in columns:
        column = identifier(name)
        value = "DATE_FORMAT(%s,'%%Y-%%m-%%dT%%H:%%i:%%s.%%fZ')" % column if datatype in {"datetime", "timestamp"} else column
        if datatype in {"binary", "varbinary", "blob", "longblob", "mediumblob", "tinyblob"}:
            raise ValueError("binary column requires explicit source codec")
        pairs.append(literal(name) + "," + value)
    if not pairs:
        raise ValueError("source table has no columns")
    return "SELECT JSON_OBJECT('source_id',%s,'kind',%s,'locator',%s,'row',JSON_OBJECT(%s)) FROM %s.%s" % (
        literal(source_id), literal(kind), literal(database + "." + table), ",".join(pairs), identifier(database), identifier(table))


def private_write(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)



def private_jsonl(path, records):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        for record in records:
            stream.write((canonical(record) + '\n').encode('utf-8'))

def save_snapshot(root, records, source_deployment, metadata, on_record=None):
    root = Path(root)
    if root.exists():
        raise FileExistsError(root)
    root.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=root.name + ".pending-", dir=root.parent))
    sha, counts, count = hashlib.sha256(), Counter(), 0
    try:
        fd = os.open(staging / "records.jsonl", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as stream:
            for row in records:
                payload = (canonical(row) + "\n").encode("utf-8")
                if len(payload) > record_limit():
                    raise ValueError("source_record_exceeds_limit")
                stream.write(payload)
                sha.update(payload)
                counts[row.get("locator") or row["source_id"]] += 1
                count += 1
                if on_record and count % 1000 == 0:
                    on_record(count)
            if on_record:
                on_record(count)
            stream.flush()
            os.fsync(stream.fileno())
        manifest = {"format_version": 1, "source_deployment": source_deployment, "metadata": metadata,
                    "records_file": "records.jsonl", "records_sha256": sha.hexdigest(), "record_count": count,
                    "source_counts": dict(sorted(counts.items()))}
        private_write(staging / "snapshot.json", (canonical(manifest) + "\n").encode("utf-8"))
        os.rename(staging, root)
        return manifest
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def record_limit():
    limit = int(os.environ.get("BKN_HISTORY_RECORD_BYTES", 8 << 20))
    if not 0 < limit <= 64 << 20:
        raise ValueError("invalid_source_record_limit")
    return limit


def bounded_lines(stream):
    limit = record_limit()
    while True:
        line = stream.readline(limit + 1)
        if not line:
            return
        if len(line) > limit:
            raise ValueError("source_record_exceeds_limit")
        yield line


def read_snapshot(root):
    root = Path(root)
    manifest = strict_loads((root / "snapshot.json").read_text())
    if manifest.get("format_version") != 1 or manifest.get("records_file") != "records.jsonl":
        raise ValueError("unsupported snapshot format")
    sha, count = hashlib.sha256(), 0
    with (root / "records.jsonl").open("rb") as stream:
        for line in bounded_lines(stream):
            sha.update(line)
            count += 1
    if sha.hexdigest() != manifest["records_sha256"]:
        raise ValueError("snapshot hash mismatch")
    if count != manifest["record_count"]:
        raise ValueError("snapshot count mismatch")
    def records():
        with (root / "records.jsonl").open("rb") as stream:
            for line in bounded_lines(stream):
                yield strict_loads(line)
    return records()


class SQLSource:
    def __init__(self, profile):
        self.profile = profile

    def query(self, sql):
        profile = self.profile
        if profile["kind"] == "kubernetes":
            prefix = ["kubectl", "--context", profile["context"], "-n", profile["namespace"], "exec", profile["pod"], "--"]
        elif profile["kind"] == "docker":
            prefix = ["docker", "exec", profile["container"]]
        else:
            raise ValueError("unsupported SQL source transport")
        # Credentials remain in the database container's environment, never argv/output.
        command = 'MYSQL_PWD="$MARIADB_ROOT_PASSWORD" mariadb -uroot --quick --batch --raw --skip-column-names -e "$1"'
        # A file prevents stderr pipe deadlock; never print container diagnostics.
        with tempfile.TemporaryFile() as errors:
            process = subprocess.Popen(prefix + ["sh", "-c", command, "query", readonly(sql)],
                                       stdout=subprocess.PIPE, stderr=errors)
            try:
                for line in bounded_lines(process.stdout):
                    yield line.decode("utf-8").rstrip("\r\n")
                if process.wait():
                    raise ValueError("source_SQL_transport_failed")
            finally:
                process.stdout.close()
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()

    def export(self):
        queries, tables = [], []
        for database, table, source_id, kind in SOURCES:
            rows = list(self.query("SELECT COLUMN_NAME,DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION" % (literal(database), literal(table))))
            if not rows:
                tables.append({"locator": database + "." + table, "state": "absent"})
                continue
            columns = [tuple(row.split("\t")) for row in rows]
            queries.append(export_query(database, table, columns, source_id, kind))
            tables.append({"locator": database + "." + table, "state": "present", "columns": columns})
        lines = self.query(";".join(queries)) if queries else []
        return (strict_loads(line) for line in lines), tables


def check_blob_size(source, database, table, columns):
    """Check binary size before HEX/JSON expansion in the database container."""
    if not columns:
        return
    expression = '+'.join('COALESCE(OCTET_LENGTH(%s),0)' % identifier(column) for column in columns)
    sizes = source.query('SELECT COALESCE(MAX(%s),0) FROM %s.%s' %
                         (expression, identifier(database), identifier(table)))
    for raw in sizes:
        if int(raw) * 2 + 4096 > record_limit():
            raise ValueError('source_blob_exceeds_record_limit')
