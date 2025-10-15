import boto3
import time
from datetime import date, datetime, timedelta

athena = boto3.client("athena")
glue = boto3.client("glue")


def _split_table(full_name: str, default_db: str) -> tuple[str, str]:
    if "." in full_name:
        db, tbl = full_name.split(".", 1)
        return db, tbl
    return default_db, full_name


def _await_query(qid: str) -> dict:
    while True:
        resp = athena.get_query_execution(QueryExecutionId=qid)
        state = resp["QueryExecution"]["Status"]["State"]
        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(2)
    if state != "SUCCEEDED":
        reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "Athena query failed")
        raise RuntimeError(reason)
    return athena.get_query_results(QueryExecutionId=qid)


def _run_query(query: str, database: str, workgroup: str, output: str) -> dict:
    qid = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
        ResultConfiguration={"OutputLocation": output},
    )["QueryExecutionId"]
    return _await_query(qid)


def _table_exists(database: str, table: str) -> bool:
    try:
        glue.get_table(DatabaseName=database, Name=table)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False


def _parse_date(value: str | None) -> date | None:
    if not value or value.upper() == "NULL":
        return None
    return datetime.fromisoformat(value).date()


def lambda_handler(event, _context):
    database = event["athena_database"]
    silver_table = event["silver_table"]
    uid_hourly_table = event["uid_hourly_table"]
    pairs_daily_table = event["pairs_daily_table"]
    workgroup = event["athena_workgroup"]
    staging = event["athena_results_s3"]

    silver_db, silver_tbl = _split_table(silver_table, database)
    uid_db, uid_tbl = _split_table(uid_hourly_table, database)
    pairs_db, pairs_tbl = _split_table(pairs_daily_table, database)

    min_max_sql = f"""
    WITH parsed AS (
        SELECT COALESCE(
            TRY_CAST(base_date_time AS TIMESTAMP),
            TRY_CAST(
                from_iso8601_timestamp(
                    CASE
                        WHEN base_date_time IS NULL THEN NULL
                        WHEN regexp_like(base_date_time, '.*[Tt].*') THEN base_date_time
                        WHEN regexp_like(base_date_time, '.*[+-][0-9]{{2}}:?[0-9]{{2}}$')
                             OR regexp_like(base_date_time, '.*[Zz]$')
                             THEN replace(base_date_time, ' ', 'T')
                        ELSE replace(base_date_time, ' ', 'T') || 'Z'
                    END
                ) AS TIMESTAMP
            )
        ) AS event_ts
        FROM {silver_db}.{silver_tbl}
        WHERE base_date_time IS NOT NULL
    )
    SELECT DATE(min(event_ts)) AS min_dt,
           DATE(max(event_ts)) AS max_dt
    FROM parsed
    WHERE event_ts IS NOT NULL;
    """

    min_max_res = _run_query(min_max_sql, database, workgroup, staging)
    silver_rows = min_max_res["ResultSet"]["Rows"]
    if len(silver_rows) < 2:
        return {
            "create_uid_table": False,
            "create_pairs_table": False,
            "dates": [],
            "date_count": 0,
            "details": "Silver has no rows",
        }

    min_dt = _parse_date(silver_rows[1]["Data"][0].get("VarCharValue"))
    max_dt = _parse_date(silver_rows[1]["Data"][1].get("VarCharValue"))
    if not min_dt or not max_dt:
        return {
            "create_uid_table": False,
            "create_pairs_table": False,
            "dates": [],
            "date_count": 0,
            "details": "Silver timestamps incomplete",
        }

    uid_exists = _table_exists(uid_db, uid_tbl)
    pairs_exists = _table_exists(pairs_db, pairs_tbl)

    start_dt = min_dt
    if uid_exists:
        max_uid_sql = f"SELECT max(dt) AS max_dt FROM {uid_db}.{uid_tbl};"
        max_uid_res = _run_query(max_uid_sql, database, workgroup, staging)
        uid_rows = max_uid_res["ResultSet"]["Rows"]
        if len(uid_rows) > 1:
            last_dt = _parse_date(uid_rows[1]["Data"][0].get("VarCharValue"))
            if last_dt:
                start_dt = last_dt + timedelta(days=1)

    if start_dt > max_dt:
        return {
            "create_uid_table": not uid_exists,
            "create_pairs_table": not pairs_exists,
            "dates": [],
            "date_count": 0,
            "details": "No new partitions beyond existing gold table",
        }

    dates = []
    current = start_dt
    while current <= max_dt:
        dates.append({"dt": current.isoformat()})
        current += timedelta(days=1)

    return {
        "create_uid_table": not uid_exists,
        "create_pairs_table": not pairs_exists,
        "dates": dates,
        "date_count": len(dates),
        "start_dt": start_dt.isoformat(),
        "end_dt": max_dt.isoformat(),
    }
