"""Import a supplied Delta snapshot and prepare aggregate-only dashboard data."""

import argparse
import hashlib
import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from zipfile import ZipFile

import duckdb
import pyarrow.parquet as pq
from deltalake import DeltaTable

from taxi_profile import write_report


ROOT = Path(__file__).resolve().parent
RULES = {
    "DQ01": "DQ01_critical_fields",
    "DQ02": "DQ02_timestamp_consistency",
    "DQ03": "DQ03_duration",
    "DQ04": "DQ04_distance",
    "DQ05": "DQ05_code_validity",
    "DQ06": "DQ06_source_completeness",
    "DQ07": "DQ07_duplicate",
    "DQ08": "DQ08_location_validity",
    "DQ09": "DQ09_passenger_count",
    "DQ10": "DQ10_negative_payment",
}
DIMENSIONS = ["pickup_date", "pickup_hour", "vendor", "payment", "source", "status"]
FILTER_COLUMNS = ("vendor", "payment", "source", "status")
REQUIRED = {
    *RULES.values(),
    "tpep_pickup_datetime", "VendorID", "payment_type", "request_source", "dq_status",
    "trip_distance", "trip_duration_minutes", "total_amount", "tip_amount",
    "PULocationID", "DOLocationID",
}


def import_archive(archive_path: Path, destination: Path) -> str:
    if destination.exists():
        raise FileExistsError(f"Silver destination already exists: {destination}")
    digest = hashlib.sha256()
    with archive_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(archive_path) as archive, tempfile.TemporaryDirectory(
        dir=destination.parent, prefix="silver-import-"
    ) as temporary:
        logs = [
            item for item in archive.infolist()
            if item.filename.endswith("/_delta_log/00000000000000000000.json")
        ]
        if len(logs) != 1:
            raise ValueError("Expected exactly one Delta table with a version-zero log")
        prefix = logs[0].filename.removesuffix("_delta_log/00000000000000000000.json")
        paths: set[str] = set()
        extracted = 0
        for item in archive.infolist():
            name = item.filename
            parts = PurePosixPath(name).parts
            if (
                "\\" in name or ":" in name or name.startswith("/") or ".." in parts
                or any(part.endswith((" ", ".")) for part in parts)
            ):
                raise ValueError(f"Unsafe archive path: {name}")
            if item.is_dir() or not name.startswith(prefix):
                continue
            relative = name[len(prefix):]
            if not (relative.endswith(".parquet") or relative.endswith(".json")):
                continue
            normalized = str(PurePosixPath(relative)).casefold()
            if normalized in paths:
                raise ValueError(f"Duplicate archive entry: {relative}")
            paths.add(normalized)
            extracted += item.file_size
            if extracted > 1024**3:
                raise ValueError("Silver snapshot exceeds the 1 GiB import limit")
            target = Path(temporary).joinpath(*PurePosixPath(relative).parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(item) as source, target.open("wb") as output:
                shutil.copyfileobj(source, output)
        # Delta resolves the active snapshot; never glob all Parquet files.
        table = DeltaTable(temporary)
        if not table.file_uris():
            raise ValueError("Delta snapshot contains no active files")
        for uri in table.file_uris():
            if not Path(uri).resolve().is_relative_to(Path(temporary).resolve()):
                raise ValueError("Delta snapshot references a file outside the imported table")
        table.to_pyarrow_dataset()
        Path(temporary).rename(destination)
    return digest.hexdigest()


def build_gold(silver: Path, gold: Path, archive_sha256: str | None = None) -> dict:
    if gold.exists():
        raise FileExistsError(f"Gold destination already exists: {gold}")
    delta = DeltaTable(str(silver.resolve()))
    dataset = delta.to_pyarrow_dataset()
    missing = REQUIRED - set(dataset.schema.names)
    if missing:
        raise ValueError(f"Missing Silver columns: {', '.join(sorted(missing))}")
    gold.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=gold.parent, prefix="gold-build-") as temporary:
        with duckdb.connect() as connection:
            connection.execute("SET threads = 4")
            connection.register("silver", dataset)
            for column in [*RULES.values(), "dq_status"]:
                unexpected = connection.execute(
                    f'SELECT "{column}", count(*) FROM silver '
                    f'WHERE "{column}" IS NULL OR "{column}" NOT IN '
                    "('PASS', 'WARNING', 'FAIL') GROUP BY 1"
                ).fetchall()
                if unexpected:
                    raise ValueError(f"Unknown or missing outcomes in {column}: {unexpected}")
            any_fail = " OR ".join(f'"{column}" = \'FAIL\'' for column in RULES.values())
            any_warn = " OR ".join(f'"{column}" = \'WARNING\'' for column in RULES.values())
            disagreement = connection.execute(
                "SELECT count(*) FROM silver WHERE dq_status <> CASE "
                f"WHEN {any_fail} THEN 'FAIL' WHEN {any_warn} THEN 'WARNING' ELSE 'PASS' END"
            ).fetchone()[0]
            overview = connection.execute(
                "SELECT count(*), min(tpep_pickup_datetime), max(tpep_pickup_datetime), "
                "count(*) FILTER (WHERE tpep_pickup_datetime IS NULL) FROM silver"
            ).fetchone()
            connection.execute(
                """
                CREATE TEMP VIEW trips AS SELECT *,
                    CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
                    hour(tpep_pickup_datetime) AS pickup_hour,
                    CAST(VendorID AS VARCHAR) AS vendor,
                    CAST(payment_type AS VARCHAR) AS payment,
                    request_source AS source,
                    dq_status AS status
                FROM silver
                """
            )
            metrics = [
                "count(*) AS trips",
                "sum(total_amount) FILTER (WHERE isfinite(total_amount)) AS amount_sum",
                "count(*) FILTER (WHERE isfinite(total_amount)) AS amount_count",
                "sum(tip_amount) FILTER (WHERE isfinite(tip_amount)) AS tip_sum",
                "sum(trip_distance) FILTER (WHERE isfinite(trip_distance)) AS distance_sum",
                "count(*) FILTER (WHERE isfinite(trip_distance)) AS distance_count",
                "sum(trip_duration_minutes) AS duration_sum",
                "count(trip_duration_minutes) AS duration_count",
                f"count(*) FILTER (WHERE {any_fail}) AS rows_with_rule_fail",
                f"count(*) FILTER (WHERE NOT ({any_fail}) AND ({any_warn})) AS warning_only_rows",
            ]
            for rule, column in RULES.items():
                for outcome in ("PASS", "WARNING", "FAIL"):
                    metrics.append(
                        f'count(*) FILTER (WHERE "{column}" = \'{outcome}\') '
                        f'AS "{rule}_{outcome.lower()}"'
                    )
            dimensions = ", ".join(DIMENSIONS)
            cube = connection.execute(
                f"SELECT {dimensions}, {', '.join(metrics)} FROM trips GROUP BY {dimensions}"
            ).to_arrow_table()
            pq.write_table(cube, Path(temporary) / "trips.parquet", compression="zstd")
            zone_dimensions = ", ".join(column for column in DIMENSIONS if column != "pickup_hour")
            zones = connection.execute(
                f"SELECT {zone_dimensions}, 'Pickup' AS direction, "
                f"PULocationID AS location_id, count(*) AS trips FROM trips "
                f"GROUP BY {zone_dimensions}, PULocationID UNION ALL "
                f"SELECT {zone_dimensions}, 'Drop-off' AS direction, "
                f"DOLocationID AS location_id, count(*) AS trips FROM trips "
                f"GROUP BY {zone_dimensions}, DOLocationID"
            ).to_arrow_table()
            pq.write_table(zones, Path(temporary) / "zones.parquet", compression="zstd")
            options = {
                column: [
                    row[0] for row in connection.execute(
                        f"SELECT DISTINCT {column} FROM trips ORDER BY {column} NULLS FIRST"
                    ).fetchall()
                ]
                for column in FILTER_COLUMNS
            }
        manifest = {
            "format_version": 1,
            "computed_at_utc": datetime.now(timezone.utc).isoformat(),
            "silver_table": silver.name,
            "delta_version": delta.version(),
            "active_files": len(delta.file_uris()),
            "archive_sha256": archive_sha256,
            "rows": overview[0],
            "pickup_min": overview[1].isoformat() if overview[1] else None,
            "pickup_max": overview[2].isoformat() if overview[2] else None,
            "missing_pickup_rows": overview[3],
            "status_rule_disagreement_rows": disagreement,
            "columns": [{"name": field.name, "type": str(field.type)} for field in dataset.schema],
            "options": options,
            "rules": RULES,
        }
        write_report(manifest, Path(temporary) / "manifest.json")
        Path(temporary).rename(gold)
    return manifest


def query_gold(
    gold: Path,
    date_range: tuple[str, str] | None,
    selections: dict[str, list[str | None]],
) -> dict:
    clauses: list[str] = []
    parameters: list[object] = []
    if date_range is not None:
        clauses.append("pickup_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)")
        parameters.extend(date_range)
    for column in FILTER_COLUMNS:
        if column not in selections:
            continue
        values = selections[column]
        if not values:
            clauses.append("FALSE")
            continue
        non_null = [value for value in values if value is not None]
        parts: list[str] = []
        if non_null:
            parts.append(f"{column} IN ({', '.join('?' for _ in non_null)})")
            parameters.extend(non_null)
        if None in values:
            parts.append(f"{column} IS NULL")
        clauses.append(f"({' OR '.join(parts)})")
    where = " AND ".join(clauses) if clauses else "TRUE"
    with duckdb.connect() as connection:
        connection.execute("SET threads = 4")
        for name in ("trips", "zones"):
            connection.read_parquet(str(gold / f"{name}.parquet")).create_view(name)
        connection.execute(
            f"CREATE TEMP TABLE filtered AS SELECT * FROM trips WHERE {where}", parameters
        )
        totals = connection.execute(
            """
            SELECT coalesce(sum(trips), 0) AS trips, sum(amount_sum) AS total_amount,
                sum(amount_sum) / nullif(sum(amount_count), 0) AS average_amount,
                sum(distance_sum) / nullif(sum(distance_count), 0) AS average_distance,
                sum(duration_sum) / nullif(sum(duration_count), 0) AS average_duration,
                coalesce(sum(rows_with_rule_fail), 0) AS rows_with_rule_fail,
                coalesce(sum(warning_only_rows), 0) AS warning_only_rows
            FROM filtered
            """
        ).fetchdf().iloc[0].to_dict()
        status = connection.execute(
            "SELECT status, sum(trips) AS trips FROM filtered GROUP BY status ORDER BY status"
        ).fetchdf()
        daily = connection.execute(
            "SELECT pickup_date, status, sum(trips) AS trips, "
            "sum(amount_sum) AS amount_sum, sum(amount_count) AS amount_count, "
            "sum(distance_sum) AS distance_sum, sum(distance_count) AS distance_count, "
            "sum(duration_sum) AS duration_sum, sum(duration_count) AS duration_count "
            "FROM filtered "
            "GROUP BY pickup_date, status ORDER BY pickup_date"
        ).fetchdf()
        hourly = connection.execute(
            "SELECT pickup_hour, sum(trips) AS trips FROM filtered "
            "GROUP BY pickup_hour ORDER BY pickup_hour"
        ).fetchdf()
        rules = connection.execute(
            " UNION ALL ".join(
                f"SELECT '{rule}' AS rule, coalesce(sum(\"{rule}_pass\"), 0) AS pass, "
                f"coalesce(sum(\"{rule}_warning\"), 0) AS warning, "
                f"coalesce(sum(\"{rule}_fail\"), 0) AS fail FROM filtered"
                for rule in RULES
            )
        ).fetchdf()
        distributions = {
            column: connection.execute(
                f"SELECT {column}, sum(trips) AS trips, sum(amount_sum) AS total_amount, "
                "sum(amount_sum) / nullif(sum(amount_count), 0) AS average_amount, "
                "sum(distance_sum) / nullif(sum(distance_count), 0) AS average_distance, "
                "100.0 * sum(rows_with_rule_fail) / sum(trips) AS fail_percent FROM filtered "
                f"GROUP BY {column} ORDER BY trips DESC"
            ).fetchdf()
            for column in ("vendor", "payment", "source")
        }
        heatmap = connection.execute(
            "SELECT isodow(pickup_date) AS weekday, pickup_hour, sum(trips) AS trips, "
            "sum(amount_sum) AS total_amount, "
            "100.0 * sum(rows_with_rule_fail) / sum(trips) AS fail_percent "
            "FROM filtered WHERE pickup_date IS NOT NULL AND pickup_hour IS NOT NULL "
            "GROUP BY weekday, pickup_hour ORDER BY weekday, pickup_hour"
        ).fetchdf()
        zones = connection.execute(
            f"SELECT direction, location_id, sum(trips) AS trips FROM zones WHERE {where} "
            "GROUP BY direction, location_id ORDER BY trips DESC", parameters
        ).fetchdf()
    return {
        "totals": totals, "status": status, "daily": daily, "hourly": hourly,
        "rules": rules, "distributions": distributions, "zones": zones, "heatmap": heatmap,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path)
    parser.add_argument(
        "--silver", type=Path, default=ROOT / "data_lake" / "silver" / "taxi_trips_dq"
    )
    parser.add_argument(
        "--gold", type=Path, default=ROOT / "data_lake" / "gold" / "taxi_dashboard"
    )
    args = parser.parse_args()
    fingerprint = import_archive(args.archive, args.silver) if args.archive else None
    manifest = build_gold(args.silver, args.gold, fingerprint)
    print(
        f"Silver Delta v{manifest['delta_version']}: {manifest['rows']:,} rows, "
        f"{manifest['active_files']} active files. Gold aggregates: {args.gold}"
    )


if __name__ == "__main__":
    main()
