"""Scan local TLC Parquet data and publish aggregate-only profiling results."""

import argparse
import hashlib
import json
import math
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import NamedTemporaryFile

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


CATEGORY_COLUMNS = {
    "VendorID",
    "RatecodeID",
    "payment_type",
    "store_and_fwd_flag",
    "request_source",
    "passenger_count",
}


def json_scalar(value: object) -> str | int | float | bool | None:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Unsupported aggregate value type: {type(value).__name__}")


def profile_parquet(path: Path, batch_size: int = 65_536) -> dict:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    before = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)

    with pq.ParquetFile(path) as parquet:
        schema = parquet.schema_arrow
        if len(set(schema.names)) != len(schema.names):
            raise ValueError("Duplicate column names are not supported")
        columns: list[dict] = []
        categories: dict[str, dict[str | int | float | bool | None, int]] = {}
        for field in schema:
            numeric = (
                pa.types.is_integer(field.type)
                or pa.types.is_floating(field.type)
                or pa.types.is_decimal(field.type)
            )
            ordered = (
                numeric or pa.types.is_timestamp(field.type) or pa.types.is_date(field.type)
            )
            columns.append(
                {
                    "name": field.name,
                    "type": str(field.type),
                    "null_count": 0,
                    **({"minimum": None, "maximum": None} if ordered else {}),
                    **({"negative_count": 0} if numeric else {}),
                    **(
                        {"non_finite_count": 0}
                        if pa.types.is_floating(field.type)
                        else {}
                    ),
                }
            )
            if field.name in CATEGORY_COLUMNS:
                categories[field.name] = {}

        row_count = 0
        passenger_signals = {
            "standard_rows": 0,
            "standard_null_passenger_count": 0,
            "standard_zero_passenger_count": 0,
        }
        source_present = "request_source" in schema.names
        passengers_present = "passenger_count" in schema.names
        for batch in parquet.iter_batches(batch_size=batch_size):
            row_count += batch.num_rows
            for index, result in enumerate(columns):
                values = batch.column(index)
                result["null_count"] += values.null_count
                if "non_finite_count" in result:
                    finite = pc.is_finite(values)
                    result["non_finite_count"] += (
                        pc.sum(pc.invert(finite)).as_py() or 0
                    )
                    values = pc.filter(values, finite)
                if "minimum" in result:
                    limits = pc.min_max(values).as_py()
                    low, high = limits["min"], limits["max"]
                    if low is not None:
                        previous = result["minimum"]
                        result["minimum"] = low if previous is None else min(previous, low)
                        previous = result["maximum"]
                        result["maximum"] = high if previous is None else max(previous, high)
                if "negative_count" in result:
                    result["negative_count"] += (
                        pc.sum(pc.less(values, 0)).as_py() or 0
                    )
                if result["name"] in categories:
                    counts = categories[result["name"]]
                    for entry in pc.value_counts(batch.column(index)).to_pylist():
                        key = json_scalar(entry["values"])
                        counts[key] = counts.get(key, 0) + entry["counts"]

            if source_present and passengers_present:
                standard = pc.is_null(batch.column(schema.get_field_index("request_source")))
                passengers = batch.column(schema.get_field_index("passenger_count"))
                passenger_signals["standard_rows"] += pc.sum(standard).as_py() or 0
                passenger_signals["standard_null_passenger_count"] += (
                    pc.sum(pc.and_(standard, pc.is_null(passengers))).as_py() or 0
                )
                passenger_signals["standard_zero_passenger_count"] += (
                    pc.sum(pc.and_(standard, pc.fill_null(pc.equal(passengers, 0), False))).as_py()
                    or 0
                )

        if row_count != parquet.metadata.num_rows:
            raise ValueError("Scanned row count does not match Parquet metadata")

    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError("Input file changed during profiling; discard and rerun")
    for column in columns:
        column["null_percent"] = (
            100 * column["null_count"] / row_count if row_count else None
        )
        for key in ("minimum", "maximum"):
            if key in column:
                column[key] = json_scalar(column[key])

    return {
        "format_version": 1,
        "computed_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_file": path.name,
        "source_sha256": digest.hexdigest(),
        "source_bytes": before.st_size,
        "full_scan": True,
        "row_count": row_count,
        "column_count": len(columns),
        "columns": columns,
        "categories": {
            name: [
                {"value": value, "count": count}
                for value, count in sorted(
                    counts.items(), key=lambda item: (-item[1], str(item[0]))
                )
            ]
            for name, counts in categories.items()
        },
        "source_aware_passengers": {
            "available": source_present and passengers_present,
            "reason": (
                None
                if source_present and passengers_present
                else "Both request_source and passenger_count are required; no source is inferred."
            ),
            **(passenger_signals if source_present and passengers_present else {}),
        },
    }


def write_report(report: dict, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=output.parent, suffix=".tmp", delete=False
        ) as handle:
            temporary = Path(handle.name)
            json.dump(report, handle, indent=2, allow_nan=False)
            handle.write("\n")
        os.replace(temporary, output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("reports") / "profile.json")
    args = parser.parse_args()
    if args.input.resolve() == args.output.resolve():
        parser.error("--output must not overwrite --input")
    report = profile_parquet(args.input)
    write_report(report, args.output)
    print(
        f"Profiled {report['row_count']:,} rows and {report['column_count']} columns. "
        f"Aggregate report: {args.output}"
    )


if __name__ == "__main__":
    main()
