# Databricks notebook source
# Shared setup, run from the other notebooks with %run ./_bootstrap
import os
import sys

_src = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _src not in sys.path:
    sys.path.insert(0, _src)

from bridge import contract, control, loader, notification, planning, process, synapse  # noqa: E402,F401

# Datetime conversions assume UTC so wall-clock values are preserved exactly.
spark.conf.set("spark.sql.session.timeZone", "UTC")


def widget(name, default=""):
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass
    return dbutils.widgets.get(name).strip()


def load_settings():
    # A job runner cannot redirect privileged reads/writes by overriding a widget.
    path = os.path.abspath(os.path.join(_src, "..", "config", "inventory.json"))
    return contract.load_inventory(path)


# SQL Server uses the proleptic Gregorian calendar, including 0001-01-01 sentinels.
# Confirm CETAS physical encodings on the target runtime before enabling the job.
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
