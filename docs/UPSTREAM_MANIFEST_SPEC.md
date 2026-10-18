# Upstream manifest: file contract

For the team that runs the upstream pipeline rebuilding the Synapse source tables.

Every night, after your pipeline has finished rebuilding **all** source tables,
write two files to Azure storage:

1. `row_counts.csv`: one row per table, with the number of rows you loaded;
2. `_SUCCESS`: a small JSON marker, written **last**, saying the CSV is complete.

The Databricks bridge starts copying only when both files exist and agree with
each other. It uses your row counts to check that every copy is complete.

Examples: `config/row_counts.example.csv` and `config/success_marker.example.json`.

## 1. Where

Both files go in the same folder, one folder per business date:

```
abfss://<container>@<account>.dfs.core.windows.net/upstream-manifests/2026-10-12/row_counts.csv
abfss://<container>@<account>.dfs.core.windows.net/upstream-manifests/2026-10-12/_SUCCESS
```

The container, account and folder pattern are agreed once with the bridge team
(the date can also be written as `20261012`). The container must be separate
from the bridge's staging container; the bridge only reads it.

## 2. `row_counts.csv`

```
business_date,upstream_run_id,schema,table,row_count
2026-10-12,2026-10-12,dbo,Customer,1204331
2026-10-12,2026-10-12,dbo,SalesOrder,58412907
```

| Column | Required | Value |
|---|---|---|
| `business_date` | Yes | The business date, `yyyy-mm-dd` (see section 4). Same value on every row. |
| `upstream_run_id` | Yes | Not blank. Put the business date here as well (`2026-10-12`). Same value on every row. |
| `upstream_sequence` | No | Leave this column out. Add it only for a same-day correction (section 7). |
| `schema` | Yes | Synapse schema of the object, e.g. `dbo`. |
| `table` | Yes | Synapse table or view name. Not case-sensitive. |
| `row_count` | Yes | Rows in that object now: a whole number, no separators or decimals (`1204331`, not `1,204,331`). |

Format rules:

* First line is the header. Column order does not matter.
* UTF-8 (a BOM is fine), comma-delimited, standard CSV quoting. Windows or Unix line endings are both fine.
* One row per object. List the object that the bridge actually copies: if the
  bridge copies a view, give the view's count, not its base table's.
* Rows for objects the bridge does not copy are ignored. A table missing from the
  file is counted by the bridge itself, which is slower; please include every table.
* Header names or the delimiter can differ if agreed in advance; the bridge maps them by configuration.
* Keep the file under 20 MB.

### Why `upstream_run_id`, and why is it the date?

`business_date`, `schema`, `table` and `row_count` carry the counts.
`upstream_run_id` ties the `_SUCCESS` marker to one specific version of the CSV,
so an old marker can never certify a CSV that is being rewritten for the same
date. Your pipeline writes one file per business date, so the date already
identifies the file; repeating it in `upstream_run_id` satisfies that check with
nothing extra to produce. The bridge still requires the column to be present and
not blank.

Without an `upstream_sequence` column the bridge uses `yyyymmdd00` (e.g.
`2026101200`) as the version of the night. That number is the write guard on
every target table: a table skips any version equal to or lower than the one it
already holds, so a retry never loads twice and an older night never overwrites a
newer one.

### When the night fails

The whole night fails (nothing is refreshed) if a required column is missing,
the file has no rows, or rows disagree on `business_date`, `upstream_run_id` or
`upstream_sequence`. A single bad row (for example a non-numeric count) only
makes the bridge count that table itself.

## 3. `_SUCCESS`

A JSON object naming exactly the CSV it certifies:

```json
{"business_date": "2026-10-12", "upstream_run_id": "2026-10-12", "upstream_sequence": 2026101200}
```

* `business_date` and `upstream_run_id` must equal the CSV's.
* `upstream_sequence` is always required here, even though the CSV leaves it out:
  use `yyyymmdd00` for the business date. It is a JSON number, not text.
* Under 64 KB. An empty marker is not accepted.
* Write it **only after** the CSV is completely written (or write the CSV under a
  temporary name and rename it, then write the marker).

## 4. Which date is the business date

The business date is the **Sydney calendar date of the morning the bridge loads
the data**, not the date your pipeline finished.

The bridge runs at **01:30 Sydney time** and looks only for that day's date.
If your pipeline finishes at 23:30 on 11 October, label the files `2026-10-12`.
A file labelled `2026-10-11` would be ignored and the night would fail.

## 5. Timing

* Both files must exist by **01:30 Sydney time**. The bridge then waits up to the
  agreed wait (`upstream_wait_minutes`; 60 minutes in the example settings, no wait
  if the setting is left out) and, if they still have not appeared,
  fails the night; every table keeps the previous day's data.
* Nothing is published after **07:00 Sydney**.
* The bridge judges freshness only by the values inside the files, never by file timestamps.

## 6. Do not change the source tables while the bridge is copying

After writing `_SUCCESS`, do not modify any source table until the bridge's run
for that night has finished. The bridge team will tell you how you receive that
signal (it is the `FINISHED` or `FINISHED_WITH_FAILURES` event of the run). The
bridge cannot detect a table that changes while it is being copied.

## 7. Same-day corrections

Your pipeline normally runs once per business date, so this should not happen.
It also applies if a failed run is rerun after the bridge has already loaded
that date. To republish corrected data for the same business date:

1. Wait until the bridge's current run has finished (section 6).
2. **Delete the old `_SUCCESS`** before touching any source table.
3. Rebuild the sources.
4. Write the new CSV with an added `upstream_sequence` column holding the **next
   revision**: `2026101201` (then `…02`, …), and a new `upstream_run_id` such as
   `2026-10-12-01`.
5. Write the new `_SUCCESS` last, with the same three values.

Then ask the bridge team to run the job again (same-day reruns are possible until
11:00 Sydney). Reusing the same sequence makes the bridge treat the night as already loaded.

## 8. Checklist before the first night

- [ ] Agreed folder path, container and access (bridge has read-only access).
- [ ] Sample CSV for a real night sent to the bridge team, with the real header names.
- [ ] `_SUCCESS` written last, with JSON matching the CSV.
- [ ] Business date is the Sydney date of the morning load.
- [ ] Every copied table and view is in the file, with counts of the copied object.
- [ ] Correction procedure (section 7) and the no-change window (section 6) agreed.
