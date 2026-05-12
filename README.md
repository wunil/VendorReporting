# Vendor Reporting

A portfolio sample of a vendor-side retail reporting and replenishment toolkit.
The original scripts ran against POS data from a large retail customer; all
client-, vendor-, and third-party-specific names have been anonymized for
public release. The structure, business logic, and SQL/pandas patterns reflect
the production code.

## Anonymization

| Real-world entity            | Placeholder used here       |
| ---------------------------- | --------------------------- |
| Retail customer              | `Fortune500Client` / `f5c`  |
| Vendor / internal company    | `internal`                  |
| Weather forecast vendor      | `ForecastVendor`            |
| Customer demand forecast     | `CLIENT_FORECAST`           |
| Warehouse pack (WHPK)        | `case_pack`                 |
| Merchandise hierarchy term   | `subcategory` (was fineline)|
| Product categories           | `Category A` ... `Category F` |

## Pipeline overview

```
                        ┌─────────────────────────────────┐
   Weekly POS CSVs ───► │  f5c_import.py / f5c_database.py│ ───► f5c_data.db
                        └─────────────────────────────────┘
                                        │
            ┌───────────────────────────┼─────────────────────────────┐
            ▼                           ▼                             ▼
   dimension_tables_setup.py   week_mapping_setup.py      productivity.py
   (stores, items,             (TY ↔ LY week lookup)      (per-category
    case-pack, client                                      weekly KPI export)
    forecast, weather)
                                        │
                                        ▼
                              projection_engine.py
                              (style-level demand model
                               with adaptive size mix)
                                        │
                                        ▼
                                  wos_push.py
                                  (Weeks-of-Supply
                                  push recommendations
                                  per category)
```

## Files

| File                              | Purpose                                                              |
| --------------------------------- | -------------------------------------------------------------------- |
| `src/f5c_database.py`             | Create the SQLite schema (POS fact tables + indexes)                 |
| `src/f5c_add_column.py`           | One-off `ALTER TABLE` migration (added inventory columns)            |
| `src/f5c_import.py`               | Bulk + upsert CSV import for the two POS fact tables                 |
| `src/dimension_tables_setup.py`   | Create + load dimension tables (stores, items, case pack, forecasts) |
| `src/week_mapping_setup.py`       | Maintain the TY ↔ LY retail-calendar week mapping                    |
| `src/productivity.py`             | Per-category weekly productivity / KPI extract                       |
| `src/projection_engine.py`        | Style-level demand projection with adaptive size-mix blending        |
| `src/test_category_a_projection.py` | Diagnostic harness for the Category A projection                   |
| `src/wos_push.py`                 | Weeks-of-Supply push calculator (trend + weather methods)            |

## Notes

- This repo contains **code only**. No data, database files, or CSV exports
  are checked in (see `.gitignore`).
- Subcategory ID values, store numbers, and example finelines have been
  changed to placeholders.
- A few scripts (`f5c_import.py`, `dimension_tables_setup.py`) reference local
  paths that originally pointed at a Windows user directory. Those have been
  changed to relative `./data/...` paths.
