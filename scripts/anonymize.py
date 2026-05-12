"""
One-shot anonymization for portfolio scripts.

Replacements are applied IN ORDER. Longer / more specific patterns first to
avoid cascading rewrites. Each pattern is a plain substring replacement.
"""
from pathlib import Path

UPLOADS = Path("/root/.claude/uploads/e7ac78b9-5132-477f-b470-ff2f48c9b217")
OUT = Path("/home/user/VendorReporting/src")

# (source upload filename, output filename in src/)
FILES = [
    ("6f75d4fa-wos_push.py",                 "wos_push.py"),
    ("c75be553-wmt_import.py",               "f5c_import.py"),
    ("70b9b5fd-wmt_database.py",             "f5c_database.py"),
    ("cd3e29cd-wmt_add_column.py",           "f5c_add_column.py"),
    ("750eee4d-week_mapping_setup.py",       "week_mapping_setup.py"),
    ("faa1ad68-test_belt_projection.py",     "test_category_a_projection.py"),
    ("f5927680-projection_engine.py",        "projection_engine.py"),
    ("f4f14157-productivity.py",             "productivity.py"),
    ("9aa1d4ce-dimension_tables_setup.py",   "dimension_tables_setup.py"),
]

# Order matters - longest / most specific first.
REPLACEMENTS = [
    # --- Hardcoded local paths ---
    ("C:/Users/Theo.kim/Documents/Import_10.24.25", "./data/import"),
    ("C:/Users/Theo.kim/Documents/WMT Database Tables", "./data/dimension_tables"),
    ("WMT Database Tables", "dimension_tables"),

    # --- Class names (must precede generic walmart/Walmart) ---
    ("WalmartDataImporter", "Fortune500ClientDataImporter"),
    ("BeltProjectionTester", "CategoryAProjectionTester"),
    ("WOSPushCalculator", "WOSPushCalculator"),  # generic enough
    ("WeekMappingManager", "WeekMappingManager"),
    ("DimensionTableManager", "DimensionTableManager"),

    # --- Method renames (belt-specific names that are actually generic) ---
    ("calculate_belt_style_projection", "calculate_style_projection"),
    ("calculate_belt_push", "calculate_trend_push"),
    ("calculate_rain_push", "calculate_weather_push"),
    ("calculate_grs_projection", "calculate_client_forecast_projection"),

    # --- DB filename / lookup file ---
    ("walmart_data.db", "f5c_data.db"),
    ("WeekLY_Lookup.xlsx", "week_lookup.xlsx"),

    # --- Walmart-prefixed schema columns ---
    ("walmart_calendar_week", "f5c_calendar_week"),
    ("walmart_item_number",   "f5c_item_number"),

    # --- Tables / facts that include "belt" (Category A) ---
    ("POS_belt_fact", "POS_category_a_fact"),
    ("dim_belt_style", "dim_category_a_style"),

    # --- 'randa' (vendor) ---
    ("randa_style_color", "internal_style_color"),
    ("Randa", "Internal"),
    ("randa", "internal"),

    # --- 'planalytics' (weather vendor) ---
    ("Planalytics", "ForecastVendor"),
    ("planalytics", "forecast_vendor"),

    # --- WHPK (warehouse pack) ---
    ("WHS Pack Size",  "Case Pack Size"),
    ("WHPK",           "CASE_PACK"),
    ("whpk_qty",       "case_pack_qty"),
    ("dim_whpk",       "dim_case_pack"),
    ("push_whpk",      "push_case_pack"),
    ("whpk",           "case_pack"),

    # --- GRS (client forecast system) ---
    ("GRS_fact",                 "CLIENT_FORECAST_fact"),
    ("import_GRS",               "import_client_forecast"),
    ("idx_GRS_store",            "idx_client_forecast_store"),
    ("GRS Forecast",             "Client Forecast"),
    ("'GRS'",                    "'CLIENT_FORECAST'"),
    ("GRS.csv",                  "client_forecast.csv"),
    ("grs_blend_weight",         "client_forecast_blend_weight"),
    ("grs_weeks",                "client_forecast_weeks"),
    ("use_grs",                  "use_client_forecast"),
    ("grs_df",                   "client_forecast_df"),

    # --- Walmart-specific column ---
    ("merchandise_major_zone_number", "merch_zone_number"),

    # --- Fineline -> subcategory ---
    ("fineline_number",       "subcategory_number"),
    ("fineline_description",  "subcategory_description"),
    ("fineline_filter",       "subcategory_filter"),
    ("fineline_nbr",          "subcategory_nbr"),
    ("belt_finelines",        "category_a_subcategories"),
    ("rain_finelines",        "category_d_subcategories"),
    ("neckwear_finelines",    "category_c_subcategories"),
    ("wallet_finelines",      "category_b_subcategories"),
    ("category_finelines",    "category_subcategories"),
    ("'finelines'",           "'subcategories'"),
    (": [1919, 1995]",        ": [1001, 1002]"),  # category A finelines example
    (": [812, 822, 834, 835]", ": [4001, 4002, 4003, 4004]"),
    (": [2049]",              ": [3001]"),
    (": [2258]",              ": [2001]"),
    (": [703]",               ": [5001]"),
    (": [2215, 2216, 2219]",  ": [6001, 6002, 6003]"),
    ("[1919, 1995]",          "[1001, 1002]"),  # any remaining
    ("'1919', '1995'",        "'1001', '1002'"),
    ("['1919', '1995']",      "['1001', '1002']"),
    ("Fineline",              "Subcategory"),
    ("fineline",              "subcategory"),

    # --- Belt extras / flags ---
    ("belt_extra_columns", "category_a_extra_columns"),
    ("belt_data",          "category_a_data"),
    ("is_belt",            "is_category_a"),

    # --- Output file prefixes ---
    ("belt_push_recommendation",     "category_a_push_recommendation"),
    ("wallet_push_recommendation",   "category_b_push_recommendation"),
    ("neckwear_push_recommendation", "category_c_push_recommendation"),
    ("rain_push_recommendation",     "category_d_push_recommendation"),
    ("wcw_push_recommendation",      "category_e_push_recommendation"),
    ("mcw_push_recommendation",      "category_f_push_recommendation"),
    ("belt_weekly_detailed.csv",     "category_a_weekly_detailed.csv"),
    ("wallet_weekly_detailed.csv",   "category_b_weekly_detailed.csv"),
    ("neckwear_weekly_detailed.csv", "category_c_weekly_detailed.csv"),
    ("rain_weekly_detailed.csv",     "category_d_weekly_detailed.csv"),
    ("belt_projection_test.log",     "category_a_projection_test.log"),
    ("store_793_projection_debug.csv", "store_001_projection_debug.csv"),

    # --- Logging / display strings ---
    ("BELT PROJECTION FOR STORE 793", "CATEGORY A PROJECTION FOR STORE 001"),
    ("BELT STYLE-LEVEL PROJECTION ENGINE", "STYLE-LEVEL PROJECTION ENGINE"),
    ("for store 793", "for store 001"),
    ("STORE 793",     "STORE 001"),
    ("store 793",     "store 001"),
    ("= 793",         "= 1"),
    ("store_number = 793", "store_number = 1"),

    # --- Category dictionary keys / 'name' values (in CATEGORY_CONFIGS) ---
    ("'belts':",        "'category_a':"),
    ("'wallets':",      "'category_b':"),
    ("'neckwear':",     "'category_c':"),
    ("'rain':",         "'category_d':"),
    ("'w_cold_weather':", "'category_e':"),
    ("'m_cold_weather':", "'category_f':"),

    ("'name': 'Belts'",        "'name': 'Category A'"),
    ("'name': 'Wallets'",      "'name': 'Category B'"),
    ("'name': 'Neckwear'",     "'name': 'Category C'"),
    ("'name': 'neckwear'",     "'name': 'Category C'"),
    ("'name': 'Rain'",         "'name': 'Category D'"),
    ("'name': 'w_coldweather'", "'name': 'Category E'"),
    ("'name': 'm_coldweather'", "'name': 'Category F'"),

    # 'source' values
    ("'source': 'belt'",     "'source': 'category_a'"),
    ("'source': 'non-belt'", "'source': 'non-category_a'"),

    # --- Source-table strings used in unified view ---
    ("'belt' as source_table",     "'category_a' as source_table"),
    ("'non-belt' as source_table", "'non-category_a' as source_table"),

    # --- Filename heuristics inside import scripts ---
    ("'belt' in filename",     "'category_a' in filename"),
    ("'rain' in filename",     "'category_d' in filename"),

    # Category labels assigned via heuristic
    ("category = 'Belts'",            "category = 'Category A'"),
    ("category = 'Rain'",             "category = 'Category D'"),
    ("category = 'Mixed Categories'", "category = 'Mixed Categories'"),
    ("category = 'Mixed'",            "category = 'Mixed'"),

    # --- ProjectionEngine docstring mentions ---
    ("Calculate belt projections at STYLE level",
     "Calculate style-level projections"),
    ("BELT STYLE-LEVEL PROJECTION ENGINE",
     "STYLE-LEVEL PROJECTION ENGINE"),

    # --- Comments referencing belts ---
    ("# Belt finelines",         "# Category A subcategories"),
    ("# Belt extra columns",     "# Category A extra columns"),
    ("# Bulk insert belt data",  "# Bulk insert Category A data"),
    ("# Bulk insert other category data", "# Bulk insert other category data"),
    ("# Upsert belt data",       "# Upsert Category A data"),
    ("# Separate belts from other categories",
     "# Separate Category A from other categories"),
    ("belt records",             "Category A records"),
    ("non-belt records",         "non-Category A records"),
    ("Updated: ",                "Updated: "),

    # --- Import path / filename references ---
    ("import_10.24.25",          "import"),
    ("Belts.10.20.25.csv",       "category_a_data.csv"),
    ("Data.10.20.25.csv",        "general_data.csv"),
    ("Rain.10.20.25.csv",        "category_d_data.csv"),

    # --- Display banners and remaining mentions ---
    ("WMT", "F5C"),
    ("Walmart", "Fortune500Client"),
    ("walmart", "fortune500client"),

    # --- Cleanup: residual belt mentions in comments / strings / vars ---
    # Variables in f5c_import._print_summary / clear_all_data
    ("belt_count",  "category_a_count"),
    ("belt_weeks",  "category_a_weeks"),
    (" Belt records:", " Category A records:"),

    # Source string comparisons used in wos_push to branch logic
    ("source='non-belt'", "source='non-category_a'"),
    ("source='belt'",     "source='category_a'"),
    ("if source == 'belt'", "if source == 'category_a'"),
    ("'non-belt' as source_table", "'non-category_a' as source_table"),

    # Comments / docstrings
    ("# Create belt fact table",                "# Create Category A fact table"),
    ("# Belt subcategorys",                     "# Category A subcategories"),
    ("# Main query - include belt columns",     "# Main query - include Category A columns"),
    ("# Belts style-ROP visibility (some may be NaN for non-belt or non-style_rop)",
     "# Category A style-ROP visibility (some may be NaN for non-Category A or non-style_rop)"),
    ("# fits 2 belts per mod",                  "# fits 2 units per shelf module"),
    ("# ------- Capacity clamp (2 belts fit per mod per item) -------",
     "# ------- Capacity clamp (units that fit per shelf module per item) -------"),
    ("GRS blending and style-level ROP/WOS strategy for belts.",
     "client-forecast blending and style-level ROP/WOS strategy for Category A."),
    ('logging.info("Using style-level target inventory with size coverage for Belts")',
     'logging.info("Using style-level target inventory with size coverage for Category A")'),

    # POS Records labels in dimension_tables_setup.print_summary
    ("'POS Records (Non-Belt)'", "'POS Records (Non-Category A)'"),
    ("'POS Records (Belt)'",     "'POS Records (Category A)'"),

    # Remaining GRS mentions in comments / log strings (variable names already renamed)
    ('logging.info(f"[OK] Imported {count:,} GRS forecast")',
     'logging.info(f"[OK] Imported {count:,} client forecast records")'),
    ("# lean on GRS",                                "# lean on client forecast"),
    ("# STEP: GRS INTEGRATION (any category with use_client_forecast=True)",
     "# STEP: CLIENT FORECAST INTEGRATION (any category with use_client_forecast=True)"),
    ('"\\nIntegrating GRS (blend={client_forecast_blend_weight})',
     '"\\nIntegrating client forecast (blend={client_forecast_blend_weight})'),
    ('"GRS used on {has_grs.sum():,} rows "',
     '"Client forecast used on {has_client_forecast.sum():,} rows "'),
    ('"No GRS rows returned for these subcategorys; using Engine projections only"',
     '"No client forecast rows returned for these subcategories; using Engine projections only"'),
    ("# Style-level projected weekly sales from size rows (post GRS blend)",
     "# Style-level projected weekly sales from size rows (post client forecast blend)"),
    ("# GRS visibility", "# Client forecast visibility"),

    # has_grs variable in wos_push (used in two places)
    ("has_grs", "has_client_forecast"),

    # Plural typo: "finelines" -> "subcategorys" (fix to "subcategories")
    ("subcategorys", "subcategories"),

    # Index name leftovers
    ("idx_belt_fact_", "idx_category_a_fact_"),

    # wm_ leftover in CLIENT_FORECAST_fact schema
    ("wm_item_nbr", "f5c_item_nbr"),
]


def transform(text: str) -> str:
    for old, new in REPLACEMENTS:
        text = text.replace(old, new)
    return text


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for src_name, dst_name in FILES:
        src_path = UPLOADS / src_name
        dst_path = OUT / dst_name
        original = src_path.read_text(encoding="utf-8")
        transformed = transform(original)
        dst_path.write_text(transformed, encoding="utf-8")
        print(f"  {src_name:50s} -> src/{dst_name}")
    print(f"\nWrote {len(FILES)} files to {OUT}")


if __name__ == "__main__":
    main()
