import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from projection_engine import ProjectionEngine

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('wos_calculation.log'),
        logging.StreamHandler()
    ]
)

# =============================================================================
# CATEGORY CONFIGURATIONS
# =============================================================================
CATEGORY_CONFIGS = {
    'category_a': {
        'name': 'Category A',
        'subcategories': [1001, 1002],
        'method': 'trend',
        'source': 'category_a',
        'output_file': 'category_a_push_recommendation',
        'use_style_projection': 'True',
        'use_client_forecast': True,
        'client_forecast_blend_weight': 0.8,
        'client_forecast_weeks': 4,
        'push_strategy': 'style_rop',   # style-level target (WOS) + size coverage
        'style_target_wos': 10,
        'size_min_wos': 2,
        'hysteresis_weeks': 0.25,
        'min_push_units': 1,
        'lead_time_weeks': 2.0,
        'safety_weeks': 1.5,
        'use_rop_gate': True,
        'rop_floor': 0.95,          # compare vs historical avg pipeline (optional)
        'front_capacity_per_item': 4,  # fits 2 units per shelf module
        'backroom_buffer_units': 6,     # allow 0–2 extra if you want
        # ROP + throttle
        'use_rop_gate': True,
        'z_service': 0.4,          # ← loosen/tighten: 1.0–1.65
        'ramp_weeks': 0.8,        # ← throttle softness around ROP
        # Capacity & manual-order policy
        'shelf_fill_cap_low': 0.80,    # 60% cap (default)
        'shelf_fill_cap_top': 0.95,    # 75% cap for top doors
        'top_door_pct': 0.20,          # top 20% doors by baseline sales
        'zero_pipeline_first': True
    },
    'category_b': {
        'name': 'Category B',
        'subcategories': [2001],
        'method': 'trend',
        'source': 'non-category_a',
        'output_file': 'category_b_push_recommendation',
        'use_style_projection': 'True',
        'use_client_forecast': True,
        'client_forecast_blend_weight': 1,        # lean on client forecast
        'client_forecast_weeks': 4,
        'push_strategy': 'wos',
        'target_wos': 8,
        'hysteresis_weeks': 0.5,
        'min_push_units': 1
    },
    'category_d': {
        'name': 'Category D',
        'subcategories': [4001, 4002, 4003, 4004],
        'method': 'weather',
        'source': 'non-category_a',
        'output_file': 'category_d_push_recommendation',
        'use_style_projection': 'False',
        'use_client_forecast': False,
        'client_forecast_blend_weight': 0.0,
        'client_forecast_weeks': 4,
        'push_strategy': 'wos',
        'target_wos': 6,
        'hysteresis_weeks': 0.5,
        'min_push_units': 1
    },
    'category_c': {
        'name': 'Category C',
        'subcategories': [3001],
        'method': 'trend',
        'source': 'non-category_a',
        'output_file': 'category_c_push_recommendation',
        'use_style_projection': 'True',
        'use_client_forecast': False,
        'client_forecast_blend_weight': .5,
        'client_forecast_weeks': 4,
        'push_strategy': 'wos',
        'target_wos': 10,
        'hysteresis_weeks': 0.5,
        'min_push_units': 1
    },
    'category_e': {
        'name': 'Category E',
        'subcategories': [5001],
        'method': 'trend',
        'source': 'non-category_a',
        'output_file': 'category_e_push_recommendation',
        'use_style_projection': 'True',
        'use_client_forecast': False,
        'client_forecast_blend_weight': 0.0,
        'client_forecast_weeks': 4,
        'push_strategy': 'wos',
        'target_wos': 6,
        'hysteresis_weeks': 0.5,
        'min_push_units': 1,
        'convert_to_case_pack': True  
    # Add more categories here as needed
    },
    'category_f': {
        'name': 'Category F',
        'subcategories': [6001, 6002, 6003],
        'method': 'trend',
        'source': 'non-category_a',
        'output_file': 'category_f_push_recommendation',
        'use_style_projection': 'True',
        'use_client_forecast': False,
        'client_forecast_blend_weight': 0.0,
        'client_forecast_weeks': 4,
        'push_strategy': 'wos',
        'target_wos': 4,
        'hysteresis_weeks': 0.5,
        'min_push_units': 1,
        'convert_to_case_pack': True  
    },
}

class WOSPushCalculator:
    def __init__(self, db_path='f5c_data.db'):
        self.db_path = db_path
        self.target_wos = 6
        self.max_wos = 15
        self.weather_floor = -0.1
        self.projection_engine = ProjectionEngine()
    
    def get_ly_week(self, ty_week):
        """Get corresponding LY week from week mapping table"""
        conn = self.connect_db()
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT ly_week FROM dim_week_mapping
        WHERE ty_week = ?
        ''', (ty_week,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return result[0]
        else:
            logging.warning(f"No mapping found for TY week {ty_week}, using default (TY - 100)")
            return ty_week - 100
    
    def connect_db(self):
        return sqlite3.connect(self.db_path)
    
    def create_unified_view(self):
        """Create pos_all view that combines both fact tables"""
        logging.info("Creating unified POS view...")
        
        conn = self.connect_db()
        cursor = conn.cursor()
        
        cursor.execute("DROP VIEW IF EXISTS pos_all")
        
        cursor.execute("""
        CREATE VIEW pos_all AS
        SELECT
            f5c_calendar_week,
            subcategory_description,
            subcategory_number,
            store_number,
            f5c_item_number,
            pos_quantity_this_year,
            pos_quantity_last_year,
            store_in_warehouse_quantity_this_year,
            store_in_warehouse_quantity_last_year,
            store_in_transit_quantity_this_year,
            store_in_transit_quantity_last_year,
            store_on_order_quantity_this_year,
            store_on_order_quantity_last_year,
            store_on_hand_quantity_this_year,
            store_on_hand_quantity_last_year,
            pos_sales_this_year,
            pos_sales_last_year,
            max_shelf_quantity_this_year,
            store_on_hand_quantity_this_year_eop,
            store_on_hand_quantity_last_year_eop,
            traited_store_count_this_year,
            valid_store_count_this_year,
            traited_store_count_last_year,
            vendor_stock_id,
            NULL as color_description,
            NULL as trait_description,
            'non-category_a' as source_table
        FROM POS_fact
        
        UNION ALL
        
        SELECT
            f5c_calendar_week,
            subcategory_description,
            subcategory_number,
            store_number,
            f5c_item_number,
            pos_quantity_this_year,
            pos_quantity_last_year,
            store_in_warehouse_quantity_this_year,
            store_in_warehouse_quantity_last_year,
            store_in_transit_quantity_this_year,
            store_in_transit_quantity_last_year,
            store_on_order_quantity_this_year,
            store_on_order_quantity_last_year,
            store_on_hand_quantity_this_year,
            store_on_hand_quantity_last_year,
            pos_sales_this_year,
            pos_sales_last_year,
            max_shelf_quantity_this_year,
            store_on_hand_quantity_this_year_eop,
            store_on_hand_quantity_last_year_eop,
            traited_store_count_this_year,
            valid_store_count_this_year,
            traited_store_count_last_year,
            vendor_stock_id,
            color_description,
            trait_description,
            'category_a' as source_table
        FROM POS_category_a_fact
        """)
        
        conn.commit()
        conn.close()
        logging.info("[OK] Unified view created")
    
    def get_latest_week(self):
        """Get the most recent week in the database"""
        conn = self.connect_db()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(f5c_calendar_week) FROM pos_all")
        latest_week = cursor.fetchone()[0]
        conn.close()
        return latest_week
    
    def calculate_trend_push(self,
        category_subcategories,
        category_name,
        output_file=None,
        source='non-category_a',
        use_style_projection=False,
        use_client_forecast=False,
        client_forecast_blend_weight=1.0,
        client_forecast_weeks=4,
        push_strategy='wos',
        style_target_wos=10,
        size_min_wos=2,
        target_wos=None,
        hysteresis_weeks=0.5,
        min_push_units=1,
        export_store_clusters=False,
        convert_to_case_pack=False
    ):
        """
        Calculate WOS-based push recommendations (trend method), with optional
        client-forecast blending and style-level ROP/WOS strategy for Category A.
        Optionally converts push_units to CASE_PACK packs when convert_to_case_pack=True.
        """
        if target_wos is None:
            target_wos = self.target_wos
        
        logging.info("="*60)
        logging.info(f"CALCULATING WOS PUSH FOR: {category_name.upper()}")
        logging.info("Using 13-week TY vs LY trend methodology")
        logging.info("="*60)
        
        conn = self.connect_db()
        subcategory_filter = ','.join([f"'{fl}'" for fl in category_subcategories])
        latest_week = self.get_latest_week()
        
        logging.info(f"Current TY week: {latest_week}")
        
        ty_start_week = latest_week - 12
        ly_latest = self.get_ly_week(latest_week)
        ly_start = self.get_ly_week(ty_start_week)
        
        logging.info(f"TY period: {ty_start_week} to {latest_week}")
        logging.info(f"LY period: {ly_start} to {ly_latest}")
        
        # Main query - include Category A columns
        query = f"""
        SELECT
            p.f5c_calendar_week,
            p.subcategory_number,
            p.subcategory_description,
            p.store_number,
            p.f5c_item_number,
            p.vendor_stock_id,
            p.color_description,
            p.trait_description,
            bs.internal_style_color,
            p.pos_quantity_this_year,
            p.pos_quantity_last_year,
            p.store_on_hand_quantity_this_year_eop as on_hand_ty,
            p.store_on_hand_quantity_last_year_eop as on_hand_ly,
            p.store_on_hand_quantity_this_year,
            p.store_on_hand_quantity_last_year,
            p.store_in_warehouse_quantity_this_year as in_warehouse_ty,
            p.store_in_warehouse_quantity_last_year as in_warehouse_ly,
            p.store_in_transit_quantity_this_year as in_transit_ty,
            p.store_in_transit_quantity_last_year as in_transit_ly,
            p.store_on_order_quantity_this_year as on_order_ty,
            p.store_on_order_quantity_last_year as on_order_ly,
            p.pos_sales_this_year,
            p.pos_sales_last_year,
            p.max_shelf_quantity_this_year,
            s.region,
            s.state,
            s.zip_code,
            s.merch_zone_number
        FROM pos_all p
        LEFT JOIN dim_stores s ON p.store_number = s.store_number
        LEFT JOIN dim_category_a_style bs ON p.f5c_item_number = bs.customer_style_number
        WHERE p.subcategory_number IN ({subcategory_filter})
        AND p.f5c_calendar_week >= {ty_start_week}
        AND p.f5c_calendar_week <= {latest_week}
        """
        
        ly_query = f"""
        SELECT
            p.f5c_calendar_week,
            p.store_number,
            p.f5c_item_number,
            p.vendor_stock_id,
            p.color_description,
            bs.internal_style_color,
            p.pos_quantity_this_year
        FROM pos_all p
        LEFT JOIN dim_category_a_style bs ON p.f5c_item_number = bs.customer_style_number
        WHERE p.subcategory_number IN ({subcategory_filter})
        AND p.f5c_calendar_week >= {ly_start}
        AND p.f5c_calendar_week <= {ly_latest}
        """

        logging.info("Loading data...")
        df = pd.read_sql(query, conn)
        ly_df = pd.read_sql(ly_query, conn)
        conn.close()

        if len(df) == 0:
            logging.error(f"No data found for {category_name}")
            return pd.DataFrame(), pd.DataFrame()

        logging.info(f"Loaded {len(df):,} records")

        # ------------------------------------------------------------
        # NEW: 13-week total POS (TY) per store/item (no averages)
        # ------------------------------------------------------------
        ty_13wk_totals = (
            df.groupby(['store_number', 'f5c_item_number'])['pos_quantity_this_year']
            .sum()
            .reset_index()
        )
        ty_13wk_totals.columns = ['store_number', 'f5c_item_number', 'ty_13wk_pos_total']

        # ------------------------------------------------------------
        # NEW: 52-week total POS (TY) per store/item (no averages)
        # ------------------------------------------------------------
        ty52_start_week = latest_week - 51
        conn = self.connect_db()
        ty_52wk_query = f"""
            SELECT
                store_number,
                f5c_item_number,
                SUM(pos_quantity_this_year) AS ty_52wk_pos_total
            FROM pos_all
            WHERE subcategory_number IN ({subcategory_filter})
            AND f5c_calendar_week BETWEEN {ty52_start_week} AND {latest_week}
            GROUP BY store_number, f5c_item_number
        """
        ty_52wk_totals = pd.read_sql(ty_52wk_query, conn)
        conn.close()

        logging.info(f"Computed 13-week totals for {len(ty_13wk_totals):,} store/items")
        logging.info(f"Computed 52-week totals for {len(ty_52wk_totals):,} store/items")

        # Calculate 13-week TY average per store/item (existing logic)
        ty_avg = df.groupby(['store_number', 'f5c_item_number']).agg({
            'pos_quantity_this_year': 'mean'
        }).reset_index()
        ty_avg.columns = ['store_number', 'f5c_item_number', 'ty_13wk_avg']

        current_week = df[df['f5c_calendar_week'] == latest_week].copy()
        current_week['wtd_pos'] = current_week['pos_quantity_this_year']

        
        lw_df = df[df['f5c_calendar_week'] == latest_week - 1][
            ['store_number', 'f5c_item_number', 'pos_quantity_this_year']
        ].copy()
        lw_df.columns = ['store_number', 'f5c_item_number', 'lw_pos']

        # --- POS for the last 4 completed weeks: pos_m1..pos_m4 (m1 = last week) ---
        recent_pos = df[
            (df["f5c_calendar_week"] >= latest_week - 4) &
            (df["f5c_calendar_week"] <= latest_week - 1)
        ][["store_number", "f5c_item_number", "f5c_calendar_week", "pos_quantity_this_year"]].copy()

        # Map each row to an offset: 1..4 where 1 = last week, 4 = four weeks ago
        recent_pos["offset"] = latest_week - recent_pos["f5c_calendar_week"]  # 1..4

        # Pivot offsets to columns
        pos_4wk_pivot = (
            recent_pos
            .pivot_table(
                index=["store_number", "f5c_item_number"],
                columns="offset",
                values="pos_quantity_this_year",
                aggfunc="sum",
                fill_value=0
            )
            .reset_index()
        )

        # Rename columns to pos_m1 .. pos_m4
        pos_4wk_pivot = pos_4wk_pivot.rename(columns={
            1: "pos_m1", 2: "pos_m2", 3: "pos_m3", 4: "pos_m4"
        })

        # Ensure missing columns exist (e.g., if some weeks weren’t present in df)
        for c in ["pos_m1", "pos_m2", "pos_m3", "pos_m4"]:
            if c not in pos_4wk_pivot.columns:
                pos_4wk_pivot[c] = 0

        # --- merge into result downstream ---
        
        result = current_week.merge(ty_avg, on=["store_number", "f5c_item_number"], how="left")
        result = result.merge(lw_df, on=["store_number", "f5c_item_number"], how="left")
        result = result.merge(
            pos_4wk_pivot[["store_number", "f5c_item_number", "pos_m1", "pos_m2", "pos_m3", "pos_m4"]],
            on=["store_number", "f5c_item_number"],
            how="left"
        )

        # Merge in the new POS totals
        result = result.merge(
            ty_13wk_totals,
            on=['store_number', 'f5c_item_number'],
            how='left'
        )
        result = result.merge(
            ty_52wk_totals,
            on=['store_number', 'f5c_item_number'],
            how='left'
        )


        # Safety: coerce numeric
        for col in ["lw_pos", "pos_m1", "pos_m2", "pos_m3", "pos_m4"]:
            result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0)
        
        result = result.infer_objects(copy=False)
        numeric_cols = ['ty_13wk_avg', 'lw_pos', 'wtd_pos', 
                        'ty_13wk_pos_total', 'ty_52wk_pos_total',
                        'on_hand_ty', 'on_hand_ly',
                        'store_on_hand_quantity_this_year', 'store_on_hand_quantity_last_year',
                        'in_warehouse_ty', 'in_warehouse_ly',
                        'in_transit_ty', 'in_transit_ly',
                        'on_order_ty', 'on_order_ly',
                        'pos_sales_this_year', 'pos_sales_last_year',
                        'max_shelf_quantity_this_year']
        for col in numeric_cols:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors='coerce').fillna(0)

        # ----------------------------------------------------
        # OPTIONAL CASE_PACK MERGE (e.g., cold weather uses this)
        # ----------------------------------------------------
        if convert_to_case_pack:
            logging.info("Merging CASE_PACK quantities from dim_case_pack...")
            conn = self.connect_db()
            case_pack = pd.read_sql("SELECT item_nbr, case_pack_qty FROM dim_case_pack", conn)
            conn.close()

            case_pack['case_pack_qty'] = pd.to_numeric(case_pack['case_pack_qty'], errors='coerce').fillna(1)
            case_pack['case_pack_qty'] = case_pack['case_pack_qty'].replace(0, 1)

            result = result.merge(
                case_pack,
                left_on='f5c_item_number',
                right_on='item_nbr',
                how='left'
            )
            result['case_pack_qty'] = pd.to_numeric(result.get('case_pack_qty', 1), errors='coerce').fillna(1)
            result['case_pack_qty'] = result['case_pack_qty'].replace(0, 1)

        # Safely compute ship_qty and sell_through_pct
        sold = pd.to_numeric(result["pos_quantity_this_year"], errors="coerce").fillna(0)
        on_hand = pd.to_numeric(result["on_hand_ty"], errors="coerce").fillna(0)

        result["ship_qty"] = sold + on_hand

        # avoid divide-by-zero; clip to [0,1]; round for reporting
        den = result["ship_qty"]
        result["sell_through_pct"] = np.where(den > 0, sold / den, np.nan)
        result["sell_through_pct"] = np.clip(result["sell_through_pct"], 0, 1).round(4)

        # optional quick QC flag you can filter on later
        result["sell_through_flag"] = np.where(den <= 0, "check", "")

        # ==================================================================
        # PROJECTION CALCULATION
        # ==================================================================
        if use_style_projection:
            logging.info("\nUsing STYLE-BASED projection engine...")
            # Ensure style column available/fallbacks
            for frame in (df, ly_df, current_week):
                if 'internal_style_color' in frame.columns:
                    frame['internal_style_color'] = frame['internal_style_color'].fillna(frame['vendor_stock_id'])
                else:
                    frame['internal_style_color'] = frame['vendor_stock_id']

            projection_result = self.projection_engine.calculate_style_projection(
                df=df,
                ly_df=ly_df,
                current_week_df=current_week,
                style_column='internal_style_color',
                latest_week=latest_week,
                ly_current=self.get_ly_week(latest_week),
                ly_next=self.get_ly_week(latest_week) + 1
            )
            
            projection_cols = ['store_number', 'f5c_item_number', 'projected_weekly_sales', 
                               'baseline_weekly_sales', 'style_growth_rate', 'blended_size_mix_pct',
                               'internal_style_color', 'style_projected_weekly_sales',
                               'style_ty_13wk_avg', 'style_ly_13wk_avg']  # keep style context where present

            # Some cols may not exist on projection_result in all datasets; guard
            projection_cols = [c for c in projection_cols if c in projection_result.columns]

            result = result.merge(
                projection_result[projection_cols],
                on=['store_number', 'f5c_item_number'],
                how='left'
            )

            result['growth_rate'] = result.get('style_growth_rate', 0)

            # Create ly_13wk_avg for compatibility with downstream code
            ly_item_avg = ly_df.groupby(['store_number', 'f5c_item_number']).agg({
                'pos_quantity_this_year': 'mean'
            }).reset_index()
            ly_item_avg.columns = ['store_number', 'f5c_item_number', 'ly_13wk_avg']
            result = result.merge(ly_item_avg, on=['store_number', 'f5c_item_number'], how='left')
            result['ly_13wk_avg'] = result['ly_13wk_avg'].fillna(0)
        else:
            logging.info("\nUsing ITEM-LEVEL projection (traditional method)...")
            ly_item_avg = ly_df.groupby(['store_number', 'f5c_item_number']).agg({
                'pos_quantity_this_year': 'mean'
            }).reset_index()
            ly_item_avg.columns = ['store_number', 'f5c_item_number', 'ly_13wk_avg']
            result = result.merge(ly_item_avg, on=['store_number', 'f5c_item_number'], how='left')
            result['ly_13wk_avg'] = result['ly_13wk_avg'].fillna(0)

            result['growth_rate'] = np.where(
                result['ly_13wk_avg'] > 0,
                (result['ty_13wk_avg'] - result['ly_13wk_avg']) / result['ly_13wk_avg'],
                0
            )
            
            ly_current = self.get_ly_week(latest_week)
            ly_next = ly_current + 1
            
            ly_projection_query = f"""
            SELECT
                store_number,
                f5c_item_number,
                f5c_calendar_week,
                pos_quantity_this_year as ly_actual
            FROM pos_all
            WHERE subcategory_number IN ({subcategory_filter})
            AND f5c_calendar_week IN ({ly_current}, {ly_next})
            """
            conn = self.connect_db()
            ly_projection = pd.read_sql(ly_projection_query, conn)
            conn.close()
            
            if len(ly_projection) > 0:
                ly_pivot = ly_projection.pivot_table(
                    index=['store_number', 'f5c_item_number'],
                    columns='f5c_calendar_week',
                    values='ly_actual',
                    fill_value=0
                ).reset_index()
                
                col_mapping = {ly_current: 'ly_current_week', ly_next: 'ly_next_week'}
                ly_pivot = ly_pivot.rename(columns=col_mapping)
                
                if 'ly_current_week' not in ly_pivot.columns:
                    ly_pivot['ly_current_week'] = 0
                if 'ly_next_week' not in ly_pivot.columns:
                    ly_pivot['ly_next_week'] = 0
                
                result = result.merge(ly_pivot, on=['store_number', 'f5c_item_number'], how='left')
                result['ly_current_week'] = result['ly_current_week'].fillna(0)
                result['ly_next_week'] = result['ly_next_week'].fillna(0)
            else:
                logging.warning("No LY projection data found, using TY averages")
                result['ly_current_week'] = result['ty_13wk_avg']
                result['ly_next_week'] = result['ty_13wk_avg']
            
            result['tw_projected'] = result['ly_current_week'] * (1 + result['growth_rate'])
            result['nw_projected'] = result['ly_next_week'] * (1 + result['growth_rate'])
            result['projected_weekly_sales'] = (result['tw_projected'] + result['nw_projected']) / 2
            result['projected_weekly_sales'] = result['projected_weekly_sales'].apply(lambda x: max(0.01, x))
            result['baseline_weekly_sales'] = result['ty_13wk_avg']

        # ==============================================================
        # STEP: CLIENT FORECAST INTEGRATION (any category with use_client_forecast=True)
        # ==============================================================
        if use_client_forecast:
            logging.info(f"\nIntegrating client forecast (blend={client_forecast_blend_weight}) for {category_name}...")
            conn = self.connect_db()
            client_forecast_df = self.projection_engine.calculate_client_forecast_projection(conn, subcategory_filter, weeks=client_forecast_weeks)
            conn.close()

            if len(client_forecast_df) > 0:
                result = result.merge(
                    client_forecast_df[['store_number', 'f5c_item_number', 'avg_forecast_each']],
                    on=['store_number', 'f5c_item_number'],
                    how='left'
                )

                alpha = float(client_forecast_blend_weight)
                has_client_forecast = result['avg_forecast_each'].notna()

                result['engine_projected_weekly_sales'] = result['projected_weekly_sales']

                result.loc[has_client_forecast, 'projected_weekly_sales'] = (
                    (1 - alpha) * result.loc[has_client_forecast, 'engine_projected_weekly_sales'] +
                    alpha * result.loc[has_client_forecast, 'avg_forecast_each']
                ).clip(lower=0.01)

                result['projection_source'] = np.where(
                    has_client_forecast,
                    np.where(alpha >= 0.999, 'CLIENT_FORECAST', 'Blend'),
                    'Engine'
                )

                logging.info(
                    f"Client forecast used on {has_client_forecast.sum():,} rows "
                    f"({(has_client_forecast.mean()*100):.1f}% of store/items). "
                    f"Avg alpha={alpha:.2f}"
                )
            else:
                logging.warning("No client forecast rows returned for these subcategories; using Engine projections only")

        # Common pipeline fields
        result['total_pipeline'] = result['on_hand_ty'] + result['in_warehouse_ty'] + result['in_transit_ty'] + result['on_order_ty']
        result['total_pipeline_ly'] = result['on_hand_ly'] + result['in_warehouse_ly'] + result['in_transit_ly'] + result['on_order_ly']
        
        result['current_wos'] = np.where(
            result['projected_weekly_sales'] > 0,
            result['on_hand_ty'] / result['projected_weekly_sales'],
            999
        )

        result['pipeline_wos'] = np.where(
            result['projected_weekly_sales'] > 0,
            result['total_pipeline'] / result['projected_weekly_sales'],
            999
        )
        
        result['ly_wos'] = np.where(
            result['ly_13wk_avg'] > 0,
            result['on_hand_ly'] / result['ly_13wk_avg'],
            999
        )

        # ==============================================================
        # PUSH STRATEGY SELECTION
        # ==============================================================
        if push_strategy == 'style_rop' and use_style_projection:
            logging.info("Using style-level target inventory with size coverage for Category A")

            # Ensure style id on result
            if 'internal_style_color' not in result.columns:
                result['internal_style_color'] = result.get('vendor_stock_id', np.nan)
            result['internal_style_color'] = result['internal_style_color'].fillna(result['vendor_stock_id'])

            # Style-level projected weekly sales from size rows (post client forecast blend)
            style_proj = (
                result.groupby(['store_number', 'internal_style_color'], as_index=False)
                      .agg(style_projected_weekly_sales_from_sizes=('projected_weekly_sales', 'sum'))
            )

            # Style target inventory = style weekly * target WOS
            style_proj['style_target_inventory'] = (
                style_proj['style_projected_weekly_sales_from_sizes'] * float(style_target_wos)
            ).clip(lower=0)

            # Style current pipeline = sum of size pipelines
            style_pipe = (
                result.groupby(['store_number', 'internal_style_color'], as_index=False)
                      .agg(style_total_pipeline=('total_pipeline', 'sum'))
            )

            style_target = style_proj.merge(style_pipe, on=['store_number', 'internal_style_color'], how='left')
            style_target['style_total_pipeline'] = style_target['style_total_pipeline'].fillna(0)
            style_target['style_gap'] = (style_target['style_target_inventory'] - style_target['style_total_pipeline']).clip(lower=0)

            # Merge style target/gap back to size rows
            result = result.merge(
                style_target[['store_number','internal_style_color','style_target_inventory','style_total_pipeline','style_gap']],
                on=['store_number','internal_style_color'],
                how='left'
            )

            # --- Transit deduction (use lead_time_weeks from config; default 1.0) ---
            L = float(locals().get('lead_time_weeks', 1.0))

            # Already have: style_target['style_projected_weekly_sales_from_sizes']
            style_target['mu_weekly'] = style_target['style_projected_weekly_sales_from_sizes'].clip(lower=0.01)

            # Deduct expected sales during transit from style pipeline
            style_target['style_total_pipeline_eff'] = np.maximum(
                0,
                style_target['style_total_pipeline'] - style_target['mu_weekly'] * L
            )

            # Recompute style gap against target using effective pipeline
            style_target['style_raw_gap'] = (
                style_target['style_target_inventory'] - style_target['style_total_pipeline_eff']
            ).clip(lower=0)


            # ---------- ROP (gate) + Throttle  ----------
            L, P = 1.0, 1.0                   # 1 week transit, weekly review
            LP = L + P                         # = 2 weeks
            z = float(locals().get('z_service', 1.0))       # tunable service level
            ramp_w = float(locals().get('ramp_weeks', 0.75))# ramp softness (weeks)

            # μ (weekly demand) per store-style, σ (stdev) from last 13 TY weeks (fallback sqrt(μ))
            style_target['mu_weekly'] = style_target['style_projected_weekly_sales_from_sizes'].clip(lower=0.01)

            weekly_style_ty = (
                df.groupby(['store_number','internal_style_color','f5c_calendar_week'], as_index=False)
                .agg(week_sales=('pos_quantity_this_year','sum'))
            )
            sigma = weekly_style_ty.groupby(['store_number','internal_style_color'], as_index=False)\
                                .agg(sigma_weekly=('week_sales','std'))
            style_target = style_target.merge(sigma, on=['store_number','internal_style_color'], how='left')
            style_target['sigma_weekly'] = style_target['sigma_weekly'].fillna(0)
            need_fallback = style_target['sigma_weekly'] <= 0
            style_target.loc[need_fallback, 'sigma_weekly'] = np.sqrt(style_target.loc[need_fallback, 'mu_weekly'])

            # Hard ROP point (S)
            style_target['rop_S'] = (
                style_target['mu_weekly'] * LP + z * style_target['sigma_weekly'] * np.sqrt(LP)
            ).round(2)

            # Soft throttle around S (a "dimmer switch")
            style_target['ramp_units'] = (style_target['mu_weekly'] * ramp_w).round(4)
            style_target['gate'] = (
                (style_target['rop_S'] + style_target['ramp_units'] - style_target['style_total_pipeline_eff']) / style_target['ramp_units']
            )
            style_target['gate'] = style_target['gate'].clip(lower=0, upper=1).fillna(0)

            # Style raw gap to 10 WOS (before throttle)
            style_target['style_raw_gap'] = (style_target['style_target_inventory'] - style_target['style_total_pipeline']).clip(lower=0)

            # Throttled style cap
            use_rop_gate = bool(locals().get('use_rop_gate', True))
            style_target['style_cap_after_gate'] = np.where(
                use_rop_gate,
                (style_target['style_raw_gap'] * style_target['gate']).round(2),
                style_target['style_raw_gap']
            )

            # Use throttled cap as the authoritative style cap
            style_target['style_gap'] = style_target['style_cap_after_gate']

            # Size targets: max(size_min_wos * size_weekly, size share of style)
            result['size_target_from_style'] = result['projected_weekly_sales'] * float(style_target_wos)
            result['size_target_from_min'] = result['projected_weekly_sales'] * float(size_min_wos)
            result['size_target_inventory'] = np.maximum(result['size_target_from_style'], result['size_target_from_min'])

            # Size-level gap with hysteresis
            result['shortfall_weeks'] = np.where(
                result['projected_weekly_sales'] > 0,
                (result['size_target_inventory'] - result['total_pipeline']) / result['projected_weekly_sales'],
                0
            )
            # Size-level effective pipeline (deduct L weeks of size demand)
            result['mu_size'] = result['projected_weekly_sales'].clip(lower=0.01)
            result['total_pipeline_eff'] = np.maximum(0, result['total_pipeline'] - result['mu_size'] * L)

            # Size targets (unchanged)
            result['size_target_from_style'] = result['projected_weekly_sales'] * float(style_target_wos)
            result['size_target_from_min']   = result['projected_weekly_sales'] * float(size_min_wos)
            result['size_target_inventory']  = np.maximum(result['size_target_from_style'], result['size_target_from_min'])

            # Use effective pipeline in gaps
            raw_push = (result['size_target_inventory'] - result['total_pipeline_eff']).clip(lower=0)


            # ------- Capacity clamp (units that fit per shelf module per item) -------
            front_capacity_per_item = int(locals().get('front_capacity_per_item', 4))
            backroom_buffer_units = int(locals().get('backroom_buffer_units', 6))

            # If you have item-level shelf capacity in data (max_shelf_quantity_this_year), prefer that:
            if 'max_shelf_quantity_this_year' in result.columns:
                size_front_cap = result['max_shelf_quantity_this_year'].fillna(front_capacity_per_item).clip(lower=front_capacity_per_item)
            else:
                size_front_cap = pd.Series(front_capacity_per_item, index=result.index)

            size_max_after_push = size_front_cap + backroom_buffer_units
            size_cap_gap = (size_max_after_push - result['total_pipeline']).clip(lower=0)

            # Apply capacity clamp, then hysteresis/min-push
            candidate_push = np.minimum(raw_push, size_cap_gap)

            result['shortfall_weeks'] = np.where(
                result['projected_weekly_sales'] > 0,
                candidate_push / result['projected_weekly_sales'],
                0
            )
            result['push_units'] = np.where(
                result['shortfall_weeks'] >= float(hysteresis_weeks),
                candidate_push,
                0
            )

            # Cap sum of size pushes to style_gap
            # Cap sum of size pushes to the throttled style_gap (after ROP gate)
            def cap_by_style(group):
                total_push = group['push_units'].sum()
                cap_col = 'style_gap' if 'style_gap' in group else (
                    'style_gap_after_gate' if 'style_gap_after_gate' in group else None
                )
                if cap_col:
                    cap = group[cap_col].iloc[0]
                else:
                    cap = total_push

                if cap <= 0 or total_push <= 0:
                    return group

                scale = min(1.0, cap / total_push)
                group['push_units'] = np.floor(group['push_units'] * scale)
                return group
            # QA: where do we lose units?
            result['raw_size_push'] = (result['size_target_inventory'] - result['total_pipeline']).clip(lower=0)
            result['after_cap_candidate'] = np.minimum(result['raw_size_push'],
                                                    (size_max_after_push - result['total_pipeline']).clip(lower=0))

            style_view = (
                result.groupby(['store_number','internal_style_color'], as_index=False)
                    .agg(raw_sum=('raw_size_push','sum'),
                        after_cap_sum=('after_cap_candidate','sum'),
                        style_cap=('style_gap','first'))
            )
            logging.info(
                "[QA] Style-level sums — raw=%s, after_cap=%s, cap=%s",
                f"{style_view['raw_sum'].sum():,.0f}",
                f"{style_view['after_cap_sum'].sum():,.0f}",
                f"{style_view['style_cap'].sum():,.0f}"
            )


            result = (
                result.groupby(['store_number','internal_style_color'], group_keys=False)
                      .apply(cap_by_style)
                      .reset_index(drop=True)
            )

            # Floors and projected WOS after push
            result['push_units'] = np.where(result['push_units'] < float(min_push_units), 0, result['push_units'])
            result['projected_wos_after_push'] = np.where(
                result['projected_weekly_sales'] > 0,
                (result['total_pipeline'] + result['push_units']) / result['projected_weekly_sales'],
                999
            )

            # Visibility
            result['style_target_wos'] = float(style_target_wos)
            result['size_min_wos'] = float(size_min_wos)

        else:
            # Default WOS push (wallets, rain, etc.)
            result['wos_gap'] = target_wos - result['current_wos']
            result['push_units'] = np.where(
                result['wos_gap'] > 0,
                result['wos_gap'] * result['projected_weekly_sales'],
                0
            )
            # Don't push if over max WOS
            result['push_units'] = np.where(result['current_wos'] > self.max_wos, 0, result['push_units'])

            # Target inventory for reporting
            result['target_inventory'] = result['projected_weekly_sales'] * target_wos

            # Use target - pipeline form (same as your later override)
            result['push_units'] = np.maximum(0, result['target_inventory'] - result['total_pipeline'])

            # Hysteresis & small-push filter
            result['shortfall_weeks'] = np.where(
                result['projected_weekly_sales'] > 0,
                (result['target_inventory'] - result['total_pipeline']) / result['projected_weekly_sales'],
                0
            )
            result['push_units'] = np.where(result['shortfall_weeks'] >= float(hysteresis_weeks), result['push_units'], 0)
            result['push_units'] = np.where(result['push_units'] < float(min_push_units), 0, result['push_units'])

            result['projected_wos_after_push'] = np.where(
                result['projected_weekly_sales'] > 0,
                (result['total_pipeline'] + result['push_units']) / result['projected_weekly_sales'],
                999
            )

        # Grading & summaries (unchanged)
        total_baseline = result['baseline_weekly_sales'].sum() if 'baseline_weekly_sales' in result.columns else 0
        if total_baseline > 0:
            result['pct_of_total_sales'] = (result['baseline_weekly_sales'] / total_baseline * 100)
            result = result.sort_values('baseline_weekly_sales', ascending=False, ignore_index=True)
            result['cumulative_pct'] = result['pct_of_total_sales'].cumsum()
            def assign_grade(cum_pct):
                if cum_pct <= 20:   return 'A'
                elif cum_pct <= 50: return 'B'
                elif cum_pct <= 95: return 'C'
                else:               return 'D'
            result['store_item_grade'] = result['cumulative_pct'].apply(assign_grade)
        else:
            result['pct_of_total_sales'] = 0
            result['cumulative_pct'] = 0
            result['store_item_grade'] = 'D'
        
        def get_reason(row):
            if row['current_wos'] > self.max_wos:
                return 'Overstocked (>15 WOS)'
            elif row['push_units'] == 0 and row['current_wos'] >= target_wos:
                return 'Adequate inventory'
            elif row['push_units'] == 0:
                return 'No push needed'
            elif 'growth_rate' in row and row['growth_rate'] > 0.15:
                return 'Strong growth trend'
            else:
                return 'Standard replenishment'
        
        result['push_reason'] = result.apply(get_reason, axis=1)
        
        # Final rounding/typing
        if 'push_units' in result.columns:
            result['push_units'] = result['push_units'].round(0).astype(int)
        for c in ['current_wos','ly_wos','projected_wos_after_push','pct_of_total_sales']:
            if c in result.columns:
                result[c] = pd.to_numeric(result[c], errors='coerce')
        if 'pct_of_total_sales' in result.columns:
            result['pct_of_total_sales'] = result['pct_of_total_sales'].round(2)
        if 'growth_rate' in result.columns:
            result['growth_rate'] = result['growth_rate'].round(3)

        # ------------------------------------------
        # NEW: convert push_units → CASE_PACK (optional)
        # ------------------------------------------
        if convert_to_case_pack:
            if 'case_pack_qty' not in result.columns:
                # Failsafe: if somehow missing, default to 1
                result['case_pack_qty'] = 1
            result['case_pack_qty'] = pd.to_numeric(result['case_pack_qty'], errors='coerce').fillna(1)
            result['case_pack_qty'] = result['case_pack_qty'].replace(0, 1)

            result['push_case_pack'] = result['push_units'] / result['case_pack_qty']
            result['push_case_pack'] = result['push_case_pack'].replace([np.inf, -np.inf], np.nan).fillna(0)
            result['push_case_pack'] = result['push_case_pack'].round(0).astype(int)

        # --------------------------------------------------------------
        # STORE-LEVEL CLUSTERS (Top 20 / Next 30 / Next 30 / Bottom 20)
        # --------------------------------------------------------------
        # Always tag rows with store_cluster; only write CSV if requested
        result, store_cluster = self.add_store_clusters(
            result,
            output_file_prefix=output_file if export_store_clusters else None
        )

        # Build output columns
        output_cols = [
            'region', 'state',"zip_code", "merch_zone_number",
            'store_number', 'f5c_item_number', 'subcategory_description', 'vendor_stock_id'
        ]
        if source == 'category_a':
            output_cols.extend(['color_description', 'trait_description'])
        
        # Common columns + new visibility fields
        extra_cols = [
            'wtd_pos', 'lw_pos', 'pos_m1', 'pos_m2', 'pos_m3', 'pos_m4',
            'ty_13wk_pos_total', 'ty_52wk_pos_total', 
            'ty_13wk_avg', 'ly_13wk_avg', 'growth_rate',
            'ly_current_week', 'ly_next_week',
            'tw_projected', 'nw_projected',
            'baseline_weekly_sales', 'projected_weekly_sales',
            'on_hand_ty', 'in_warehouse_ty', 'in_transit_ty', 'on_order_ty',
            'store_on_hand_quantity_this_year',
            'on_hand_ly', 'in_warehouse_ly', 'in_transit_ly', 'on_order_ly',
            'store_on_hand_quantity_last_year',
            'total_pipeline', 'total_pipeline_ly',
            'current_wos', 'pipeline_wos', 'ly_wos',
            'target_inventory', 'push_units',
            'projected_wos_after_push',
            'pos_sales_this_year', 'pos_sales_last_year',
            'store_item_grade', 'pct_of_total_sales', 'cumulative_pct', 'push_reason',
            # Client forecast visibility
            'projection_source', 'engine_projected_weekly_sales', 'avg_forecast_each',
            # Category A style-ROP visibility (some may be NaN for non-Category A or non-style_rop)
            'style_target_wos', 'size_min_wos', 'size_target_inventory', 'shortfall_weeks',
            'max_shelf_quantity_this_year', 'ship_qty', 'sell_through_pct', 'sell_through_flag',
            # CASE_PACK
            'case_pack_qty', 'push_case_pack',
            # clusters
            'store_cluster', 'store_pct_of_total_sales', 'store_cumulative_pct',
        ]

        output_cols.extend([c for c in extra_cols if c in result.columns])

        for col in output_cols:
            if col not in result.columns:
                result[col] = np.nan
        
        result = result[output_cols]

        summary = result.groupby('region', dropna=False).agg({
            'push_units': 'sum',
            'current_wos': 'mean',
            'ly_wos': 'mean',
            'projected_wos_after_push': 'mean',
            'store_number': 'nunique',
            'total_pipeline': 'sum',
            'total_pipeline_ly': 'sum'
        }).reset_index()
        
        summary.columns = ['region', 'total_push_units', 
                           'avg_current_wos', 'avg_ly_wos', 'avg_projected_wos', 
                           'store_count', 
                           'total_pipeline_ty', 'total_pipeline_ly']
        summary = summary.round(2)
        
        summary['pipeline_change'] = summary['total_pipeline_ty'] - summary['total_pipeline_ly']
        summary['pipeline_change_pct'] = np.where(
            summary['total_pipeline_ly'] > 0,
            (summary['pipeline_change'] / summary['total_pipeline_ly'] * 100),
            0
        ).round(1)

        logging.info(f"\n{category_name} SUMMARY:")
        logging.info(f"  Total Push Units: {result['push_units'].sum():,}")
        logging.info(f"  Average Current WOS: {result['current_wos'].mean():.1f} weeks")
        logging.info(f"  Average LY WOS: {result['ly_wos'].mean():.1f} weeks")
        logging.info(f"  Average Projected WOS: {result['projected_wos_after_push'].mean():.1f} weeks")
        logging.info(f"  Stores affected: {result['store_number'].nunique():,}")
        logging.info(f"  Total TY Pipeline: {result['total_pipeline'].sum():,.0f}")
        logging.info(f"  Total LY Pipeline: {result['total_pipeline_ly'].sum():,.0f}")
        
        logging.info("\nBY REGION:")
        for _, row in summary.iterrows():
            logging.info(f"  {row['region']}: {row['total_push_units']:,} units "
                         f"({row['avg_current_wos']:.1f} -> {row['avg_projected_wos']:.1f} WOS, "
                         f"Pipeline: {row['pipeline_change']:+,.0f} [{row['pipeline_change_pct']:+.1f}%])")
        
        if output_file:
            result.to_csv(f'{output_file}_detail.csv', index=False)
            summary.to_csv(f'{output_file}_summary.csv', index=False)
            logging.info(f"\n[OK] Results saved to {output_file}_detail.csv and {output_file}_summary.csv")
        
        return result, summary

    
    
    def add_store_clusters(self, result: pd.DataFrame, output_file_prefix: str = None):

        if 'baseline_weekly_sales' not in result.columns:
            logging.warning("baseline_weekly_sales missing; skipping store clustering.")
            return result, pd.DataFrame()

        # 1. CREATE STORE-LEVEL TABLE (ONE ROW PER STORE)
        store_totals = (
            result.groupby(['region','state','store_number'], as_index=False)
            .agg(
                store_baseline=('baseline_weekly_sales','sum'),
                store_projected=('projected_weekly_sales','sum'),
                store_push=('push_units','sum'),
                store_current_wos=('current_wos','mean')
            )
        )

        # 2. GET TOTAL SALES ACROSS ALL STORES
        total_sales = store_totals['store_baseline'].sum()
        if total_sales == 0:
            return result, store_totals

        # 3. STORE SHARE OF SALES
        store_totals['pct_of_total_sales'] = (store_totals['store_baseline'] / total_sales) * 100

        # 4. SORT DESCENDING BY SALES (STORE-LEVEL)
        store_totals = store_totals.sort_values('store_baseline', ascending=False).reset_index(drop=True)

        # 5. CUMULATIVE %
        store_totals['cumulative_pct'] = store_totals['pct_of_total_sales'].cumsum()

        # 6. CLUSTER ASSIGNMENT (STORE-LEVEL)
        def assign_cluster(c):
            if c <= 20:
                return 'Top 20%'
            elif c <= 50:
                return 'Next 30%'
            elif c <= 80:
                return 'Next 30%'
            else:
                return 'Bottom 20%'

        store_totals['store_cluster'] = store_totals['cumulative_pct'].apply(assign_cluster)

        # 7. MERGE STORE CLUSTER BACK TO ROW-LEVEL DETAIL
        result = result.merge(
            store_totals[['region','state','store_number','store_cluster','pct_of_total_sales','cumulative_pct']],
            on=['region','state','store_number'],
            how='left'
        )

        # 8. OPTIONAL: EXPORT STORE-LEVEL CLUSTER FILE
        if output_file_prefix:
            store_totals.to_csv(f'{output_file_prefix}_store_clusters.csv', index=False)

        return result, store_totals




    # (Rain logic unchanged — your existing method below)
    def calculate_weather_push(self, category_subcategories, category_name, output_file=None, target_week=None, source='non-category_a',
                            export_store_clusters=False,
                            apply_shelf_fill=False,
                            shelf_fill_region=None):
        """
        Calculate WOS-based push recommendations using LY actual sales x weather projection (6-week window)
        """
        logging.info("="*60)
        logging.info(f"CALCULATING WOS PUSH FOR: {category_name.upper()}")
        logging.info("Using LY actual sales x weather projection (6-week window)")
        logging.info("="*60)
        
        conn = self.connect_db()
        subcategory_filter = ','.join([f"'{fl}'" for fl in category_subcategories])
        
        if target_week is None:
            latest_week = self.get_latest_week()
        else:
            latest_week = target_week
        
        ly_current = self.get_ly_week(latest_week)
        ly_minus_3 = ly_current - 3
        ly_plus_2 = ly_current + 2
        
        logging.info(f"Current TY week: {latest_week}")
        logging.info(f"Current LY week: {ly_current}")
        logging.info(f"LY window: {ly_minus_3} to {ly_plus_2}")
        
        current_inventory_query = f"""
        SELECT
            p.store_number,
            p.f5c_item_number,
            p.subcategory_number,
            p.subcategory_description,
            p.vendor_stock_id,
            p.color_description,
            p.trait_description,
            p.pos_quantity_this_year as wtd_pos,
            p.store_on_hand_quantity_this_year_eop as on_hand_ty,
            p.store_on_hand_quantity_last_year_eop as on_hand_ly,
            p.store_on_hand_quantity_this_year,
            p.store_on_hand_quantity_last_year,
            p.store_in_warehouse_quantity_this_year as in_warehouse_ty,
            p.store_in_warehouse_quantity_last_year as in_warehouse_ly,
            p.store_in_transit_quantity_this_year as in_transit_ty,
            p.store_in_transit_quantity_last_year as in_transit_ly,
            p.store_on_order_quantity_this_year as on_order_ty,
            p.store_on_order_quantity_last_year as on_order_ly,
            p.pos_sales_this_year,
            p.pos_sales_last_year,
            p.max_shelf_quantity_this_year,
            s.region,
            s.state,
            w.case_pack_qty
        FROM pos_all p
        LEFT JOIN dim_stores s ON p.store_number = s.store_number
        LEFT JOIN dim_case_pack w ON p.f5c_item_number = w.item_nbr
        WHERE p.subcategory_number IN ({subcategory_filter})
        AND p.f5c_calendar_week = {latest_week}
        """
        
        lw_query = f"""
        SELECT
            store_number,
            f5c_item_number,
            pos_quantity_this_year as lw_pos
        FROM pos_all
        WHERE subcategory_number IN ({subcategory_filter})
        AND f5c_calendar_week = {latest_week - 1}
        """
        
        ly_pos_query = f"""
        SELECT
            store_number,
            f5c_item_number,
            f5c_calendar_week,
            pos_quantity_this_year as ly_pos
        FROM pos_all
        WHERE subcategory_number IN ({subcategory_filter})
        AND f5c_calendar_week BETWEEN {ly_minus_3} AND {ly_plus_2}
        """
        
        forecast_vendor_query = """
        SELECT LOWER(region) as region, tw_pct, nw_pct
        FROM fact_forecast_vendor
        WHERE week_imported = (SELECT MAX(week_imported) FROM fact_forecast_vendor)
        """
        
        logging.info("Loading data...")
        current_inv = pd.read_sql(current_inventory_query, conn)
        lw_pos = pd.read_sql(lw_query, conn)
        ly_pos = pd.read_sql(ly_pos_query, conn)
        forecast_vendor = pd.read_sql(forecast_vendor_query, conn)
        conn.close()
        
        if len(current_inv) == 0:
            logging.error(f"No current week data found for {category_name}")
            return pd.DataFrame(), pd.DataFrame()
        
        logging.info(f"Loaded {len(current_inv):,} store/item combinations")
        
        ly_pos_pivot = ly_pos.pivot_table(
            index=['store_number', 'f5c_item_number'],
            columns='f5c_calendar_week',
            values='ly_pos',
            fill_value=0
        ).reset_index()
        
        week_cols = {}
        for week in range(ly_minus_3, ly_plus_2 + 1):
            if week in ly_pos_pivot.columns:
                offset = week - ly_current
                week_cols[week] = f'ly_pos_week_{offset:+d}'
        ly_pos_pivot = ly_pos_pivot.rename(columns=week_cols)
        
        result = current_inv.copy()
        result['region'] = result['region'].str.lower()
        
        result = result.merge(lw_pos, on=['store_number', 'f5c_item_number'], how='left')
        result = result.merge(ly_pos_pivot, on=['store_number', 'f5c_item_number'], how='left')
        result = result.merge(forecast_vendor, on='region', how='left')
        
        result = result.infer_objects(copy=False)
        numeric_cols = ['wtd_pos', 'lw_pos', 
                       'on_hand_ty', 'on_hand_ly',
                       'store_on_hand_quantity_this_year', 'store_on_hand_quantity_last_year',
                       'in_warehouse_ty', 'in_warehouse_ly',
                       'in_transit_ty', 'in_transit_ly',
                       'on_order_ty', 'on_order_ly',
                       'pos_sales_this_year', 'pos_sales_last_year', 
                       'case_pack_qty', 'tw_pct', 'nw_pct']
        for i in range(-3, 3):
            numeric_cols.append(f'ly_pos_week_{i:+d}')
        for col in numeric_cols:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors='coerce').fillna(0)
        
        result['case_pack_qty'] = result['case_pack_qty'].replace(0, 1)
        result['tw_pct'] = result['tw_pct'].apply(lambda x: max(self.weather_floor, x))
        result['nw_pct'] = result['nw_pct'].apply(lambda x: max(self.weather_floor, x))
        
        result['ly_current_week'] = result.get('ly_pos_week_+0', pd.Series([0]*len(result)))
        result['ly_next_week'] = result.get('ly_pos_week_+1', pd.Series([0]*len(result)))
        result['ly_current_week'] = pd.to_numeric(result['ly_current_week'], errors='coerce').fillna(0)
        result['ly_next_week'] = pd.to_numeric(result['ly_next_week'], errors='coerce').fillna(0)
        
        result['tw_projected'] = result['ly_current_week'] * (1 + result['tw_pct'])
        result['nw_projected'] = result['ly_next_week'] * (1 + result['nw_pct'])
        result['projected_weekly_sales'] = (result['tw_projected'] + result['nw_projected']) / 2
        result['projected_weekly_sales'] = result['projected_weekly_sales'].apply(lambda x: max(0.01, x))
        
        ly_week_cols = [f'ly_pos_week_{i:+d}' for i in range(-3, 3) if f'ly_pos_week_{i:+d}' in result.columns]
        if ly_week_cols:
            result['baseline_weekly_sales'] = result[ly_week_cols].mean(axis=1)
        else:
            result['baseline_weekly_sales'] = result['ly_current_week']
        
        result['weather_adjustment'] = (result['tw_pct'] + result['nw_pct']) / 2
        
        result['total_pipeline'] = result['on_hand_ty'] + result['in_warehouse_ty'] + result['in_transit_ty'] + result['on_order_ty']
        result['total_pipeline_ly'] = result['on_hand_ly'] + result['in_warehouse_ly'] + result['in_transit_ly'] + result['on_order_ly']
        
        result['current_wos'] = np.where(
            result['projected_weekly_sales'] > 0,
            result['total_pipeline'] / result['projected_weekly_sales'],
            999
        )
        
        result['ly_wos'] = np.where(
            result['baseline_weekly_sales'] > 0,
            result['total_pipeline_ly'] / result['baseline_weekly_sales'],
            999
        )
        
        result['wos_gap'] = self.target_wos - result['current_wos']
        result['push_units'] = np.where(
            result['wos_gap'] > 0,
            result['wos_gap'] * result['projected_weekly_sales'],
            0
        )
        result['push_units'] = np.where(result['current_wos'] > self.max_wos, 0, result['push_units'])
        result['target_inventory'] = result['projected_weekly_sales'] * self.target_wos
        result['push_units'] = np.maximum(0, result['target_inventory'] - result['total_pipeline'])
        
        # ------------------------------------------------------------------
        # NEW: optional shelf-fill rule (only for chosen region)
        # ------------------------------------------------------------------
        if apply_shelf_fill and 'max_shelf_quantity_this_year' in result.columns:
            shelf_cap = pd.to_numeric(
                result['max_shelf_quantity_this_year'],
                errors='coerce'
            ).fillna(0)

            # current pipeline after "normal" push
            pipeline_after_push = result['total_pipeline'] + result['push_units']

            # base condition: shelf capacity higher than pipeline+push
            needs_shelf_fill = shelf_cap > pipeline_after_push

            # region filter: if shelf_fill_region is set, only apply there
            if shelf_fill_region:
                # region in rain is already lowercased earlier
                needs_shelf_fill = needs_shelf_fill & (
                    result['region'] == shelf_fill_region.lower()
                )

            # units needed to reach shelf capacity = shelf_cap - current pipeline
            add_units = (shelf_cap - result['total_pipeline']).clip(lower=0)

            # overwrite push_units only for rows that qualify
            result.loc[needs_shelf_fill, 'push_units'] = add_units[needs_shelf_fill]


        # now convert to CASE_PACK and recompute WOS after push
        result['push_case_pack'] = result['push_units'] / result['case_pack_qty']
        
        result['projected_wos_after_push'] = np.where(
            result['projected_weekly_sales'] > 0,
            (result['total_pipeline'] + result['push_units']) / result['projected_weekly_sales'],
            999
        )

        result['push_case_pack'] = result['push_units'] / result['case_pack_qty']
        
        result['projected_wos_after_push'] = np.where(
            result['projected_weekly_sales'] > 0,
            (result['total_pipeline'] + result['push_units']) / result['projected_weekly_sales'],
            999
        )
        
        total_baseline = result['baseline_weekly_sales'].sum()
        if total_baseline > 0:
            result['pct_of_total_sales'] = (result['baseline_weekly_sales'] / total_baseline * 100)
            result = result.sort_values('baseline_weekly_sales', ascending=False, ignore_index=True)
            result['cumulative_pct'] = result['pct_of_total_sales'].cumsum()
            def assign_grade(cum_pct):
                if cum_pct <= 50:   return 'A'
                elif cum_pct <= 80: return 'B'
                elif cum_pct <= 95: return 'C'
                else:               return 'D'
            result['store_item_grade'] = result['cumulative_pct'].apply(assign_grade)
        else:
            result['pct_of_total_sales'] = 0
            result['cumulative_pct'] = 0
            result['store_item_grade'] = 'D'
        
        def get_reason(row):
            if row['current_wos'] > self.max_wos:
                return 'Overstocked (>15 WOS)'
            elif row['push_units'] == 0 and row['current_wos'] >= self.target_wos:
                return 'Adequate inventory'
            elif row['push_units'] == 0:
                return 'No push needed'
            elif row['current_wos'] < 2:
                return 'Critical - Low inventory'
            elif row['weather_adjustment'] > 0.15:
                return 'High demand forecast'
            else:
                return 'Standard replenishment'
        
        result['push_reason'] = result.apply(get_reason, axis=1)
        
        result['push_units'] = result['push_units'].round(0).astype(int)
        result['push_case_pack'] = result['push_case_pack'].round(0).astype(int)
        result['current_wos'] = result['current_wos'].round(1)
        result['ly_wos'] = result['ly_wos'].round(1)
        result['projected_wos_after_push'] = result['projected_wos_after_push'].round(1)
        result['pct_of_total_sales'] = result['pct_of_total_sales'].round(2)
        result['tw_pct'] = result['tw_pct'].round(3)
        result['nw_pct'] = result['nw_pct'].round(3)
        result['weather_adjustment'] = result['weather_adjustment'].round(3)

        # --------------------------------------------------------------
        # STORE-LEVEL CLUSTERS (Top 20 / Next 30 / Next 30 / Bottom 20)
        # --------------------------------------------------------------
        result, store_cluster = self.add_store_clusters(
            result,
            output_file_prefix=output_file if export_store_clusters else None
        )
        
        output_cols = [
            'region', 'state', 'store_number', 'f5c_item_number', 'subcategory_description',
        ]
        if source == 'category_a':
            output_cols.extend(['vendor_stock_id', 'color_description', 'trait_description'])
        
        output_cols.extend([
            'wtd_pos', 'lw_pos',
            'ly_pos_week_-3', 'ly_pos_week_-2', 'ly_pos_week_-1', 
            'ly_pos_week_+0', 'ly_pos_week_+1', 'ly_pos_week_+2',
            'baseline_weekly_sales', 'tw_pct', 'nw_pct', 'weather_adjustment',
            'tw_projected', 'nw_projected', 'projected_weekly_sales',
            'on_hand_ty', 'in_warehouse_ty', 'in_transit_ty', 'on_order_ty',
            'store_on_hand_quantity_this_year',
            'on_hand_ly', 'in_warehouse_ly', 'in_transit_ly', 'on_order_ly',
            'store_on_hand_quantity_last_year',
            'total_pipeline', 'total_pipeline_ly',
            'current_wos', 'ly_wos', 'target_inventory', 'push_units', 'push_case_pack',
            'projected_wos_after_push',
            'pos_sales_this_year', 'pos_sales_last_year',
            'store_item_grade', 'pct_of_total_sales', 'cumulative_pct', 'push_reason', 'case_pack_qty', 'max_shelf_quantity_this_year',
            'ship_qty', 'sell_through_pct', 'sell_through_flag',
            'store_cluster', 'store_pct_of_total_sales', 'store_cumulative_pct',
        ])
        
        for col in output_cols:
            if col not in result.columns:
                result[col] = np.nan
        
        result = result[output_cols]
        
        summary = result.groupby('region').agg({
            'push_units': 'sum',
            'push_case_pack': 'sum',
            'current_wos': 'mean',
            'ly_wos': 'mean',
            'projected_wos_after_push': 'mean',
            'store_number': 'nunique',
            'weather_adjustment': 'mean',
            'total_pipeline': 'sum',
            'total_pipeline_ly': 'sum'
        }).reset_index()
        
        summary.columns = ['region', 'total_push_units', 'total_push_case_pack',
                           'avg_current_wos', 'avg_ly_wos', 'avg_projected_wos', 
                           'store_count', 'avg_weather_adj',
                           'total_pipeline_ty', 'total_pipeline_ly']
        summary = summary.round(2)
        
        summary['pipeline_change'] = summary['total_pipeline_ty'] - summary['total_pipeline_ly']
        summary['pipeline_change_pct'] = np.where(
            summary['total_pipeline_ly'] > 0,
            (summary['pipeline_change'] / summary['total_pipeline_ly'] * 100),
            0
        ).round(1)
        
        logging.info(f"\n{category_name} SUMMARY:")
        logging.info(f"  Total Push Units: {result['push_units'].sum():,}")
        logging.info(f"  Total Push CASE_PACK: {result['push_case_pack'].sum():,}")
        logging.info(f"  Average Current WOS: {result['current_wos'].mean():.1f} weeks")
        logging.info(f"  Average LY WOS: {result['ly_wos'].mean():.1f} weeks")
        logging.info(f"  Average Projected WOS: {result['projected_wos_after_push'].mean():.1f} weeks")
        logging.info(f"  Average Weather Adjustment: {result['weather_adjustment'].mean():.1%}")
        logging.info(f"  Stores affected: {result['store_number'].nunique():,}")
        logging.info(f"  Total TY Pipeline: {result['total_pipeline'].sum():,.0f}")
        logging.info(f"  Total LY Pipeline: {result['total_pipeline_ly'].sum():,.0f}")
        
        logging.info("\nBY REGION:")
        for _, row in summary.iterrows():
            logging.info(f"  {row['region']}: {row['total_push_case_pack']:,} CASE_PACK "
                         f"({row['avg_current_wos']:.1f} -> {row['avg_projected_wos']:.1f} WOS, "
                         f"{row['avg_weather_adj']:.1%} weather adj, "
                         f"Pipeline: {row['pipeline_change']:+,.0f} [{row['pipeline_change_pct']:+.1f}%])")
        
        if 'store_item_grade' in result.columns:
            logging.info("\nSTORE/ITEM GRADE DISTRIBUTION:")
            grade_summary = result.groupby('store_item_grade').agg({
                'push_units': 'sum',
                'push_case_pack': 'sum',
                'store_number': 'count'
            }).reset_index()
            for _, row in grade_summary.iterrows():
                logging.info(f"  Grade {row['store_item_grade']}: {row['push_case_pack']:,} CASE_PACK "
                             f"({row['store_number']:,} store/items)")
        
        if output_file:
            result.to_csv(f'{output_file}_detail.csv', index=False)
            summary.to_csv(f'{output_file}_summary.csv', index=False)
            logging.info(f"\n[OK] Results saved to {output_file}_detail.csv and {output_file}_summary.csv")
        
        return result, summary

# =============================================================================
# MAIN MENU
# =============================================================================
def display_menu():
    print("\n" + "="*60)
    print("WOS PUSH CALCULATOR - MAIN MENU")
    print("="*60)
    
    menu_items = []
    for key, config in CATEGORY_CONFIGS.items():
        menu_items.append((key, config['name']))
    
    for idx, (key, name) in enumerate(menu_items, 1):
        print(f"{idx}. Calculate WOS for {name}")
    
    print(f"{len(menu_items) + 1}. Calculate WOS for All Categories")
    print(f"{len(menu_items) + 2}. Exit")
    print("="*60)
    
    return menu_items

def run_category(calculator, category_key):
    """Run WOS calculation for a single category"""
    config = CATEGORY_CONFIGS[category_key]
    
    print(f"\n[Running {config['name']} WOS Calculation...]")

    # NEW: ask once per category run
    export_choice = input("Export store cluster summary file for this run? (y/N): ").strip().lower()
    export_store_clusters = (export_choice == 'y')

    # Optional shelf-fill only relevant for weather (rain) runs
    apply_shelf_fill = False
    shelf_fill_region = None
    if config['method'] == 'weather':
        sf_choice = input("Apply shelf-capacity top-up for a specific region? (y/N): ").strip().lower()
        if sf_choice == 'y':
            shelf_fill_region = input("Enter region code (e.g. se, sw, ne): ").strip()
            if shelf_fill_region:
                apply_shelf_fill = True

    if config['method'] == 'trend':
        detail, summary = calculator.calculate_trend_push(
            category_subcategories=config['subcategories'],
            category_name=config['name'],
            output_file=config['output_file'],
            source=config['source'],
            use_style_projection=config.get('use_style_projection',False),
            use_client_forecast=config.get('use_client_forecast', False),
            client_forecast_blend_weight=config.get('client_forecast_blend_weight', 1.0),
            client_forecast_weeks=config.get('client_forecast_weeks', 4),
            push_strategy=config.get('push_strategy', 'wos'),
            style_target_wos=config.get('style_target_wos', 10),
            size_min_wos=config.get('size_min_wos', 2),
            target_wos=config.get('target_wos', calculator.target_wos),
            hysteresis_weeks=config.get('hysteresis_weeks', 0.5),
            min_push_units=config.get('min_push_units', 1),
            export_store_clusters=export_store_clusters,
            convert_to_case_pack=config.get('convert_to_case_pack', False)   # ← NEW
        )
    elif config['method'] == 'weather':
        detail, summary = calculator.calculate_weather_push(
            category_subcategories=config['subcategories'],
            category_name=config['name'],
            output_file=config['output_file'],
            source=config['source'],
            export_store_clusters=export_store_clusters,
            apply_shelf_fill=apply_shelf_fill,
            shelf_fill_region=shelf_fill_region
        )
    
    if config['method'] == 'trend':
        detail, summary = calculator.calculate_trend_push(
            category_subcategories=config['subcategories'],
            category_name=config['name'],
            output_file=config['output_file'],
            source=config['source'],
            use_style_projection=config.get('use_style_projection',False),
            use_client_forecast=config.get('use_client_forecast', False),
            client_forecast_blend_weight=config.get('client_forecast_blend_weight', 1.0),
            client_forecast_weeks=config.get('client_forecast_weeks', 4),
            push_strategy=config.get('push_strategy', 'wos'),
            style_target_wos=config.get('style_target_wos', 10),
            size_min_wos=config.get('size_min_wos', 2),
            target_wos=config.get('target_wos', calculator.target_wos),
            hysteresis_weeks=config.get('hysteresis_weeks', 0.5),
            min_push_units=config.get('min_push_units', 1),
            export_store_clusters=export_store_clusters
        )
    elif config['method'] == 'weather':
        detail, summary = calculator.calculate_weather_push(
            category_subcategories=config['subcategories'],
            category_name=config['name'],
            output_file=config['output_file'],
            source=config['source'],
            export_store_clusters=export_store_clusters
        )
    else:
        print(f"[ERROR] Unknown method: {config['method']}")
        return None, None
    
    print(f"\n[OK] {config['name']} calculation complete!")
    return detail, summary

if __name__ == "__main__":
    calculator = WOSPushCalculator('f5c_data.db')
    calculator.create_unified_view()
    
    while True:
        menu_items = display_menu()
        max_option = len(menu_items) + 2
        
        choice = input(f"\nEnter your choice (1-{max_option}): ").strip()
        
        try:
            choice_num = int(choice)
        except ValueError:
            print("\n[ERROR] Please enter a valid number.")
            continue
        
        if 1 <= choice_num <= len(menu_items):
            category_key = menu_items[choice_num - 1][0]
            run_category(calculator, category_key)
            
        elif choice_num == len(menu_items) + 1:
            print("\n[Running All Categories...]")
            for category_key in CATEGORY_CONFIGS.keys():
                print(f"\n{'='*60}")
                run_category(calculator, category_key)
            print("\n[OK] All categories complete!")
            
        elif choice_num == len(menu_items) + 2:
            print("\nExiting... Goodbye!")
            break
            
        else:
            print(f"\n[ERROR] Invalid choice. Please enter 1-{max_option}.")
        
        input("\nPress Enter to continue...")

    print("\n" + "="*60)
    print("[OK] WOS CALCULATIONS COMPLETE")
    print("="*60)
