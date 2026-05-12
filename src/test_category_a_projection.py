import sqlite3
import pandas as pd
import numpy as np
import logging
from projection_engine import ProjectionEngine

# Setup detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('category_a_projection_test.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class CategoryAProjectionTester:
    def __init__(self, db_path='f5c_data.db'):
        self.db_path = db_path
        self.projection_engine = ProjectionEngine()
    
    def connect_db(self):
        return sqlite3.connect(self.db_path)
    
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
            logger.warning(f"No mapping found for TY week {ty_week}, using default (TY - 100)")
            return ty_week - 100
    
    def get_latest_week(self):
        """Get the most recent week in the database"""
        conn = self.connect_db()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(f5c_calendar_week) FROM pos_all")
        latest_week = cursor.fetchone()[0]
        conn.close()
        return latest_week
    
    def test_store_793(self):
        """
        Test projection logic for store 001
        Expected: 344 units over 26 weeks = ~13.2 units/week
        """
        
        logger.info("="*80)
        logger.info("TESTING CATEGORY A PROJECTION FOR STORE 001")
        logger.info("Target: 344 units over 26 weeks = 13.2 units/week")
        logger.info("="*80)
        
        conn = self.connect_db()
        
        # Category A subcategories
        category_a_subcategories = [1001, 1002]
        subcategory_filter = ','.join([f"'{fl}'" for fl in category_a_subcategories])
        
        # Get latest week
        latest_week = self.get_latest_week()
        logger.info(f"\nLatest week in database: {latest_week}")
        
        # First, let's verify the 26-week number
        verify_start = latest_week - 25  # 26 weeks including current
        
        try:
            verify_query = f"""
            SELECT SUM(p.pos_quantity_this_year) as total_pos_26wk
            FROM pos_all p
            WHERE p.subcategory_number IN ({subcategory_filter})
            AND p.store_number = 1
            AND p.f5c_calendar_week >= {verify_start}
            AND p.f5c_calendar_week <= {latest_week}
            """
            
            verify_df = pd.read_sql(verify_query, conn)
            actual_26wk = verify_df['total_pos_26wk'].iloc[0] if len(verify_df) > 0 else 0
            logger.info(f"\nVERIFICATION: Last 26 weeks POS for store 001: {actual_26wk:,.0f} units")
            logger.info(f"  Weekly average: {actual_26wk / 26:,.2f} units/week")
        except Exception as e:
            logger.error(f"Error in verification query: {e}")
            actual_26wk = 0
        
        # Get 13 weeks of TY data for store 001
        ty_start_week = latest_week - 12
        logger.info(f"\nTY 13-week period: {ty_start_week} to {latest_week}")
        
        # Get corresponding LY weeks
        ly_latest = self.get_ly_week(latest_week)
        ly_start = self.get_ly_week(ty_start_week)
        logger.info(f"LY 13-week period: {ly_start} to {ly_latest}")
        
        # =====================================================================
        # QUERY 1: Get 13 weeks of TY data for store 001 WITH STYLE JOIN
        # =====================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 1: Loading TY 13-week data for store 001 (with style join)")
        logger.info("="*80)
        
        try:
            ty_query = f"""
            SELECT
                p.f5c_calendar_week,
                p.store_number,
                p.f5c_item_number,
                p.vendor_stock_id,
                p.color_description,
                bs.internal_style_color,
                p.subcategory_number,
                p.subcategory_description,
                p.pos_quantity_this_year,
                p.store_on_hand_quantity_this_year_eop as on_hand_ty,
                p.store_in_warehouse_quantity_this_year as in_warehouse_ty,
                p.store_in_transit_quantity_this_year as in_transit_ty,
                p.store_on_order_quantity_this_year as on_order_ty
            FROM pos_all p
            LEFT JOIN dim_category_a_style bs ON p.f5c_item_number = bs.customer_style_number
            WHERE p.subcategory_number IN ({subcategory_filter})
            AND p.store_number = 1
            AND p.f5c_calendar_week >= {ty_start_week}
            AND p.f5c_calendar_week <= {latest_week}
            ORDER BY bs.internal_style_color, p.f5c_item_number, p.f5c_calendar_week
            """
            
            logger.debug("Executing TY query...")
            df_ty = pd.read_sql(ty_query, conn)
            logger.info(f"\nLoaded {len(df_ty):,} TY records for store 001")
            
        except Exception as e:
            logger.error(f"Error loading TY data: {e}")
            logger.error(f"Query was:\n{ty_query}")
            conn.close()
            return None
        
        if len(df_ty) == 0:
            logger.error("No TY data found for store 001!")
            conn.close()
            return None
        
        # Check for missing style mappings
        missing_styles = df_ty['internal_style_color'].isna().sum()
        if missing_styles > 0:
            logger.warning(f"\n⚠️  {missing_styles} records missing internal_style_color mapping!")
            logger.warning("Items without style mapping:")
            no_style = df_ty[df_ty['internal_style_color'].isna()]['f5c_item_number'].unique()
            for item in no_style[:10]:
                logger.warning(f"  Item: {item}")
            # Fill missing with vendor_stock_id as fallback
            df_ty['internal_style_color'] = df_ty['internal_style_color'].fillna(df_ty['vendor_stock_id'])
        
        # Show summary by style
        logger.info("\nTY Data Summary by Style (internal_style_color):")
        style_summary_ty = df_ty.groupby('internal_style_color').agg({
            'pos_quantity_this_year': 'sum',
            'f5c_item_number': 'nunique',
            'vendor_stock_id': 'first'
        }).reset_index()
        style_summary_ty.columns = ['internal_style_color', 'total_pos_ty_13wk', 'num_sizes', 'vendor_stock_id']
        style_summary_ty = style_summary_ty.sort_values('total_pos_ty_13wk', ascending=False)
        
        for _, row in style_summary_ty.head(10).iterrows():
            logger.info(f"  Style {row['internal_style_color']} (VSI: {row['vendor_stock_id']}): "
                       f"{row['total_pos_ty_13wk']:.0f} units across {row['num_sizes']} sizes")
        
        total_ty_13wk = df_ty['pos_quantity_this_year'].sum()
        logger.info(f"\nTotal TY 13-week POS for store 001: {total_ty_13wk:,.0f} units")
        logger.info(f"Weekly average: {total_ty_13wk / 13:,.2f} units/week")
        
        # =====================================================================
        # QUERY 2: Get 13 weeks of LY data for store 001 WITH STYLE JOIN
        # =====================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 2: Loading LY 13-week data for store 001 (with style join)")
        logger.info("="*80)
        
        try:
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
            AND p.store_number = 1
            AND p.f5c_calendar_week >= {ly_start}
            AND p.f5c_calendar_week <= {ly_latest}
            ORDER BY bs.internal_style_color, p.f5c_item_number, p.f5c_calendar_week
            """
            
            logger.debug("Executing LY query...")
            df_ly = pd.read_sql(ly_query, conn)
            logger.info(f"\nLoaded {len(df_ly):,} LY records for store 001")
            
        except Exception as e:
            logger.error(f"Error loading LY data: {e}")
            logger.error(f"Query was:\n{ly_query}")
            df_ly = pd.DataFrame()
        
        if len(df_ly) == 0:
            logger.warning("No LY data found for store 001!")
            df_ly = pd.DataFrame()
        else:
            # Fill missing styles
            missing_styles_ly = df_ly['internal_style_color'].isna().sum()
            if missing_styles_ly > 0:
                logger.warning(f"\n⚠️  {missing_styles_ly} LY records missing internal_style_color mapping!")
                df_ly['internal_style_color'] = df_ly['internal_style_color'].fillna(df_ly['vendor_stock_id'])
            
            # Show summary by style
            logger.info("\nLY Data Summary by Style (internal_style_color):")
            style_summary_ly = df_ly.groupby('internal_style_color').agg({
                'pos_quantity_this_year': 'sum',
                'f5c_item_number': 'nunique',
                'vendor_stock_id': 'first'
            }).reset_index()
            style_summary_ly.columns = ['internal_style_color', 'total_pos_ly_13wk', 'num_sizes', 'vendor_stock_id']
            style_summary_ly = style_summary_ly.sort_values('total_pos_ly_13wk', ascending=False)
            
            for _, row in style_summary_ly.head(10).iterrows():
                logger.info(f"  Style {row['internal_style_color']} (VSI: {row['vendor_stock_id']}): "
                           f"{row['total_pos_ly_13wk']:.0f} units across {row['num_sizes']} sizes")
            
            total_ly_13wk = df_ly['pos_quantity_this_year'].sum()
            logger.info(f"\nTotal LY 13-week POS for store 001: {total_ly_13wk:,.0f} units")
            logger.info(f"Weekly average: {total_ly_13wk / 13:,.2f} units/week")
        
        # =====================================================================
        # QUERY 3: Get current week inventory for store 001 WITH STYLE JOIN
        # =====================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 3: Loading current week inventory for store 001 (with style join)")
        logger.info("="*80)
        
        try:
            current_query = f"""
            SELECT
                p.store_number,
                p.f5c_item_number,
                p.vendor_stock_id,
                p.color_description,
                bs.internal_style_color,
                p.subcategory_number,
                p.subcategory_description,
                p.pos_quantity_this_year as wtd_pos,
                p.store_on_hand_quantity_this_year_eop as on_hand_ty,
                p.store_in_warehouse_quantity_this_year as in_warehouse_ty,
                p.store_in_transit_quantity_this_year as in_transit_ty,
                p.store_on_order_quantity_this_year as on_order_ty,
                p.store_on_hand_quantity_last_year_eop as on_hand_ly,
                p.store_in_warehouse_quantity_last_year as in_warehouse_ly,
                p.store_in_transit_quantity_last_year as in_transit_ly,
                p.store_on_order_quantity_last_year as on_order_ly
            FROM pos_all p
            LEFT JOIN dim_category_a_style bs ON p.f5c_item_number = bs.customer_style_number
            WHERE p.subcategory_number IN ({subcategory_filter})
            AND p.store_number = 1
            AND p.f5c_calendar_week = {latest_week}
            ORDER BY bs.internal_style_color, p.f5c_item_number
            """
            
            logger.debug("Executing current week query...")
            df_current = pd.read_sql(current_query, conn)
            conn.close()
            
            logger.info(f"\nLoaded {len(df_current):,} current week records for store 001")
            
        except Exception as e:
            logger.error(f"Error loading current week data: {e}")
            logger.error(f"Query was:\n{current_query}")
            conn.close()
            return None
        
        # Fill missing styles
        missing_styles_current = df_current['internal_style_color'].isna().sum()
        if missing_styles_current > 0:
            logger.warning(f"\n⚠️  {missing_styles_current} current week records missing internal_style_color mapping!")
            df_current['internal_style_color'] = df_current['internal_style_color'].fillna(df_current['vendor_stock_id'])
        
        # Convert to numeric
        numeric_cols = ['on_hand_ty', 'in_warehouse_ty', 'in_transit_ty', 'on_order_ty',
                       'on_hand_ly', 'in_warehouse_ly', 'in_transit_ly', 'on_order_ly', 'wtd_pos']
        for col in numeric_cols:
            if col in df_current.columns:
                df_current[col] = pd.to_numeric(df_current[col], errors='coerce').fillna(0)
        
        # Show inventory summary
        logger.info("\nCurrent Inventory Summary:")
        logger.info(f"  On Hand: {df_current['on_hand_ty'].sum():,.0f}")
        logger.info(f"  In Warehouse: {df_current['in_warehouse_ty'].sum():,.0f}")
        logger.info(f"  In Transit: {df_current['in_transit_ty'].sum():,.0f}")
        logger.info(f"  On Order: {df_current['on_order_ty'].sum():,.0f}")
        total_pipeline = (df_current['on_hand_ty'] + df_current['in_warehouse_ty'] + 
                         df_current['in_transit_ty'] + df_current['on_order_ty']).sum()
        logger.info(f"  Total Pipeline: {total_pipeline:,.0f}")
        
        # Show inventory by style
        logger.info("\nInventory by Style:")
        style_inv = df_current.groupby('internal_style_color').agg({
            'on_hand_ty': 'sum',
            'in_warehouse_ty': 'sum',
            'in_transit_ty': 'sum',
            'on_order_ty': 'sum',
            'f5c_item_number': 'count'
        }).reset_index()
        style_inv['total_pipeline'] = (style_inv['on_hand_ty'] + style_inv['in_warehouse_ty'] + 
                                       style_inv['in_transit_ty'] + style_inv['on_order_ty'])
        style_inv = style_inv.sort_values('total_pipeline', ascending=False)
        
        for _, row in style_inv.head(10).iterrows():
            logger.info(f"  Style {row['internal_style_color']}: {row['total_pipeline']:.0f} units "
                       f"({row['f5c_item_number']} sizes)")
        
        # =====================================================================
        # STEP 4: Run the style-based projection
        # =====================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 4: Running style-based projection algorithm")
        logger.info("="*80)
        
        try:
            result = self.projection_engine.calculate_style_projection(
                df=df_ty,
                ly_df=df_ly,
                current_week_df=df_current,
                style_column='internal_style_color',
                latest_week=latest_week,
                ly_current=self.get_ly_week(latest_week),
                ly_next=self.get_ly_week(latest_week) + 1
            )
            
            # =====================================================================
            # STEP 5: Analyze results
            # =====================================================================
            logger.info("\n" + "="*80)
            logger.info("STEP 5: RESULTS ANALYSIS")
            logger.info("="*80)
            
            total_projection = result['projected_weekly_sales'].sum()
            logger.info(f"\n TOTAL PROJECTED WEEKLY SALES FOR STORE 001: {total_projection:,.2f} units/week")
            logger.info(f"\n COMPARISON:")
            logger.info(f"  Expected (26-week avg):  13.23 units/week (344 total / 26 weeks)")
            logger.info(f"  TY 13-week actual:       {total_ty_13wk / 13:,.2f} units/week")
            logger.info(f"  Projected (our model):   {total_projection:,.2f} units/week")
            
            diff_from_expected = total_projection - 13.23
            pct_diff = (diff_from_expected / 13.23) * 100
            logger.info(f"\n  Difference from expected: {diff_from_expected:+.2f} units/week ({pct_diff:+.1f}%)")
            
            # Save detailed results
            result.to_csv('store_001_projection_debug.csv', index=False)
            logger.info("\n" + "="*80)
            logger.info("[OK] Detailed results saved to 'store_001_projection_debug.csv'")
            logger.info("="*80)
            
            return result
            
        except Exception as e:
            logger.error(f"\n[Error] ERROR during projection: {e}", exc_info=True)
            return None

if __name__ == "__main__":
    tester = CategoryAProjectionTester('f5c_data.db')
    result = tester.test_store_793()
    
    if result is not None:
        print("\n" + "="*80)
        print("[Ok] TEST COMPLETE - Check 'category_a_projection_test.log' for detailed output")
        print("="*80)
    else:
        print("\n" + "="*80)
        print("[Error] TEST FAILED - Check 'category_a_projection_test.log' for error details")
        print("="*80)
