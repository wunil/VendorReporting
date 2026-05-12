import sqlite3
import pandas as pd
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# =============================================================================
# CATEGORY CONFIGURATIONS
# =============================================================================
CATEGORY_CONFIGS = {
    'category_a': {
        'name': 'Category A',
        'subcategories': [1001, 1002],
        'table': 'POS_category_a_fact',
        'has_style_table': True,
        'style_table': 'dim_category_a_style',
        'detailed_columns': ['trait_description', 'vendor_stock_id', 'color_description'],
        'output_file': 'category_a_weekly_detailed.csv'
    },
    'category_b': {
        'name': 'Category B',
        'subcategories': [2001],
        'table': 'POS_fact',
        'has_style_table': True,
        'style_table': 'dim_category_a_style',  # Same style table, filtered by category
        'detailed_columns': [],  # No extra columns
        'output_file': 'category_b_weekly_detailed.csv'
    },
    'category_c': {
        'name': 'Category C',
        'subcategories': [3001],
        'table': 'POS_fact',
        'has_style_table': True,
        'style_table': 'dim_category_a_style',
        'detailed_columns': [],
        'output_file': 'category_c_weekly_detailed.csv'
    },
    'category_d': {
        'name': 'Category D',
        'subcategories': [4001, 4002, 4003, 4004],
        'table': 'POS_fact',
        'has_style_table': False,
        'style_table': None,
        'detailed_columns': [],
        'output_file': 'category_d_weekly_detailed.csv'
    },
}

class WeeklyAnalysis:
    def __init__(self, db_path='f5c_data.db'):
        self.db_path = db_path
    
    def connect_db(self):
        """Create database connection"""
        return sqlite3.connect(self.db_path)
    
    def assign_season(self, week):
        """Assign season based on week number"""
        if pd.isna(week):
            return None
        
        week_str = str(week)
        
        # Extract the week number portion (last 2 digits)
        if len(week_str) >= 6:
            week_num = int(week_str[-2:])
            year = int(week_str[:4])
            
            # Weeks 01-26 = Spring, Weeks 27-52/53 = Fall
            if 1 <= week_num <= 26:
                return f"Spring {year}"
            elif week_num >= 27:
                return f"Fall {year}"
        
        return "Unknown"
    
    def get_weekly_detailed(self, category_key, output_file=None):
        """
        Extract weekly data for any category with Python calculations
        """
        config = CATEGORY_CONFIGS[category_key]
        category_name = config['name']
        
        if output_file is None:
            output_file = config['output_file']
        
        logging.info(f"Extracting detailed weekly data for {category_name}...")
        
        conn = self.connect_db()
        subcategory_filter = ','.join([str(fl) for fl in config['subcategories']])
        
        # Define seasons
        seasons = [
            ('Spring 2024', 202401, 202426),
            ('Fall 2024', 202427, 202453),
            ('Spring 2025', 202501, 202526),
            ('Fall 2025', 202527, 202553),
        ]
        
        all_data = []
        
        for season_name, start_week, end_week in seasons:
            logging.info(f"Processing {season_name}...")
            
            # Build dynamic query based on category configuration
            query = self._build_query(config, subcategory_filter, start_week, end_week)
            
            df_season = pd.read_sql_query(query, conn)
            logging.info(f"  Retrieved {len(df_season):,} rows for {season_name}")
            all_data.append(df_season)
        
        conn.close()
        
        # Combine all seasons
        logging.info("Combining all seasons...")
        df = pd.concat(all_data, ignore_index=True)
        logging.info(f"Total rows: {len(df):,}")
        
        # Convert numeric columns
        numeric_cols = [
            'ty_store_ct', 'ly_store_ct',
            'pos_qty_ty', 'pos_qty_ly',
            'sales_ty', 'sales_ly',
            'oh_qty_ty', 'oh_qty_ly',
            'in_transit_ty', 'in_transit_ly',
            'in_warehouse_ty', 'in_warehouse_ly',
            'on_order_ty', 'on_order_ly'
        ]
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Add season column
        logging.info("Adding season classification...")
        df['season'] = df['f5c_calendar_week'].apply(self.assign_season)
        
        # Determine grouping columns based on what's available
        grouping_cols = self._get_grouping_columns(df, config)
        
        # Sort for calculations
        df = df.sort_values(grouping_cols + ['f5c_calendar_week'])
        
        logging.info("Calculating metrics...")
        
        # Weekly metrics
        df['weekly_productivity_ty'] = (df['pos_qty_ty'] / df['ty_store_ct']).fillna(0).round(2)
        df['weekly_productivity_ly'] = (df['pos_qty_ly'] / df['ly_store_ct']).fillna(0).round(2)
        df['avg_oh_per_store_ty'] = (df['oh_qty_ty'] / df['ty_store_ct']).fillna(0).round(2)
        df['avg_oh_per_store_ly'] = (df['oh_qty_ly'] / df['ly_store_ct']).fillna(0).round(2)
        df['weeks_of_supply_ty'] = (df['oh_qty_ty'] / df['pos_qty_ty']).fillna(0).replace([float('inf')], 0).round(2)
        
        # Season-to-date cumulative (using grouping columns + season)
        season_group_cols = grouping_cols + ['season']
        
        df['season_pos_qty_cumulative_ty'] = df.groupby(season_group_cols)['pos_qty_ty'].cumsum()
        df['season_sales_cumulative_ty'] = df.groupby(season_group_cols)['sales_ty'].cumsum()
        df['weeks_in_season_so_far'] = df.groupby(season_group_cols).cumcount() + 1
        
        # Season averages
        df['avg_weekly_pos_qty_std_ty'] = (df['season_pos_qty_cumulative_ty'] / df['weeks_in_season_so_far']).round(2)
        df['avg_weekly_sales_std_ty'] = (df['season_sales_cumulative_ty'] / df['weeks_in_season_so_far']).round(2)
        
        # Season productivity
        df['season_store_weeks_cumulative_ty'] = df.groupby(season_group_cols)['ty_store_ct'].cumsum()
        df['season_productivity_std_ty'] = (
            df['season_pos_qty_cumulative_ty'] / df['season_store_weeks_cumulative_ty']
        ).fillna(0).round(2)
        
        # Export
        df.to_csv(output_file, index=False)
        logging.info(f"[OK] Exported {len(df):,} rows to {output_file}")
        
        # Summary
        self._print_summary(df, category_name)
        
        return df
    
    def _build_query(self, config, subcategory_filter, start_week, end_week):
        """Build SQL query dynamically based on category configuration"""
        
        table = config['table']
        has_style = config['has_style_table']
        style_table = config['style_table']
        detailed_cols = config['detailed_columns']
        
        # Base columns that all categories have
        base_select = """
            f.f5c_calendar_week,
            f.f5c_item_number,
            f.subcategory_description,
            
            SUM(f.traited_store_count_this_year) as ty_store_ct,
            SUM(f.traited_store_count_last_year) as ly_store_ct,
            
            SUM(f.pos_quantity_this_year) as pos_qty_ty,
            SUM(f.pos_quantity_last_year) as pos_qty_ly,
            SUM(CAST(REPLACE(f.pos_sales_this_year, '$', '') AS REAL)) as sales_ty,
            SUM(CAST(REPLACE(f.pos_sales_last_year, '$', '') AS REAL)) as sales_ly,
            SUM(f.store_on_hand_quantity_this_year_eop) as oh_qty_ty,
            SUM(f.store_on_hand_quantity_last_year_eop) as oh_qty_ly,
            SUM(f.store_in_transit_quantity_this_year) as in_transit_ty,
            SUM(f.store_in_transit_quantity_last_year) as in_transit_ly,
            SUM(f.store_in_warehouse_quantity_this_year) as in_warehouse_ty,
            SUM(f.store_in_warehouse_quantity_last_year) as in_warehouse_ly,
            SUM(f.store_on_order_quantity_this_year) as on_order_ty,
            SUM(f.store_on_order_quantity_last_year) as on_order_ly
        """
        
        # Add detailed columns if this category has them
        detailed_select = ""
        if detailed_cols:
            detailed_select = ",\n            " + ",\n            ".join([f"f.{col}" for col in detailed_cols])
        
        # Add style columns if this category has style mapping
        style_select = ""
        style_join = ""
        if has_style:
            style_select = """
            ,d.internal_style_color,
            d.product_category"""
            
            # If detailed columns don't include size but style table has it, add it
            if 'size' not in detailed_cols:
                style_select += ",\n            d.size"
            
            style_join = f"""
            INNER JOIN {style_table} d 
                ON f.f5c_item_number = d.customer_style_number"""
        
        # Build GROUP BY clause
        group_by_cols = ["f.f5c_calendar_week", "f.f5c_item_number", "f.subcategory_description"]
        
        if detailed_cols:
            group_by_cols.extend([f"f.{col}" for col in detailed_cols])
        
        if has_style:
            group_by_cols.extend(["d.internal_style_color", "d.product_category"])
            if 'size' not in detailed_cols:
                group_by_cols.append("d.size")
        
        group_by = ",\n                    ".join(group_by_cols)
        
        # Build final query
        query = f"""
            WITH item_totals AS (
                SELECT 
                    {base_select}{detailed_select}{style_select}
                    
                FROM {table} f
                {style_join}
                WHERE f.subcategory_number IN ({subcategory_filter})
                AND f.f5c_calendar_week >= {start_week}
                AND f.f5c_calendar_week <= {end_week}
                GROUP BY 
                    {group_by}
            )
            SELECT * FROM item_totals
        """
        
        return query
    
    def _get_grouping_columns(self, df, config):
        """Determine grouping columns based on what's available in the dataframe"""
        grouping_cols = []
        
        # Always have these if they exist
        if 'internal_style_color' in df.columns:
            grouping_cols.append('internal_style_color')
        
        if 'f5c_item_number' in df.columns:
            grouping_cols.append('f5c_item_number')
        
        # Add detailed columns if they exist
        for col in config['detailed_columns']:
            if col in df.columns:
                grouping_cols.append(col)
        
        # Add size if it exists (from style table)
        if 'size' in df.columns and 'size' not in grouping_cols:
            grouping_cols.append('size')
        
        # Fallback: if no grouping columns found, use item number only
        if not grouping_cols:
            grouping_cols = ['f5c_item_number']
        
        return grouping_cols
    
    def _print_summary(self, df, category_name):
        """Print summary statistics"""
        logging.info("\n" + "="*60)
        logging.info(f"{category_name.upper()} SUMMARY STATISTICS")
        logging.info("="*60)
        
        logging.info(f"\nTotal rows: {len(df):,}")
        
        logging.info(f"\nWeek range: {df['f5c_calendar_week'].min()} to {df['f5c_calendar_week'].max()}")
        logging.info(f"Number of unique weeks: {df['f5c_calendar_week'].nunique()}")
        
        logging.info(f"\nSeason breakdown:")
        season_counts = df['season'].value_counts()
        for season, count in season_counts.items():
            logging.info(f"  {season}: {count:,} rows")
        
        # Product category breakdown (if exists)
        if 'product_category' in df.columns:
            logging.info(f"\nProduct Category breakdown:")
            category_counts = df['product_category'].value_counts()
            for category, count in category_counts.items():
                logging.info(f"  {category}: {count:,} rows")
        
        # Store count ranges
        logging.info(f"\nStore count ranges:")
        logging.info(f"  Min TY store count: {df['ty_store_ct'].min():,}")
        logging.info(f"  Max TY store count: {df['ty_store_ct'].max():,}")
        logging.info(f"  Avg TY store count: {df['ty_store_ct'].mean():,.0f}")
        
        # Sales check
        logging.info(f"\nSales check:")
        logging.info(f"  Total sales TY: ${df['sales_ty'].sum():,.2f}")
        logging.info(f"  Total sales LY: ${df['sales_ly'].sum():,.2f}")
        logging.info(f"  Rows with sales_ty > 0: {(df['sales_ty'] > 0).sum():,}")
        
        # Productivity metrics
        logging.info(f"\nProductivity metrics:")
        logging.info(f"  Avg weekly productivity TY: {df['weekly_productivity_ty'].mean():,.2f}")
        logging.info(f"  Max weekly productivity TY: {df['weekly_productivity_ty'].max():,.2f}")
        
        # Inventory metrics
        logging.info(f"\nInventory metrics:")
        logging.info(f"  Avg OH per store: {df['avg_oh_per_store_ty'].mean():,.2f}")
        logging.info(f"  Avg weeks of supply: {df['weeks_of_supply_ty'].mean():.2f}")
        
        # Show top 5 items by total sales
        if 'internal_style_color' in df.columns:
            logging.info(f"\nTop 5 styles by total sales TY:")
            top_items = df.groupby('internal_style_color')['sales_ty'].sum().sort_values(ascending=False).head(5)
        else:
            logging.info(f"\nTop 5 items by total sales TY:")
            top_items = df.groupby('f5c_item_number')['sales_ty'].sum().sort_values(ascending=False).head(5)
        
        for item, sales in top_items.items():
            logging.info(f"  {item}: ${sales:,.2f}")
        
        logging.info("\n" + "="*60)

# =============================================================================
# MAIN MENU
# =============================================================================
def display_menu():
    print("\n" + "="*60)
    print("WEEKLY ANALYSIS - MAIN MENU")
    print("="*60)
    
    menu_items = []
    for key, config in CATEGORY_CONFIGS.items():
        menu_items.append((key, config['name']))
    
    for idx, (key, name) in enumerate(menu_items, 1):
        print(f"{idx}. Run analysis for {name}")
    
    print(f"{len(menu_items) + 1}. Run analysis for All Categories")
    print(f"{len(menu_items) + 2}. Exit")
    print("="*60)
    
    return menu_items

def run_category(analyzer, category_key):
    """Run analysis for a single category"""
    config = CATEGORY_CONFIGS[category_key]
    
    print(f"\n[Running {config['name']} Weekly Analysis...]")
    
    df = analyzer.get_weekly_detailed(
        category_key=category_key,
        output_file=config['output_file']
    )
    
    print(f"\n[OK] {config['name']} analysis complete!")
    return df

# =============================================================================
# USAGE
# =============================================================================
if __name__ == "__main__":
    analyzer = WeeklyAnalysis('f5c_data.db')
    
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
            run_category(analyzer, category_key)
            
        elif choice_num == len(menu_items) + 1:
            print("\n[Running All Categories...]")
            
            for category_key in CATEGORY_CONFIGS.keys():
                print(f"\n{'='*60}")
                run_category(analyzer, category_key)
            
            print("\n[OK] All categories complete!")
            
        elif choice_num == len(menu_items) + 2:
            print("\nExiting... Goodbye!")
            break
            
        else:
            print(f"\n[ERROR] Invalid choice. Please enter 1-{max_option}.")
        
        input("\nPress Enter to continue...")
    
    print("\n" + "="*60)
    print("[OK] WEEKLY ANALYSIS COMPLETE")
    print("="*60)
