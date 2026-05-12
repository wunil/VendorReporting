import sqlite3
import pandas as pd
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s - %(levelname)s - %(message)s'
)

class DimensionTableManager:
  def __init__(self, db_path='f5c_data.db'):
    self.db_path = db_path
    self.tables_folder = Path('./data/dimension_tables')
 
  def connect_db(self):
    return sqlite3.connect(self.db_path)
 
  def create_dimension_tables(self):
    """Create all dimension tables in the database"""
    logging.info("Creating dimension tables...")
   
    conn = self.connect_db()
    cursor = conn.cursor()
   
    # 1. Store dimension table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dim_stores (
      store_number TEXT PRIMARY KEY,
      state TEXT,
      region TEXT,
      zip_code TEXT,
      merch_zone_number INTEGER,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
   
    # 2. Item/Style information dimension
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dim_items (
      f5c_item_number INTEGER PRIMARY KEY,
      subcategory TEXT,
      description TEXT,
      style_id TEXT,
      cost REAL,
      retail REAL,
      item_status TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
   
    # 3. Capacity table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dim_capacity (
      item INTEGER,
      capacity INTEGER,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (item)
    )
    ''')
   
    # 4. CASE_PACK (Warehouse Pack) table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dim_case_pack (
      item_nbr TEXT PRIMARY KEY,
      status TEXT,
      vnpk_qty INTEGER,
      case_pack_qty INTEGER,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
   
    # 5. ForecastVendor weather projections
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fact_forecast_vendor (
      region TEXT,
      week_imported DATE,
      tw_pct REAL,
      nw_pct REAL,
      m1_pct REAL,
      m2_pct REAL,
      m3_pct REAL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (region, week_imported)
    )
    ''')

    # 6. Add Client Forecast
    cursor.execute('''
      CREATE TABLE IF NOT EXISTS CLIENT_FORECAST_fact (
      f5c_item_nbr TEXT,
      subcategory_nbr TEXT,
      f5c_calendar_week INTEGER,
      store_nbr TEXT,
      final_forecast_each_quantity INTEGER,
      vendor_stock_id TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (f5c_item_nbr, store_nbr, f5c_calendar_week)
    )             
    ''')
   
    # Create indexes for better query performance
    indexes = [
      'CREATE INDEX IF NOT EXISTS idx_stores_region ON dim_stores(region)',
      'CREATE INDEX IF NOT EXISTS idx_items_subcategory ON dim_items(subcategory)',
      'CREATE INDEX IF NOT EXISTS idx_forecast_vendor_region ON fact_forecast_vendor(region)',
      'CREATE INDEX IF NOT EXISTS idx_client_forecast_store ON CLIENT_FORECAST_fact(store_nbr)'

    ]
   
    for index in indexes:
      cursor.execute(index)
   
    conn.commit()
    conn.close()
    logging.info("[OK] Dimension tables created successfully")
 
  def import_store_table(self):
    """Import store mapping"""
    logging.info("Importing Store Table...")
   
    file_path = self.tables_folder / 'StoreTable.xlsx'
    df = pd.read_excel(file_path, dtype={'store_number': str})
   
    # Clean column names
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
   
    conn = self.connect_db()
   
    # Clear existing data
    cursor = conn.cursor()
    cursor.execute('DELETE FROM dim_stores')
   
    # Insert new data
    df.to_sql('dim_stores', conn, if_exists='append', index=False)
   
    count = len(df)
    conn.close()
    logging.info(f"[OK] Imported {count:,} stores")
 
  def import_style_information(self):
    """Import item/style information"""
    logging.info("Importing Style Information...")
   
    file_path = self.tables_folder / 'Style Information.xlsx'
    df = pd.read_excel(file_path, dtype={'f5c_item_number': str, 'Subcategory': str})
   
    # Clean column names to match database schema
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
   
    conn = self.connect_db()
   
    # Clear existing data
    cursor = conn.cursor()
    cursor.execute('DELETE FROM dim_items')
   
    # Insert new data
    df.to_sql('dim_items', conn, if_exists='append', index=False)
   
    count = len(df)
    conn.close()
    logging.info(f"[OK] Imported {count:,} items")
 
  def import_capacity(self):
    """Import capacity table"""
    logging.info("Importing Capacity...")
   
    file_path = self.tables_folder / 'Capacity.xlsx'
    df = pd.read_excel(file_path, dtype={'Item': str})
   
    # Clean column names
    df.columns = df.columns.str.strip().str.lower()
   
    conn = self.connect_db()
   
    # Clear existing data
    cursor = conn.cursor()
    cursor.execute('DELETE FROM dim_capacity')
   
    # Insert new data
    df.to_sql('dim_capacity', conn, if_exists='append', index=False)
   
    count = len(df)
    conn.close()
    logging.info(f"[OK] Imported {count:,} capacity records")
 
  def import_case_pack(self):
    """Import CASE_PACK (warehouse pack) information"""
    logging.info("Importing Case Pack Size...")
   
    file_path = self.tables_folder / 'Case Pack Size.xlsx'
    df = pd.read_excel(file_path, dtype={'Item Nbr': str})
   
    # Clean column names
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
   
    conn = self.connect_db()
   
    # Clear existing data
    cursor = conn.cursor()
    cursor.execute('DELETE FROM dim_case_pack')
   
    # Insert new data
    df.to_sql('dim_case_pack', conn, if_exists='append', index=False)
   
    count = len(df)
    conn.close()
    logging.info(f"[OK] Imported {count:,} CASE_PACK records")
 
  def import_client_forecast(self, week_date=None):
    """Import or update Client Forecast"""
    logging.info("Importing Client Forecast...")
   
    file_path = self.tables_folder / 'client_forecast.csv'
    df = pd.read_csv(file_path)
   
    # Clean column names
    df.columns = df.columns.str.strip().str.lower()
   
    conn = self.connect_db()
   
    # Clear existing data
    cursor = conn.cursor()
    cursor.execute('DELETE FROM CLIENT_FORECAST_fact')
   
    # Insert new data
    df.to_sql('CLIENT_FORECAST_fact', conn, if_exists='append', index=False)
   
    count = len(df)
    conn.close()
    logging.info(f"[OK] Imported {count:,} client forecast records")

  def import_forecast_vendor(self, week_date=None):
    """Import or update ForecastVendor weather projections"""
    logging.info("Importing ForecastVendor...")
   
    file_path = self.tables_folder / 'ForecastVendor.xlsx'
    df = pd.read_excel(file_path)
   
    # Clean column names
    df.columns = df.columns.str.strip().str.lower()
   
    # Rename columns to match database schema
    column_mapping = {
      'region': 'region',
      'tw': 'tw_pct',
      'nw': 'nw_pct',
      'm1': 'm1_pct',
      'm2': 'm2_pct',
      'm3': 'm3_pct'
    }
    df = df.rename(columns=column_mapping)
   
    # Add import date (use provided or current date)
    if week_date is None:
      from datetime import datetime
      week_date = datetime.now().strftime('%Y-%m-%d')
   
    df['week_imported'] = week_date
   
    conn = self.connect_db()
    cursor = conn.cursor()
   
    # Delete existing records for this week (allows re-import if corrected)
    cursor.execute('DELETE FROM fact_forecast_vendor')
   
    # Insert new data
    df.to_sql('fact_forecast_vendor', conn, if_exists='append', index=False)
   
    count = len(df)
    conn.commit()
    conn.close()
    logging.info(f"[OK] Imported {count:,} ForecastVendor projections for {week_date}")
 
  def import_all_dimensions(self):
    """Import all dimension tables at once (initial setup)"""
    logging.info("="*60)
    logging.info("IMPORTING ALL DIMENSION TABLES")
    logging.info("="*60)
   
    try:
      self.import_store_table()
      self.import_style_information()
      self.import_capacity()
      self.import_case_pack()
      self.import_forecast_vendor()
     
      logging.info("="*60)
      logging.info("[OK] ALL DIMENSION TABLES IMPORTED SUCCESSFULLY")
      logging.info("="*60)
      self.print_summary()
     
    except Exception as e:
      logging.error(f"[ERROR] Failed to import dimension tables: {str(e)}")
      import traceback
      logging.error(traceback.format_exc())
 
  def print_summary(self):
    """Print summary of all tables in database"""
    conn = self.connect_db()
    cursor = conn.cursor()
   
    tables = [
      ('dim_stores', 'Stores'),
      ('dim_items', 'Items/Styles'),
      ('dim_capacity', 'Capacity Records'),
      ('dim_case_pack', 'CASE_PACK Records'),
      ('fact_forecast_vendor', 'ForecastVendor Projections'),
      ('POS_fact', 'POS Records (Non-Category A)'),
      ('POS_category_a_fact', 'POS Records (Category A)')
    ]
   
    logging.info("\nDATABASE SUMMARY:")
    for table_name, description in tables:
      try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        logging.info(f" {description}: {count:,} records")
      except:
        logging.info(f" {description}: Table not found")
   
    conn.close()


# =============================================================================
# USAGE
# =============================================================================

if __name__ == "__main__":

  manager = DimensionTableManager('f5c_data.db')
 
  # -------------------------------------------------------------------------
  # OPTION 1: Initial Setup - Create tables and import all data
  # -------------------------------------------------------------------------
  print("Creating dimension tables...")
  #manager.create_dimension_tables()
 
  #print("\nImporting all dimension data...")
  #manager.import_all_dimensions()
 
  # -------------------------------------------------------------------------
  # OPTION 2: Weekly ForecastVendor Update Only
  # -------------------------------------------------------------------------
  # Uncomment for weekly updates:
  from datetime import datetime
  week_date = datetime.now().strftime('%Y-%m-%d')
  manager.import_forecast_vendor(week_date)
 
  # -------------------------------------------------------------------------
  # OPTION 3: Update specific table (if data changed)
  # -------------------------------------------------------------------------
  #manager.import_client_forecast() # If capacity changed
  #manager.import_style_information() # If costs updated
  #manager.import_case_pack()
  #manager.import_store_table()
  print("\n[OK] Dimension tables setup complete!")
