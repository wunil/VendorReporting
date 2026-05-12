import sqlite3
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s - %(levelname)s - %(message)s',
  handlers=[
    logging.FileHandler('import_log.txt', encoding='utf-8'),
    logging.StreamHandler()
  ]
)

class Fortune500ClientDataImporter:
  def __init__(self, db_path='f5c_data.db'):
    self.db_path = db_path
    self.category_a_subcategories = [1001, 1002]
    self.category_d_subcategories = [812, 822, 834, 835]
    self.category_c_subcategories = [2049]
    self.category_b_subcategories = [2258]
   
  def connect_db(self):
    """Create database connection"""
    return sqlite3.connect(self.db_path)
 
  def import_csv_bulk(self, csv_path, category_name):
    """Import CSV using fast bulk insert (for initial load)"""
    logging.info(f"Starting BULK import of {csv_path}")
   
    try:
      # Read CSV with proper dtypes
      df = pd.read_csv(csv_path, dtype=str, low_memory=False)
      logging.info(f"Loaded {len(df):,} rows from {csv_path}")
     
      # Clean column names
      df.columns = df.columns.str.strip()
     
      # Validate required columns
      required_cols = ['f5c_calendar_week', 'subcategory_number', 'store_number', 'f5c_item_number']
      missing_cols = [col for col in required_cols if col not in df.columns]
      if missing_cols:
        logging.error(f"Missing required columns: {missing_cols}")
        return False
     
      # Separate Category A from other categories
      category_a_data = df[df['subcategory_number'].isin(['1001', '1002'])]
      other_data = df[~df['subcategory_number'].isin(['1001', '1002'])]
     
      conn = self.connect_db()
     
      # Bulk insert Category A data
      if len(category_a_data) > 0:
        self._bulk_insert(conn, category_a_data, 'POS_category_a_fact', is_category_a=True)
        logging.info(f"[Ok] Imported {len(category_a_data):,} Category A records")
     
      # Bulk insert other category data
      if len(other_data) > 0:
        self._bulk_insert(conn, other_data, 'POS_fact', is_category_a=False)
        logging.info(f"[Ok] Imported {len(other_data):,} non-Category A records")
     
      conn.close()
      logging.info(f"[Ok] Successfully completed import of {csv_path}")
      return True
     
    except Exception as e:
      logging.error(f"[Error] Error importing {csv_path}: {str(e)}")
      import traceback
      logging.error(traceback.format_exc())
      return False
 
  def _bulk_insert(self, conn, df, table_name, is_category_a=False):
    """Fast bulk insert - no duplicate checking"""
   
    # Define column mappings
    base_columns = [
      'f5c_calendar_week', 'subcategory_description', 'subcategory_number',
      'store_number', 'f5c_item_number', 'pos_quantity_this_year',
      'pos_quantity_last_year', 'store_in_warehouse_quantity_this_year',
      'store_in_transit_quantity_this_year', 'pos_sales_this_year',
      'pos_sales_last_year', 'max_shelf_quantity_this_year',
      'store_on_order_quantity_this_year', 'store_on_hand_quantity_this_year_eop',
      'store_on_hand_quantity_last_year_eop', 'traited_store_count_this_year',
      'valid_store_count_this_year', 'traited_store_count_last_year'
    ]
   
    category_a_extra_columns = [
      'vendor_stock_id', 'modular_based_merchandising_description',
      'color_description', 'trait_description'
    ]
   
    columns = base_columns + category_a_extra_columns if is_category_a else base_columns
   
    # Filter to available columns
    available_columns = [col for col in columns if col in df.columns]
    df_filtered = df[available_columns].copy()
   
    # Use pandas to_sql for bulk insert (much faster!)
    df_filtered.to_sql(table_name, conn, if_exists='append', index=False, method='multi', chunksize=500)
 
  def import_csv_upsert(self, csv_path, category_name):
    """Import CSV with upsert logic (for weekly updates)"""
    logging.info(f"Starting UPSERT import of {csv_path}")
   
    try:
      # Read CSV
      df = pd.read_csv(csv_path, dtype=str, low_memory=False)
      logging.info(f"Loaded {len(df):,} rows from {csv_path}")
     
      # Clean column names
      df.columns = df.columns.str.strip()
     
      # Validate required columns
      required_cols = ['f5c_calendar_week', 'subcategory_number', 'store_number', 'f5c_item_number']
      missing_cols = [col for col in required_cols if col not in df.columns]
      if missing_cols:
        logging.error(f"Missing required columns: {missing_cols}")
        return False
     
      # Separate Category A from other categories
      category_a_data = df[df['subcategory_number'].isin(['1001', '1002'])]
      other_data = df[~df['subcategory_number'].isin(['1001', '1002'])]
     
      conn = self.connect_db()
     
      # Upsert Category A data
      if len(category_a_data) > 0:
        self._upsert_data(conn, category_a_data, 'POS_category_a_fact', is_category_a=True)
        logging.info(f"[Ok] Upserted {len(category_a_data):,} Category A records")
     
      # Upsert other category data
      if len(other_data) > 0:
        self._upsert_data(conn, other_data, 'POS_fact', is_category_a=False)
        logging.info(f"[Ok] Upserted {len(other_data):,} non-Category A records")
     
      conn.close()
      logging.info(f"[Ok] Successfully completed upsert of {csv_path}")
      return True
     
    except Exception as e:
      logging.error(f"[Error] Error upserting {csv_path}: {str(e)}")
      return False
 
  def _upsert_data(self, conn, df, table_name, is_category_a=False):
    """Insert or update using temporary table merge - faster than row-by-row"""
    cursor = conn.cursor()
   
    # Define column mappings
    base_columns = [
      'f5c_calendar_week', 'subcategory_description', 'subcategory_number',
      'store_number', 'f5c_item_number', 'pos_quantity_this_year',
      'pos_quantity_last_year', 'store_in_warehouse_quantity_this_year',
      'store_in_transit_quantity_this_year', 'pos_sales_this_year',
      'pos_sales_last_year', 'max_shelf_quantity_this_year',
      'store_on_order_quantity_this_year', 'store_on_hand_quantity_this_year_eop',
      'store_on_hand_quantity_last_year_eop', 'traited_store_count_this_year',
      'valid_store_count_this_year', 'traited_store_count_last_year'
    ]
   
    category_a_extra_columns = [
      'vendor_stock_id', 'modular_based_merchandising_description',
      'color_description', 'trait_description'
    ]
   
    columns = base_columns + category_a_extra_columns if is_category_a else base_columns
    available_columns = [col for col in columns if col in df.columns]
    df_filtered = df[available_columns].copy()
    df_filtered = df_filtered.where(pd.notnull(df_filtered), None)
    
    total_rows = len(df_filtered)
    logging.info(f" Processing {total_rows:,} rows using temp table method...")
    
    # Create temporary table
    temp_table = f"{table_name}_temp"
    cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
    
    # Get original table structure
    cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    create_sql = cursor.fetchone()[0]
    create_temp_sql = create_sql.replace(f"CREATE TABLE {table_name}", f"CREATE TABLE {temp_table}")
    create_temp_sql = create_temp_sql.replace("PRIMARY KEY AUTOINCREMENT", "")  # Remove auto-increment for temp
    
    cursor.execute(create_temp_sql)
    
    # Bulk insert into temp table
    logging.info(" Inserting into temporary table...")
    df_filtered.to_sql(temp_table, conn, if_exists='append', index=False, method='multi', chunksize=1000)
    
    # Delete matching records from main table
    logging.info(" Removing old versions of records...")
    cursor.execute(f'''
        DELETE FROM {table_name}
        WHERE (f5c_calendar_week, store_number, f5c_item_number) IN (
            SELECT f5c_calendar_week, store_number, f5c_item_number FROM {temp_table}
        )
    ''')
    deleted = cursor.rowcount
    
    # Insert all from temp to main
    logging.info(" Inserting new/updated records...")
    col_list = ', '.join(available_columns)
    cursor.execute(f'''
        INSERT INTO {table_name} ({col_list})
        SELECT {col_list} FROM {temp_table}
    ''')
    inserted = cursor.rowcount
    
    # Clean up
    cursor.execute(f"DROP TABLE {temp_table}")
    conn.commit()
    
    logging.info(f" [Ok] Updated: {deleted:,} | Inserted: {inserted:,}")

 
  def clear_all_data(self, confirm=True):
      """Clear all data from both tables (use before initial load)"""
      
      if confirm:
          # Get current record counts
          conn = self.connect_db()
          cursor = conn.cursor()
          
          cursor.execute("SELECT COUNT(*) FROM POS_fact")
          main_count = cursor.fetchone()[0]
          
          cursor.execute("SELECT COUNT(*) FROM POS_category_a_fact")
          category_a_count = cursor.fetchone()[0]
          
          total = main_count + category_a_count
          
          if total > 0:
              logging.warning("="*60)
              logging.warning(f"⚠️  WARNING: About to DELETE {total:,} records!")
              logging.warning(f"   - POS_fact: {main_count:,} records")
              logging.warning(f"   - POS_category_a_fact: {category_a_count:,} records")
              logging.warning("="*60)
              
              response = input("\n🛑 Type 'DELETE ALL DATA' to confirm (or anything else to cancel): ")
              
              if response != "DELETE ALL DATA":
                  logging.info("❌ Deletion cancelled by user")
                  conn.close()
                  return False
          
          conn.close()
      
      logging.info("Clearing all existing data...")
      conn = self.connect_db()
      cursor = conn.cursor()
      
      cursor.execute("DELETE FROM POS_fact")
      cursor.execute("DELETE FROM POS_category_a_fact")
      
      conn.commit()
      conn.close()
      logging.info("[Ok] All data cleared")
      return True

 
  def initial_load(self, data_folder, clear_first=False): #important to change
    """Load all historical data files using FAST bulk insert"""
    logging.info("="*60)
    logging.info("STARTING INITIAL DATA LOAD (BULK MODE)")
    logging.info("="*60)
   
    if clear_first:
      # This will now prompt for confirmation
      if not self.clear_all_data(confirm=True):
          logging.info("Initial load cancelled - no data was deleted")
          return
   
    folder = Path(data_folder)
    csv_files = list(folder.glob("*.csv"))
   
    if not csv_files:
      logging.error(f"No CSV files found in {data_folder}")
      return
   
    logging.info(f"Found {len(csv_files)} CSV files to import")
   
    for csv_file in csv_files:
      # Determine category from filename
      filename = csv_file.stem.lower()
     
      if 'category_a' in filename:
        category = 'Category A'
      elif 'category_d' in filename:
        category = 'Category D'
      elif 'data' in filename:
        category = 'Mixed Categories'
      else:
        category = 'Unknown'
     
      self.import_csv_bulk(csv_file, category)
   
    logging.info("="*60)
    logging.info("INITIAL LOAD COMPLETE")
    logging.info("="*60)
    self._print_summary()
 
  def weekly_update(self, csv_files):
    """Import weekly update files using UPSERT logic"""
    logging.info("="*60)
    logging.info(f"STARTING WEEKLY UPDATE - {datetime.now().strftime('%Y-%m-%d')}")
    logging.info("="*60)
   
    if isinstance(csv_files, str):
      csv_files = [csv_files]
   
    for csv_file in csv_files:
      csv_path = Path(csv_file)
      if csv_path.exists():
        filename = csv_path.stem.lower()
        if 'category_a' in filename:
          category = 'Category A'
        elif 'category_d' in filename:
          category = 'Category D'
        else:
          category = 'Mixed'
       
        self.import_csv_upsert(csv_path, category)
      else:
        logging.error(f"File not found: {csv_file}")
   
    logging.info("="*60)
    logging.info("WEEKLY UPDATE COMPLETE")
    logging.info("="*60)
    self._print_summary()
 
  def _print_summary(self):
    """Print database summary statistics"""
    conn = self.connect_db()
    cursor = conn.cursor()
   
    # Get counts from both tables
    cursor.execute("SELECT COUNT(*) FROM POS_fact")
    main_count = cursor.fetchone()[0]
   
    cursor.execute("SELECT COUNT(*) FROM POS_category_a_fact")
    category_a_count = cursor.fetchone()[0]
   
    # Get week ranges
    cursor.execute("SELECT MIN(f5c_calendar_week), MAX(f5c_calendar_week) FROM POS_fact")
    main_weeks = cursor.fetchone()
   
    cursor.execute("SELECT MIN(f5c_calendar_week), MAX(f5c_calendar_week) FROM POS_category_a_fact")
    category_a_weeks = cursor.fetchone()
   
    logging.info("\nDATABASE SUMMARY:")
    logging.info(f" Non-Category A records: {main_count:,}")
    if main_weeks[0]:
      logging.info(f"  Week range: {main_weeks[0]} to {main_weeks[1]}")
   
    logging.info(f" Category A records: {category_a_count:,}")
    if category_a_weeks[0]:
      logging.info(f"  Week range: {category_a_weeks[0]} to {category_a_weeks[1]}")
   
    logging.info(f" Total records: {main_count + category_a_count:,}")
   
    conn.close()


# =============================================================================
# USAGE EXAMPLES
# =============================================================================

if __name__ == "__main__":
  importer = Fortune500ClientDataImporter('f5c_data.db')
 
  # -------------------------------------------------------------------------
  # INITIAL LOAD (Use this for your 2-year historical data - FAST!)
  # -------------------------------------------------------------------------
  # Change the path to your CSV folder
  importer.initial_load('./data/import')
 
  # -------------------------------------------------------------------------
  # WEEKLY UPDATE (Use this for ongoing weekly imports - has duplicate checking)
  # -------------------------------------------------------------------------
  # Uncomment for weekly updates
  #importer.weekly_update([
  #   'category_a_data.csv',
  #   'general_data.csv',
   #  'category_d_data.csv'
  # ])
 
  print("\nImport script ready!")
  print("\nTo use:")
  print("1. For initial load: importer.initial_load('folder_path')")
  print("2. For weekly update: importer.weekly_update(['file1.csv', 'file2.csv'])")
