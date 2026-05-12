import sqlite3
import pandas as pd
import logging

# Setup logging
logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s - %(levelname)s - %(message)s'
)

class WeekMappingManager:
  def __init__(self, db_path='f5c_data.db'):
    self.db_path = db_path
    self.excel_path = 'week_lookup.xlsx'
 
  def connect_db(self):
    return sqlite3.connect(self.db_path)
 
  def create_week_mapping_table(self):
    """Create the week mapping table in the database"""
    logging.info("Creating week mapping table...")
   
    conn = self.connect_db()
    cursor = conn.cursor()
   
    # Create table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dim_week_mapping (
      ty_week INTEGER PRIMARY KEY,
      ly_week INTEGER,
      notes TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
   
    # Create index
    cursor.execute('''
    CREATE INDEX IF NOT EXISTS idx_week_mapping_ty 
    ON dim_week_mapping(ty_week)
    ''')
   
    conn.commit()
    conn.close()
    logging.info("[OK] Week mapping table created")
 
  def create_sample_csv(self, start_week=202501, num_weeks=52):
    """
    Create a sample CSV template for you to fill in
   
    Parameters:
    -----------
    start_week : int
      Starting week (e.g., 202501 for 2025 Week 1)
    num_weeks : int
      Number of weeks to generate
    """
    logging.info(f"Creating sample CSV starting from week {start_week}...")
   
    # Generate week sequences
    ty_weeks = []
    ly_weeks = []
   
    for i in range(num_weeks):
      # Calculate TY week
      year = start_week // 100
      week = (start_week % 100) + i
     
      # Handle week overflow (week 53 -> week 1 next year)
      if week > 52:
        week = week - 52
        year += 1
     
      ty_week = (year * 100) + week
     
      # Calculate LY week (you'll need to adjust these manually)
      # Default: assume same week number last year
      ly_week = ((year - 1) * 100) + week
     
      ty_weeks.append(ty_week)
      ly_weeks.append(ly_week)
   
    # Create DataFrame
    df = pd.DataFrame({
      'ty_week': ty_weeks,
      'ly_week': ly_weeks,
      'notes': '' # Space for you to add notes about alignment
    })
   
    # Save to CSV
    df.to_csv(self.csv_path, index=False)
    logging.info(f"[OK] Sample CSV created: {self.csv_path}")
    logging.info(f"  Contains {len(df)} weeks")
    logging.info(f"  IMPORTANT: Review and manually adjust ly_week values based on calendar alignment!")
   
    return df
 
  def import_week_mapping(self, file_path=None):
    """
    Import week mapping from Excel file into database
   
    Parameters:
    -----------
    file_path : str, optional
      Path to Excel file. If None, uses default 'week_lookup.xlsx'
    """
    if file_path:
      self.excel_path = file_path
   
    logging.info(f"Importing week mapping from {self.excel_path}...")
   
    try:
      # Read Excel file
      df = pd.read_excel(self.excel_path)
     
      # Show columns to help identify correct ones
      logging.info(f"Found columns: {list(df.columns)}")
     
      # Try to identify ty_week and ly_week columns
      # Common variations: 'TY Week', 'This Year', 'LY Week', 'Last Year', etc.
     
      ty_col = None
      ly_col = None
     
      # Look for TY week column
      for col in df.columns:
        col_lower = str(col).lower().strip()
        if any(term in col_lower for term in ['ty', 'this year', 'current', '2025']):
          ty_col = col
          break
     
      # Look for LY week column 
      for col in df.columns:
        col_lower = str(col).lower().strip()
        if any(term in col_lower for term in ['ly', 'last year', 'prior', '2024']):
          ly_col = col
          break
     
      if ty_col is None or ly_col is None:
        logging.error("Could not automatically identify TY and LY columns")
        logging.error(f"Available columns: {list(df.columns)}")
        logging.error("Please specify column names manually")
        return False
     
      logging.info(f"Using TY column: '{ty_col}', LY column: '{ly_col}'")
     
      # Rename to standard names
      df = df.rename(columns={ty_col: 'ty_week', ly_col: 'ly_week'})
     
      # Keep only needed columns
      df = df[['ty_week', 'ly_week']].copy()
     
      # Ensure correct data types
      df['ty_week'] = pd.to_numeric(df['ty_week'], errors='coerce').astype('Int64')
      df['ly_week'] = pd.to_numeric(df['ly_week'], errors='coerce').astype('Int64')
     
      # Remove rows with missing values
      df = df.dropna()
      df['ty_week'] = df['ty_week'].astype(int)
      df['ly_week'] = df['ly_week'].astype(int)
     
      conn = self.connect_db()
      cursor = conn.cursor()
     
      # Clear existing data
      cursor.execute('DELETE FROM dim_week_mapping')
      logging.info("Cleared existing week mappings")
     
      # Insert new data
      df[['ty_week', 'ly_week']].to_sql('dim_week_mapping', conn, if_exists='append', index=False)
     
      count = len(df)
      conn.commit()
      conn.close()
     
      logging.info(f"[OK] Imported {count} week mappings")
     
      # Show sample
      logging.info("\nSample mappings:")
      for _, row in df.head(10).iterrows():
        logging.info(f"  TY Week {row['ty_week']} → LY Week {row['ly_week']}")
     
      return True
     
    except FileNotFoundError:
      logging.error(f"Excel file not found: {self.excel_path}")
      logging.error(f"Make sure the file exists at: {self.excel_path}")
      return False
    except Exception as e:
      logging.error(f"Error importing week mapping: {str(e)}")
      import traceback
      logging.error(traceback.format_exc())
      return False
 
  def get_ly_week(self, ty_week):
    """
    Look up the corresponding LY week for a given TY week
   
    Parameters:
    -----------
    ty_week : int
      This year's week (e.g., 202537)
   
    Returns:
    --------
    int : Corresponding LY week, or None if not found
    """
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
      logging.warning(f"No mapping found for TY week {ty_week}")
      # Default fallback: subtract 100 (last year, same week)
      return ty_week - 100
 
  def add_single_mapping(self, ty_week, ly_week, notes=''):
    """
    Add or update a single week mapping
   
    Parameters:
    -----------
    ty_week : int
      This year's week
    ly_week : int
      Last year's corresponding week
    notes : str, optional
      Any notes about this mapping
    """
    conn = self.connect_db()
    cursor = conn.cursor()
   
    cursor.execute('''
    INSERT OR REPLACE INTO dim_week_mapping (ty_week, ly_week, notes)
    VALUES (?, ?, ?)
    ''', (ty_week, ly_week, notes))
   
    conn.commit()
    conn.close()
   
    logging.info(f"[OK] Added mapping: TY {ty_week} → LY {ly_week}")
 
  def view_all_mappings(self):
    """Display all week mappings in the database"""
    conn = self.connect_db()
    df = pd.read_sql('SELECT * FROM dim_week_mapping ORDER BY ty_week', conn)
    conn.close()
   
    if len(df) == 0:
      logging.info("No week mappings found in database")
    else:
      logging.info(f"\nAll week mappings ({len(df)} total):")
      print(df.to_string(index=False))
   
    return df


# =============================================================================
# USAGE
# =============================================================================

if __name__ == "__main__":
  manager = WeekMappingManager('f5c_data.db')
 
  # -------------------------------------------------------------------------
  # STEP 1: Create the database table (run once)
  # -------------------------------------------------------------------------
  print("Step 1: Creating week mapping table in database...")
  manager.create_week_mapping_table()
 
  # -------------------------------------------------------------------------
  # STEP 2: Import from your existing Excel file
  # -------------------------------------------------------------------------
  print("\nStep 2: Importing week mappings from week_lookup.xlsx...")
 
  # If your file is in a different location, specify the path:
  # manager.import_week_mapping('C:/path/to/your/week_lookup.xlsx')
 
  # Otherwise, just run with default (looks for week_lookup.xlsx in current folder):
  success = manager.import_week_mapping()
 
  if success:
    print("\n" + "="*60)
    print("[OK] WEEK MAPPING IMPORTED SUCCESSFULLY")
    print("="*60)
   
    # -------------------------------------------------------------------------
    # STEP 3: Verify the import
    # -------------------------------------------------------------------------
    print("\nVerifying imported data:")
    manager.view_all_mappings()
   
    # -------------------------------------------------------------------------
    # STEP 4: Test lookup
    # -------------------------------------------------------------------------
   # print("\nTesting lookup for current week (202537):")
   # ly_week = manager.get_ly_week(202537)
   # print(f"  TY Week 202537 maps to LY Week: {ly_week}")
 
  else:
    print("\n" + "="*60)
    print("IMPORT FAILED - Check the error messages above")
    print("="*60)
    print("\nCommon issues:")
    print("1. File not found - make sure week_lookup.xlsx is in the same folder")
    print("2. Column names not recognized - check the column headers in your Excel file")
    print("  The script looks for columns containing: 'TY', 'This Year', 'LY', 'Last Year'")
 
  print("\n[OK] Week mapping script complete!")
