import sqlite3

def create_database(db_path='f5c_data.db'):
  """Create SQLite database with proper schema"""

  conn = sqlite3.connect(db_path)
  cursor = conn.cursor()

  # Create main fact table
  cursor.execute('''
  CREATE TABLE IF NOT EXISTS POS_fact (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    f5c_calendar_week INTEGER NOT NULL,
    subcategory_description TEXT NOT NULL,
    subcategory_number TEXT NOT NULL,
    store_number TEXT NOT NULL,
    f5c_item_number TEXT NOT NULL,
    pos_quantity_this_year INTEGER,
    pos_quantity_last_year INTEGER,
    store_in_warehouse_quantity_this_year INTEGER,
    store_in_transit_quantity_this_year INTEGER,
    pos_sales_this_year REAL,
    pos_sales_last_year REAL,
    max_shelf_quantity_this_year INTEGER,
    store_on_order_quantity_this_year INTEGER,
    store_on_hand_quantity_this_year_eop INTEGER,
    store_on_hand_quantity_last_year_eop INTEGER,
    traited_store_count_this_year INTEGER,
    valid_store_count_this_year INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )
  ''')

  # Create Category A fact table
  cursor.execute('''
  CREATE TABLE IF NOT EXISTS POS_category_a_fact (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    f5c_calendar_week INTEGER NOT NULL,
    subcategory_description TEXT NOT NULL,
    subcategory_number TEXT NOT NULL,
    store_number TEXT NOT NULL,
    f5c_item_number TEXT NOT NULL,
    vendor_stock_id TEXT,
    color_description TEXT,
    trait_description TEXT,
    pos_quantity_this_year INTEGER,
    pos_quantity_last_year INTEGER,
    store_in_warehouse_quantity_this_year INTEGER,
    store_in_transit_quantity_this_year INTEGER,
    pos_sales_this_year REAL,
    pos_sales_last_year REAL,
    max_shelf_quantity_this_year INTEGER,
    store_on_order_quantity_this_year INTEGER,
    store_on_hand_quantity_this_year_eop INTEGER,
    store_on_hand_quantity_last_year_eop INTEGER,
    traited_store_count_this_year INTEGER,
    valid_store_count_this_year INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )
  ''')

  # Create indexes
  indexes = [
    'CREATE INDEX IF NOT EXISTS idx_sales_fact_week ON POS_fact(f5c_calendar_week)',
    'CREATE INDEX IF NOT EXISTS idx_sales_fact_store ON POS_fact(store_number)',
    'CREATE INDEX IF NOT EXISTS idx_sales_fact_subcategory ON POS_fact(subcategory_number)',
    'CREATE INDEX IF NOT EXISTS idx_sales_fact_item ON POS_fact(f5c_item_number)',
    'CREATE INDEX IF NOT EXISTS idx_category_a_fact_week ON POS_category_a_fact(f5c_calendar_week)',
    'CREATE INDEX IF NOT EXISTS idx_category_a_fact_store ON POS_category_a_fact(store_number)',
    'CREATE INDEX IF NOT EXISTS idx_category_a_fact_subcategory ON POS_category_a_fact(subcategory_number)',
    'CREATE INDEX IF NOT EXISTS idx_category_a_fact_item ON POS_category_a_fact(f5c_item_number)',
    'CREATE INDEX IF NOT EXISTS idx_category_a_fact_vendor ON POS_category_a_fact(vendor_stock_id)'
  ]

  for index in indexes:
    cursor.execute(index)

  conn.commit()
  conn.close()

  print(f"Database created successfully at: {db_path}")
  print("Tables created: POS_fact, POS_category_a_fact")

if __name__ == "__main__":
  create_database()
