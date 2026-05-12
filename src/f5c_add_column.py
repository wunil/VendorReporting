import sqlite3

conn = sqlite3.connect('f5c_data.db')
cursor = conn.cursor()

# Add column to POS_fact table
cursor.execute('''
  ALTER TABLE POS_fact
  ADD COLUMN store_on_order_quantity_last_year INTEGER
''')
cursor.execute('''
  ALTER TABLE POS_fact
  ADD COLUMN store_in_transit_quantity_last_year INTEGER
''')
cursor.execute('''
  ALTER TABLE POS_fact
  ADD COLUMN store_in_warehouse_quantity_last_year INTEGER
''')
cursor.execute('''
  ALTER TABLE POS_fact
  ADD COLUMN store_on_hand_quantity_last_year INTEGER
''')
cursor.execute('''
  ALTER TABLE POS_fact
  ADD COLUMN store_on_hand_quantity_this_year INTEGER
''')

# Add column to POS_category_a_fact table
cursor.execute('''
  ALTER TABLE POS_category_a_fact
  ADD COLUMN store_on_order_quantity_last_year INTEGER
''')
cursor.execute('''
  ALTER TABLE POS_category_a_fact
  ADD COLUMN store_in_transit_quantity_last_year INTEGER
''')
cursor.execute('''
  ALTER TABLE POS_category_a_fact
  ADD COLUMN store_in_warehouse_quantity_last_year INTEGER
''')
cursor.execute('''
  ALTER TABLE POS_category_a_fact
  ADD COLUMN store_on_hand_quantity_last_year INTEGER
''')
cursor.execute('''
  ALTER TABLE POS_category_a_fact
  ADD COLUMN store_on_hand_quantity_this_year INTEGER
''')

conn.commit()
conn.close()

print("Column added successfully!")
