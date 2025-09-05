import sqlite3

def initialize_local_database():
    """
    Connects to a local SQLite database file and creates the necessary tables
    mirroring the Supabase schema.
    """
    db_filename = 'local_data.db'
    try:
        # Connect to the database. This will create the file if it doesn't exist.
        conn = sqlite3.connect(db_filename)
        cursor = conn.cursor()
        print(f"Successfully connected to {db_filename}")

        # --- Create the asset_types table ---
        # This table is a simple lookup for asset type names.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS asset_types (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            )
        ''')
        print("Table 'asset_types' created or already exists.")

        # --- Create the data_points table ---
        # This table stores the main data point configurations.
        # 'identifiers' and 'asset_types' will be stored as TEXT containing JSON arrays.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_points (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                identifiers TEXT,
                asset_types TEXT,
                data_type TEXT NOT NULL,
                range_min REAL,
                range_max REAL,
                string_options TEXT
            )
        ''')
        print("Table 'data_points' created or already exists.")

        # --- Populate asset_types with default values if it's empty ---
        cursor.execute("SELECT COUNT(*) FROM asset_types")
        if cursor.fetchone()[0] == 0:
            print("Populating 'asset_types' with default values...")
            default_types = [
                ("DG",), ("HVAC",), ("SOLAR Inverter",),
                ("Sub-Meter",), ("Temp Sensor",), ("Hum Sensor",)
            ]
            cursor.executemany("INSERT INTO asset_types (name) VALUES (?)", default_types)
            print(f"{len(default_types)} default asset types inserted.")
        else:
            print("'asset_types' table already contains data. Skipping population.")

        # Commit the changes and close the connection
        conn.commit()
        print("Database changes committed.")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    initialize_local_database()
