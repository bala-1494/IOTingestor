import sqlite3
import json

# --- DATABASE SETUP ---

def get_db_connection():
    """Establishes a connection to the SQLite database."""
    conn = sqlite3.connect('data_points.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initializes the database and creates/updates tables as needed."""
    conn = get_db_connection()
    cursor = conn.cursor()
    # Create data_points table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS data_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            identifiers TEXT,
            asset_types TEXT,
            data_type TEXT NOT NULL,
            range_min REAL,
            range_max REAL
        )
    ''')

    # --- SCHEMA MIGRATION: Add string_options column if it doesn't exist ---
    cursor.execute("PRAGMA table_info(data_points)")
    columns = [info['name'] for info in cursor.fetchall()]
    if 'string_options' not in columns:
        cursor.execute("ALTER TABLE data_points ADD COLUMN string_options TEXT")

    # Create asset_types table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS asset_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
    ''')
    # Check if the asset_types table is empty and populate it with defaults
    cursor.execute("SELECT COUNT(*) FROM asset_types")
    if cursor.fetchone()[0] == 0:
        default_types = ["DG", "HVAC", "SOLAR Inverter", "Sub-Meter", "Temp Sensor", "Hum Sensor"]
        for asset_type in default_types:
            cursor.execute("INSERT INTO asset_types (name) VALUES (?)", (asset_type,))

    conn.commit()
    conn.close()

def add_asset_type(name):
    """Adds a new asset type to the database, checking for uniqueness (case-insensitive)."""
    conn = get_db_connection()
    cursor = conn.cursor()
    # Case-insensitive check for uniqueness
    cursor.execute("SELECT id FROM asset_types WHERE LOWER(name) = ?", (name.lower(),))
    if cursor.fetchone():
        conn.close()
        return False # Indicates duplicate

    cursor.execute("INSERT INTO asset_types (name) VALUES (?)", (name,))
    conn.commit()
    conn.close()
    return True # Indicates success

def get_all_asset_types():
    """Fetches all asset type names from the database."""
    conn = get_db_connection()
    types = conn.execute('SELECT name FROM asset_types ORDER BY name ASC').fetchall()
    conn.close()
    return [row['name'] for row in types]

def add_data_point(name, identifiers, asset_types, data_type, range_min, range_max, string_options):
    """Adds a new data point to the database."""
    conn = get_db_connection()
    conn.execute(
        'INSERT INTO data_points (name, identifiers, asset_types, data_type, range_min, range_max, string_options) VALUES (?, ?, ?, ?, ?, ?, ?)',
        (name, json.dumps(identifiers), json.dumps(asset_types), data_type, range_min, range_max, string_options)
    )
    conn.commit()
    conn.close()

def update_data_point(dp_id, name, identifiers, asset_types, data_type, range_min, range_max, string_options):
    """Updates an existing data point in the database, identified by its ID."""
    conn = get_db_connection()
    conn.execute(
        'UPDATE data_points SET name = ?, identifiers = ?, asset_types = ?, data_type = ?, range_min = ?, range_max = ?, string_options = ? WHERE id = ?',
        (name, json.dumps(identifiers), json.dumps(asset_types), data_type, range_min, range_max, string_options, dp_id)
    )
    conn.commit()
    conn.close()

def update_data_point_by_name(name, identifiers, asset_types, data_type, range_min, range_max, string_options):
    """Updates an existing data point in the database, identified by its name."""
    conn = get_db_connection()
    conn.execute(
        'UPDATE data_points SET identifiers = ?, asset_types = ?, data_type = ?, range_min = ?, range_max = ?, string_options = ? WHERE name = ?',
        (json.dumps(identifiers), json.dumps(asset_types), data_type, range_min, range_max, string_options, name)
    )
    conn.commit()
    conn.close()

def get_all_data_points():
    """
    Fetches all data points and their column names from the database.
    Returns a tuple of (rows, column_names).
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM data_points ORDER BY id DESC')
    data_points = cursor.fetchall()
    columns = [description[0] for description in cursor.description] if cursor.description else []
    conn.close()
    return data_points, columns

def get_data_points_by_asset_type(target_asset_type):
    """Fetches all data points associated with a specific asset type."""
    conn = get_db_connection()
    all_dps = conn.execute('SELECT * FROM data_points').fetchall()
    conn.close()

    matching_dps = []
    for dp in all_dps:
        asset_types = json.loads(dp['asset_types'] or '[]')
        if target_asset_type in asset_types:
            matching_dps.append(dp)
    return matching_dps


def get_data_point_by_id(dp_id):
    """Fetches a single data point by its ID."""
    conn = get_db_connection()
    dp = conn.execute('SELECT * FROM data_points WHERE id = ?', (dp_id,)).fetchone()
    conn.close()
    return dp

def get_data_point_by_name(name):
    """Fetches a single data point by its name."""
    conn = get_db_connection()
    dp = conn.execute('SELECT * FROM data_points WHERE name = ?', (name,)).fetchone()
    conn.close()
    return dp

def delete_all_data_points():
    """Deletes all records from the data_points table."""
    conn = get_db_connection()
    conn.execute('DELETE FROM data_points')
    conn.commit()
    conn.close()

def check_identifier_uniqueness(identifiers, current_dp_id=None):
    """
    Checks if any of the given identifiers already exist in the database,
    excluding the current data point being edited.
    Returns the first duplicate identifier found, or None if all are unique.
    """
    conn = get_db_connection()
    query = 'SELECT id, identifiers FROM data_points'
    all_dps = conn.execute(query).fetchall()
    conn.close()

    for identifier in identifiers:
        for row in all_dps:
            # Skip the current data point when checking for duplicates during an update
            if current_dp_id is not None and row['id'] == current_dp_id:
                continue

            if row['identifiers']:
                try:
                    existing_identifiers = json.loads(row['identifiers'])
                    if identifier in existing_identifiers:
                        return identifier # Found a duplicate
                except json.JSONDecodeError:
                    continue # Skip corrupted data
    return None # All identifiers are unique
