import streamlit as st
from streamlit_option_menu import option_menu
import sqlite3
import pandas as pd
import json
import datetime
import random
import io
import openpyxl
from openpyxl.styles import Font
import os

# --- LOCAL SQLITE DATABASE SETUP ---

def get_db_connection():
    """Establishes a connection to the local SQLite database."""
    conn = sqlite3.connect('local_data.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initializes the local database and creates tables if they don't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Create asset_types table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS asset_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
    ''')
    
    # Create data_points table
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

    # Populate asset_types with default values if empty
    cursor.execute("SELECT COUNT(*) FROM asset_types")
    if cursor.fetchone()[0] == 0:
        default_types = ["DG", "HVAC", "SOLAR Inverter", "Sub-Meter", "Temp Sensor", "Hum Sensor"]
        for asset_type in default_types:
            conn.execute("INSERT INTO asset_types (name) VALUES (?)", (asset_type,))
    
    conn.commit()
    conn.close()


def add_asset_type(name):
    """Adds a new asset type to the local database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # Case-insensitive check
        cursor.execute("SELECT id FROM asset_types WHERE LOWER(name) = ?", (name.lower(),))
        if cursor.fetchone():
            return False  # Duplicate

        cursor.execute("INSERT INTO asset_types (name) VALUES (?)", (name,))
        conn.commit()
        return True
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_all_asset_types():
    """Fetches all asset type names from the local database."""
    try:
        conn = get_db_connection()
        types = conn.execute('SELECT name FROM asset_types ORDER BY name ASC').fetchall()
        return [row['name'] for row in types]
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
        return []
    finally:
        if conn:
            conn.close()

def add_data_point(name, identifiers, asset_types, data_type, range_min, range_max, string_options):
    """Adds a new data point to the local database."""
    try:
        conn = get_db_connection()
        conn.execute(
            'INSERT INTO data_points (name, identifiers, asset_types, data_type, range_min, range_max, string_options) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (name, json.dumps(identifiers), json.dumps(asset_types), data_type, range_min, range_max, string_options)
        )
        conn.commit()
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
    finally:
        if conn:
            conn.close()


def update_data_point(dp_id, name, identifiers, asset_types, data_type, range_min, range_max, string_options):
    """Updates an existing data point in the local database by its ID."""
    try:
        conn = get_db_connection()
        conn.execute(
            'UPDATE data_points SET name = ?, identifiers = ?, asset_types = ?, data_type = ?, range_min = ?, range_max = ?, string_options = ? WHERE id = ?',
            (name, json.dumps(identifiers), json.dumps(asset_types), data_type, range_min, range_max, string_options, dp_id)
        )
        conn.commit()
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

def update_data_point_by_name(name, identifiers, asset_types, data_type, range_min, range_max, string_options):
    """Updates an existing data point in the local database by its name."""
    try:
        conn = get_db_connection()
        conn.execute(
            'UPDATE data_points SET identifiers = ?, asset_types = ?, data_type = ?, range_min = ?, range_max = ?, string_options = ? WHERE name = ?',
            (json.dumps(identifiers), json.dumps(asset_types), data_type, range_min, range_max, string_options, name)
        )
        conn.commit()
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
    finally:
        if conn:
            conn.close()


def get_all_data_points():
    """Fetches all data points and their column names from the local database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM data_points ORDER BY id DESC')
        data_points = cursor.fetchall()
        columns = [description[0] for description in cursor.description] if cursor.description else []
        return data_points, columns
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
        return [], []
    finally:
        if conn:
            conn.close()


def get_data_points_by_asset_type(target_asset_type):
    """Fetches all data points associated with a specific asset type from local DB."""
    try:
        conn = get_db_connection()
        all_dps = conn.execute('SELECT * FROM data_points').fetchall()
        
        matching_dps = []
        for dp in all_dps:
            # Manually parse the JSON string and check for the asset type
            asset_types = json.loads(dp['asset_types'] or '[]')
            if target_asset_type in asset_types:
                matching_dps.append(dp)
        return matching_dps
    except (sqlite3.Error, json.JSONDecodeError) as e:
        st.error(f"Error fetching data points by asset type: {e}")
        return []
    finally:
        if conn:
            conn.close()


def get_data_point_by_id(dp_id):
    """Fetches a single data point by its ID from the local database."""
    try:
        conn = get_db_connection()
        dp = conn.execute('SELECT * FROM data_points WHERE id = ?', (dp_id,)).fetchone()
        return dp
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
        return None
    finally:
        if conn:
            conn.close()

def get_data_point_by_name(name):
    """Fetches a single data point by its name from the local database."""
    try:
        conn = get_db_connection()
        dp = conn.execute('SELECT * FROM data_points WHERE name = ?', (name,)).fetchone()
        return dp
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
        return None
    finally:
        if conn:
            conn.close()

def delete_all_data_points():
    """Deletes all records from the data_points table in the local database."""
    try:
        conn = get_db_connection()
        conn.execute('DELETE FROM data_points')
        conn.commit()
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

def check_identifier_uniqueness(identifiers, current_dp_id=None):
    """
    Checks if any of the given identifiers already exist in the local database.
    """
    try:
        conn = get_db_connection()
        query = 'SELECT id, identifiers FROM data_points'
        all_dps = conn.execute(query).fetchall()

        for identifier in identifiers:
            for row in all_dps:
                if current_dp_id is not None and row['id'] == current_dp_id:
                    continue
                
                if row['identifiers']:
                    try:
                        existing_identifiers = json.loads(row['identifiers'])
                        if identifier in existing_identifiers:
                            return identifier
                    except json.JSONDecodeError:
                        continue
        return None
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
        return "Error"
    finally:
        if conn:
            conn.close()

# --- HELPER FUNCTIONS (No changes needed here) ---
def format_list_for_display(items):
    if not items:
        return ""
    if isinstance(items, str):
        try:
            items = json.loads(items)
        except (json.JSONDecodeError, TypeError):
            return ""
    if isinstance(items, list):
        return ", ".join(map(str, items))
    return ""

def generate_mock_value(dp):
    data_type = dp['data_type']
    if data_type == 'float':
        min_val = dp['range_min'] if dp['range_min'] is not None else 0.0
        max_val = dp['range_max'] if dp['range_max'] is not None else 100.0
        return round(random.uniform(min_val, max_val), 2)
    elif data_type == 'int':
        min_val = int(dp['range_min'] if dp['range_min'] is not None else 0)
        max_val = int(dp['range_max'] if dp['range_max'] is not None else 100)
        return random.randint(min_val, max_val)
    elif data_type == 'boolean':
        return random.choice([True, False])
    elif data_type == 'string':
        if 'string_options' in dp.keys() and dp['string_options']:
            options = [opt.strip() for opt in dp['string_options'].split(',')]
            return random.choice(options)
        return "Sample String"
    return None

def format_timestamp(dt_object):
    tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    dt_aware = dt_object.replace(tzinfo=tz)
    iso_str = dt_aware.strftime('%Y-%m-%dT%H:%M:%S%z')
    return iso_str

# --- BULK UPLOAD FUNCTIONS (No changes needed here) ---
def validate_bulk_upload(df):
    errors = []
    required_columns = ["name", "identifiers", "asset_types", "data_type"]
    valid_data_types = ["float", "int", "boolean", "string"]
    available_asset_types = get_all_asset_types()
    
    existing_points_df, _ = get_all_data_points()
    existing_names = [row['name'] for row in existing_points_df]
    
    for col in required_columns:
        if col not in df.columns:
            errors.append(f"Missing required column: '{col}'")
            return errors, None

    for i, row in df.iterrows():
        for col in required_columns:
            if pd.isna(row[col]):
                errors.append(f"Row {i+2}: Missing value in required column '{col}'.")
        
        if row['data_type'] not in valid_data_types:
            errors.append(f"Row {i+2}: Invalid data_type '{row['data_type']}'. Must be one of {valid_data_types}.")
            
        asset_types = [atype.strip() for atype in str(row['asset_types']).split(',')]
        for atype in asset_types:
            if atype not in available_asset_types:
                errors.append(f"Row {i+2}: Asset type '{atype}' is not valid. Available types are: {available_asset_types}.")
        
    return errors, df

# --- UI PAGES (Small changes to use SQLite logic) ---

def home_page():
    st.title("Home")
    st.header("Welcome to the App!")
    st.write("Select an option from the menu to get started.")

def data_points_page():
    st.title("Data Points")
    st.header("Data Points Management")

    with st.expander("Manage Asset Types"):
        with st.form("new_asset_type_form", clear_on_submit=True):
            new_asset_name = st.text_input("New Asset Type Name")
            submitted = st.form_submit_button("Add Asset Type")
            if submitted and new_asset_name:
                if add_asset_type(new_asset_name):
                    st.success(f"Asset type '{new_asset_name}' added successfully!")
                    st.rerun()
                else:
                    st.error(f"Asset type '{new_asset_name}' already exists (case-insensitive).")

    with st.expander("Bulk Add/Update via Text"):
        st.info("Paste data from a spreadsheet. Ensure the first row is a header with columns: name, identifiers, asset_types, data_type, range_min, range_max, string_options")
        pasted_data = st.text_area("Paste table data here", height=200)
        
        if st.button("Process Pasted Data"):
            if pasted_data:
                try:
                    data_io = io.StringIO(pasted_data)
                    df = pd.read_csv(data_io, sep='\t')
                    
                    errors, validated_df = validate_bulk_upload(df)
                    
                    if errors:
                        st.error("Validation failed. Please fix the following errors:")
                        for error in errors:
                            st.write(f"- {error}")
                    else:
                        st.success("Data validation successful! Processing records...")
                        existing_points_df, _ = get_all_data_points()
                        existing_names = [row['name'] for row in existing_points_df]

                        for i, row in validated_df.iterrows():
                            name = row['name']
                            identifiers = [iden.strip() for iden in str(row['identifiers']).split(',')]
                            asset_types = [atype.strip() for atype in str(row['asset_types']).split(',')]
                            data_type = row['data_type']
                            range_min = row.get('range_min') if pd.notna(row.get('range_min')) else None
                            range_max = row.get('range_max') if pd.notna(row.get('range_max')) else None
                            string_options = row.get('string_options') if pd.notna(row.get('string_options')) else None

                            if name in existing_names:
                                update_data_point_by_name(name, identifiers, asset_types, data_type, range_min, range_max, string_options)
                            else:
                                add_data_point(name, identifiers, asset_types, data_type, range_min, range_max, string_options)
                        
                        st.success("Bulk processing complete!")
                        st.rerun()

                except Exception as e:
                    st.error(f"An error occurred while parsing the data. Please ensure it is tab-separated. Error: {e}")
            else:
                st.warning("Please paste some data into the text area.")

    with st.expander("⚠️ Danger Zone"):
        st.warning("This will permanently delete all data points. This action cannot be undone.")
        if st.button("Delete All Data Points"):
            delete_all_data_points()
            st.success("All data points have been deleted.")
            st.rerun()

    asset_type_options = get_all_asset_types()

    if "show_add_form" not in st.session_state:
        st.session_state.show_add_form = False
    if "editing_dp_id" not in st.session_state:
        st.session_state.editing_dp_id = None

    if st.session_state.editing_dp_id is not None:
        st.subheader("Editing Data Point")
        dp_to_edit = get_data_point_by_id(st.session_state.editing_dp_id)
        
        data_type_options = ["float", "int", "boolean", "string"]
        data_type_index = data_type_options.index(dp_to_edit['data_type']) if dp_to_edit['data_type'] in data_type_options else 0
        
        data_type = st.selectbox("Data Type", data_type_options, index=data_type_index, key=f"edit_data_type_{dp_to_edit['id']}")

        with st.form("edit_data_point_form"):
            dp_name = st.text_input("Data Point Name", value=dp_to_edit['name'], disabled=True)
            dp_identifiers_str = st.text_input("Identifiers (comma-separated)", value=format_list_for_display(dp_to_edit['identifiers']))
            
            string_options_val = dp_to_edit.get('string_options', "") if 'string_options' in dp_to_edit.keys() else ""
            range_min, range_max, string_options = dp_to_edit.get('range_min'), dp_to_edit.get('range_max'), string_options_val
            
            if data_type in ['float', 'int']:
                col1, col2 = st.columns(2)
                with col1:
                    range_min = st.number_input("Range Min", value=float(range_min or 0.0), format="%.2f")
                with col2:
                    range_max = st.number_input("Range Max", value=float(range_max or 100.0), format="%.2f")
            elif data_type == 'string':
                string_options = st.text_input("String Options (comma-separated)", value=string_options or "")

            asset_types_default = json.loads(dp_to_edit['asset_types'] or '[]')
            asset_types = st.multiselect(
                "Asset Type(s)",
                asset_type_options,
                default=[atype for atype in asset_types_default if atype in asset_type_options]
            )

            col_submit, col_cancel = st.columns(2)
            with col_submit:
                submitted = st.form_submit_button("Update Data Point")
            with col_cancel:
                if st.form_submit_button("Cancel", use_container_width=True):
                    st.session_state.editing_dp_id = None
                    st.rerun()

            if submitted:
                identifiers = [identifier.strip() for identifier in dp_identifiers_str.split(',') if identifier.strip()]
                duplicate = check_identifier_uniqueness(identifiers, current_dp_id=st.session_state.editing_dp_id)
                if duplicate:
                    st.error(f"Identifier '{duplicate}' is already in use. Please choose a unique identifier.")
                else:
                    update_data_point(st.session_state.editing_dp_id, dp_to_edit['name'], identifiers, asset_types, data_type, range_min, range_max, string_options)
                    st.success("Data point updated successfully!")
                    st.session_state.editing_dp_id = None
                    st.rerun()

    elif st.session_state.show_add_form:
        st.subheader("Enter New Data Point Details")
        
        data_type = st.selectbox("Data Type", ["float", "int", "boolean", "string"], key="add_data_type")
        
        with st.form("new_data_point_form"):
            dp_name = st.text_input("Data Point Name", placeholder="e.g., Main Power Consumption")
            dp_identifiers_str = st.text_input("Identifiers (comma-separated)", placeholder="e.g., id-001, main-power")
            
            range_min, range_max, string_options = None, None, None
            if data_type in ['float', 'int']:
                col1, col2 = st.columns(2)
                with col1:
                    range_min = st.number_input("Range Min", value=0.0, format="%.2f")
                with col2:
                    range_max = st.number_input("Range Max", value=100.0, format="%.2f")
            elif data_type == 'string':
                string_options = st.text_input("String Options (comma-separated)", placeholder="e.g., ON, OFF, STANDBY")

            asset_types = st.multiselect("Asset Type(s)", asset_type_options)

            col_submit, col_cancel = st.columns(2)
            with col_submit:
                submitted = st.form_submit_button("Submit Data Point")
            with col_cancel:
                if st.form_submit_button("Cancel", use_container_width=True):
                    st.session_state.show_add_form = False
                    st.rerun()

            if submitted:
                identifiers = [identifier.strip() for identifier in dp_identifiers_str.split(',') if identifier.strip()]
                duplicate_identifier = check_identifier_uniqueness(identifiers)
                if not dp_name or not data_type or not asset_types:
                    st.warning("Please fill in all required fields.")
                elif get_data_point_by_name(dp_name):
                    st.error(f"Data point with name '{dp_name}' already exists.")
                elif duplicate_identifier:
                    st.error(f"Identifier '{duplicate_identifier}' is already in use. Please choose a unique identifier.")
                else:
                    add_data_point(dp_name, identifiers, asset_types, data_type, range_min, range_max, string_options)
                    st.success(f"Successfully added data point: {dp_name}")
                    st.session_state.show_add_form = False
                    st.rerun()

    else:
        if st.button("➕ Add New Data Point"):
            st.session_state.show_add_form = True
            st.rerun()

    st.divider()

    st.subheader("Existing Data Points")
    all_points, columns = get_all_data_points()

    if not all_points:
        st.info("No data points found. Click 'Add New Data Point' to get started.")
    else:
        header_cols = st.columns([3, 3, 3, 2, 2, 1])
        headers = ["Data Point Name", "Identifiers", "Asset Types", "Data Type", "Range/Options", "Actions"]
        for col, header in zip(header_cols, headers):
            col.markdown(f"**{header}**")
        
        for point in all_points:
            row_cols = st.columns([3, 3, 3, 2, 2, 1])
            row_cols[0].write(point['name'])
            row_cols[1].write(format_list_for_display(point['identifiers']))
            row_cols[2].write(format_list_for_display(point['asset_types']))
            row_cols[3].write(point['data_type'])
            
            if point['data_type'] in ['float', 'int']:
                range_str = f"{point.get('range_min', 'N/A')} - {point.get('range_max', 'N/A')}"
            elif point['data_type'] == 'string':
                range_str = point.get('string_options', 'N/A')
            else:
                range_str = 'N/A'
            row_cols[4].write(range_str)
            
            with row_cols[5]:
                if st.button("✏️", key=f"edit_{point['id']}", use_container_width=True):
                    st.session_state.editing_dp_id = point['id']
                    st.rerun()
            st.divider()


def generator_page():
    st.title("Sample Data Generator")
    st.header("Generate Mock JSON Data")

    asset_type_options = get_all_asset_types()
    if not asset_type_options:
        st.warning("No asset types found. Please add one on the 'Data points' page first.")
        return

    with st.form("generator_form"):
        st.subheader("Generation Parameters")
        selected_asset_type = st.selectbox("Select Asset Type", asset_type_options)
        pld_id = st.text_input("Enter a unique PLD ID", placeholder="e.g., PLD-001")

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", datetime.date.today())
        with col2:
            end_date = st.date_input("End Date", datetime.date.today())
        
        start_time = datetime.time(0, 0)
        end_time = datetime.time(23, 59)

        frequency_mins = st.number_input("Frequency of packets (in minutes)", min_value=1, max_value=60, value=5)

        submitted = st.form_submit_button("Generate Data")

    if submitted:
        if not pld_id:
            st.error("PLD ID is a required field.")
        elif start_date > end_date:
            st.error("End date must be after the start date.")
        else:
            st.info("Generating data... Please wait.")
            
            matching_dps = get_data_points_by_asset_type(selected_asset_type)
            
            if not matching_dps:
                st.warning(f"No data points found for asset type '{selected_asset_type}'. Please add them on the 'Data points' page.")
            else:
                start_datetime = datetime.datetime.combine(start_date, start_time)
                end_datetime = datetime.datetime.combine(end_date, end_time)

                all_packets = []
                current_time = start_datetime
                seq_id = 1
                while current_time <= end_datetime:
                    sii_data = {
                        "tmsp": format_timestamp(current_time),
                        "evc": 300,
                        "tms": format_timestamp(current_time)
                    }
                    for dp in matching_dps:
                        identifiers = json.loads(dp['identifiers'] or '[]')
                        key = (identifiers[0] if identifiers else dp['name'].replace(" ", "_").lower())
                        sii_data[key] = generate_mock_value(dp)

                    single_packet_json = {
                        "ver": "1.0",
                        "pld": pld_id,
                        "svc": "svc33338597",
                        "aid": "A173378384",
                        "eid": "2109801",
                        "dvt": "jvt1443",
                        "dvm": "JVT1443",
                        "evt": "EV",
                        "tms": format_timestamp(current_time),
                        "evc": "300",
                        "seqid": seq_id,
                        "alt": None,
                        "ext": [{"ver": "3.0", "sii": {"1": sii_data}}]
                    }
                    all_packets.append(single_packet_json)
                    
                    current_time += datetime.timedelta(minutes=frequency_mins)
                    seq_id += 1
                
                if not all_packets:
                    st.warning("No packets were generated for the selected time range.")
                else:
                    st.success(f"Successfully generated {len(all_packets)} packets!")
                    st.subheader("Generated JSON Preview (First Packet)")
                    st.json(all_packets[0])
                    json_string = json.dumps(all_packets, indent=4)
                    st.download_button(
                        label="Download JSON File",
                        file_name=f"{pld_id}_{selected_asset_type}_data.json",
                        mime="application/json",
                        data=json_string,
                    )

def multi_json_generator_page():
    st.title("Multi JSON Generator")
    st.header("Generate Mixed-Source Mock Data")

    asset_type_options = get_all_asset_types()
    if not asset_type_options:
        st.warning("No asset types found. Please add one on the 'Data points' page first.")
        return

    if 'pld_inputs' not in st.session_state:
        st.session_state.pld_inputs = {}

    with st.form("multi_generator_form"):
        st.subheader("Generation Parameters")
        
        selected_asset_types = st.multiselect("Select Asset Types", asset_type_options)
        
        for asset_type in selected_asset_types:
            if asset_type not in st.session_state.pld_inputs:
                st.session_state.pld_inputs[asset_type] = [""]

        for asset_type in selected_asset_types:
            with st.container(border=True):
                st.markdown(f"**PLDs for {asset_type}**")
                for i in range(len(st.session_state.pld_inputs[asset_type])):
                    st.session_state.pld_inputs[asset_type][i] = st.text_input(
                        f"PLD ID {i+1}", 
                        value=st.session_state.pld_inputs[asset_type][i], 
                        key=f"pld_{asset_type}_{i}"
                    )
                if st.form_submit_button(f"Add another PLD for {asset_type}", use_container_width=True):
                    st.session_state.pld_inputs[asset_type].append("")
                    st.rerun()
        
        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", datetime.date.today())
        with col2:
            end_date = st.date_input("End Date", datetime.date.today())
        
        submitted = st.form_submit_button("Generate Data", use_container_width=True)

    if submitted:
        all_packets = []
        has_errors = False
        
        start_datetime = datetime.datetime.combine(start_date, datetime.time.min)
        end_datetime = datetime.datetime.combine(end_date, datetime.time.max)
        total_seconds = (end_datetime - start_datetime).total_seconds()

        for asset_type, plds in st.session_state.pld_inputs.items():
            if asset_type not in selected_asset_types:
                continue

            cleaned_plds = [pld.strip() for pld in plds if pld.strip()]
            if not cleaned_plds:
                st.error(f"Please provide at least one valid PLD ID for {asset_type}.")
                has_errors = True
                continue
            
            matching_dps = get_data_points_by_asset_type(asset_type)
            if not matching_dps:
                st.warning(f"No data points found for asset type '{asset_type}'. Skipping.")
                continue

            for pld_id in cleaned_plds:
                num_packets = random.randint(5, 20) 
                for _ in range(num_packets):
                    random_seconds = random.uniform(0, total_seconds)
                    random_timestamp = start_datetime + datetime.timedelta(seconds=random_seconds)
                    
                    parameters = {}
                    for dp in matching_dps:
                        identifiers = json.loads(dp['identifiers'] or '[]')
                        key = (identifiers[0] if identifiers else dp['name'].replace(" ", "_").lower())
                        parameters[key] = generate_mock_value(dp)

                    packet = {
                        "pld": pld_id,
                        "asset_type": asset_type,
                        "timestamp": format_timestamp(random_timestamp),
                        "parameters": parameters
                    }
                    all_packets.append(packet)

        if not has_errors and all_packets:
            random.shuffle(all_packets)
            
            st.success(f"Successfully generated {len(all_packets)} mixed packets!")
            st.subheader("Generated JSON Preview (First Packet)")
            st.json(all_packets[0])
            
            json_string = json.dumps(all_packets, indent=4)
            st.download_button(
                label="Download JSON File",
                file_name="multi_asset_data.json",
                mime="application/json",
                data=json_string,
            )
        elif not has_errors:
            st.warning("No packets were generated. Check if the selected asset types have data points.")


def main():
    """This is the main function for the Streamlit app."""
    with st.sidebar:
        menu_selection = option_menu(
            "Main Menu", 
            ["Home", 'Data points', 'Generator', 'Multi JSON Generator'], 
            icons=['house', 'database-add', 'file-binary', 'files'], 
            menu_icon="cast", 
            default_index=1 
        )

    if menu_selection == "Home":
        home_page()
    elif menu_selection == "Data points":
        data_points_page()
    elif menu_selection == "Generator":
        generator_page()
    elif menu_selection == "Multi JSON Generator":
        multi_json_generator_page()

if __name__ == '__main__':
    # Initialize the local database on startup
    init_db()
    main()

