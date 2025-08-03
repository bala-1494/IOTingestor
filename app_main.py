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

# --- DATABASE SETUP ---

def get_db_connection():
    """Establishes a connection to the SQLite database."""
    conn = sqlite3.connect('data_points.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initializes the database and creates the tables if they don't exist."""
    conn = get_db_connection()
    # Create data_points table
    conn.execute('''
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
    # Create asset_types table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS asset_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
    ''')
    # Check if the asset_types table is empty and populate it with defaults
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM asset_types")
    if cursor.fetchone()[0] == 0:
        default_types = ["DG", "HVAC", "SOLAR Inverter", "Sub-Meter", "Temp Sensor", "Hum Sensor"]
        for asset_type in default_types:
            conn.execute("INSERT INTO asset_types (name) VALUES (?)", (asset_type,))
    
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

# --- HELPER FUNCTIONS ---
def format_json_list_for_display(json_string):
    """
    Safely loads a JSON string that is expected to be a list and returns 
    a comma-separated string for display. Returns an empty string on failure.
    """
    if not json_string:
        return ""
    try:
        items = json.loads(json_string)
        if isinstance(items, list):
            return ", ".join(map(str, items))
        return ""
    except (json.JSONDecodeError, TypeError):
        return ""

def generate_mock_value(dp):
    """Generates a random value based on the data point's type and range."""
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
        return "Sample String" # Fallback
    return None

def format_timestamp(dt_object):
    """Formats a datetime object to 'YYYY-MM-DDTHH:MM:SS+0530'."""
    # Using a fixed timezone for consistency as Streamlit server might be in UTC
    tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    dt_aware = dt_object.replace(tzinfo=tz)
    # Format to ISO 8601 with timezone
    iso_str = dt_aware.strftime('%Y-%m-%dT%H:%M:%S%z')
    return iso_str


# --- BULK UPLOAD FUNCTIONS ---
def create_sample_excel():
    """Creates an in-memory Excel file with sample data for bulk upload."""
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "DataPoints"
    
    headers = ["name", "identifiers", "asset_types", "data_type", "range_min", "range_max", "string_options"]
    sheet.append(headers)
    
    # Style headers
    for cell in sheet[1]:
        cell.font = Font(bold=True)
        
    # Add sample data
    sample_data = [
        "Building Power", "bldg_pwr, main_kw", "Sub-Meter, HVAC", "float", 0, 5000, ""
    ]
    sheet.append(sample_data)
    
    # Save to a virtual file
    virtual_workbook = io.BytesIO()
    workbook.save(virtual_workbook)
    virtual_workbook.seek(0)
    return virtual_workbook.getvalue()

def validate_bulk_upload(df):
    """Validates the DataFrame from the uploaded Excel file."""
    errors = []
    required_columns = ["name", "identifiers", "asset_types", "data_type"]
    valid_data_types = ["float", "int", "boolean", "string"]
    available_asset_types = get_all_asset_types()
    
    for col in required_columns:
        if col not in df.columns:
            errors.append(f"Missing required column: '{col}'")
            return errors, None # Stop validation if columns are missing

    # Check for empty required fields
    for i, row in df.iterrows():
        for col in required_columns:
            if pd.isna(row[col]):
                errors.append(f"Row {i+2}: Missing value in required column '{col}'.")
        
        # Validate data_type
        if row['data_type'] not in valid_data_types:
            errors.append(f"Row {i+2}: Invalid data_type '{row['data_type']}'. Must be one of {valid_data_types}.")
            
        # Validate asset_types
        asset_types = [atype.strip() for atype in str(row['asset_types']).split(',')]
        for atype in asset_types:
            if atype not in available_asset_types:
                errors.append(f"Row {i+2}: Asset type '{atype}' is not valid. Available types are: {available_asset_types}.")

    return errors, df


# --- UI PAGES ---

def home_page():
    """This function defines the content for the 'Home' page."""
    st.title("Home")
    st.header("Welcome to the App!")
    st.write("Select an option from the menu to get started.")

def data_points_page():
    """
    This function defines the content for the 'Data points' page.
    It includes a table of existing points and forms to add or edit them.
    """
    st.title("Data Points")
    st.header("Data Points Management")

    # --- MANAGE ASSET TYPES ---
    with st.expander("Manage Asset Types"):
        with st.form("new_asset_type_form", clear_on_submit=True):
            new_asset_name = st.text_input("New Asset Type Name")
            submitted = st.form_submit_button("Add Asset Type")
            if submitted and new_asset_name:
                if add_asset_type(new_asset_name):
                    st.success(f"Asset type '{new_asset_name}' added successfully!")
                    st.rerun() # Rerun to update dropdowns
                else:
                    st.error(f"Asset type '{new_asset_name}' already exists (case-insensitive).")

    # --- BULK UPLOAD ---
    with st.expander("Bulk Add/Update Data Points"):
        st.download_button(
            label="Download Sample .xlsx File",
            data=create_sample_excel(),
            file_name="sample_data_points.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
        uploaded_file = st.file_uploader("Upload an Excel file", type=["xlsx"])
        
        if uploaded_file is not None:
            try:
                df = pd.read_excel(uploaded_file)
                errors, validated_df = validate_bulk_upload(df)
                
                if errors:
                    st.error("Validation failed. Please fix the following errors in your file:")
                    for error in errors:
                        st.write(f"- {error}")
                else:
                    st.success("File validation successful! Processing records...")
                    # Get existing data point names for update/add logic
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
                            # Update existing data point
                            update_data_point_by_name(name, identifiers, asset_types, data_type, range_min, range_max, string_options)
                        else:
                            # Add new data point
                            add_data_point(name, identifiers, asset_types, data_type, range_min, range_max, string_options)
                    
                    st.success("Bulk upload complete!")
                    st.rerun()

            except Exception as e:
                st.error(f"An error occurred while processing the file: {e}")

    # --- DANGER ZONE FOR DELETION ---
    with st.expander("⚠️ Danger Zone"):
        st.warning("This will permanently delete all data points. This action cannot be undone.")
        if st.button("Delete All Data Points"):
            delete_all_data_points()
            st.success("All data points have been deleted.")
            st.rerun()


    # Fetch asset types dynamically for dropdowns
    asset_type_options = get_all_asset_types()

    # --- STATE MANAGEMENT ---
    if "show_add_form" not in st.session_state:
        st.session_state.show_add_form = False
    if "editing_dp_id" not in st.session_state:
        st.session_state.editing_dp_id = None

    # --- ADD/EDIT FORMS ---
    if st.session_state.editing_dp_id is not None:
        # --- EDIT FORM ---
        dp_to_edit = get_data_point_by_id(st.session_state.editing_dp_id)
        with st.form("edit_data_point_form"):
            st.subheader(f"Editing Data Point: {dp_to_edit['name']}")
            
            dp_name = st.text_input("Data Point Name", value=dp_to_edit['name'], disabled=True) # Name is the key, so disable editing
            dp_identifiers_str = st.text_input("Identifiers (comma-separated)", value=format_json_list_for_display(dp_to_edit['identifiers']))
            data_type_options = ["float", "int", "boolean", "string"]
            data_type_index = data_type_options.index(dp_to_edit['data_type']) if dp_to_edit['data_type'] in data_type_options else 0
            data_type = st.selectbox("Data Type", data_type_options, index=data_type_index)
            
            string_options_val = dp_to_edit['string_options'] if 'string_options' in dp_to_edit.keys() else ""
            range_min, range_max, string_options = dp_to_edit['range_min'], dp_to_edit['range_max'], string_options_val
            
            if data_type in ['float', 'int']:
                col1, col2 = st.columns(2)
                with col1:
                    range_min = st.number_input("Range Min", value=float(range_min or 0.0), format="%.2f")
                with col2:
                    range_max = st.number_input("Range Max", value=float(range_max or 100.0), format="%.2f")
            elif data_type == 'string':
                string_options = st.text_input("String Options (comma-separated)", value=string_options or "")

            asset_types = st.multiselect(
                "Asset Type(s)",
                asset_type_options, # Use dynamic list
                default=[atype for atype in json.loads(dp_to_edit['asset_types'] or '[]') if atype in asset_type_options]
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
                elif not dp_name or not data_type or not asset_types:
                    st.warning("Please fill in all required fields.")
                else:
                    # Use the correct update function that takes an ID
                    update_data_point(st.session_state.editing_dp_id, dp_to_edit['name'], identifiers, asset_types, data_type, range_min, range_max, string_options)
                    st.success("Data point updated successfully!")
                    st.session_state.editing_dp_id = None
                    st.rerun()

    elif st.session_state.show_add_form:
        # --- ADD FORM ---
        with st.form("new_data_point_form"):
            st.subheader("Enter New Data Point Details")
            dp_name = st.text_input("Data Point Name", placeholder="e.g., Main Power Consumption")
            dp_identifiers_str = st.text_input("Identifiers (comma-separated)", placeholder="e.g., id-001, main-power")
            data_type = st.selectbox("Data Type", ["float", "int", "boolean", "string"])
            
            range_min, range_max, string_options = None, None, None
            if data_type in ['float', 'int']:
                col1, col2 = st.columns(2)
                with col1:
                    range_min = st.number_input("Range Min", value=0.0, format="%.2f")
                with col2:
                    range_max = st.number_input("Range Max", value=100.0, format="%.2f")
            elif data_type == 'string':
                string_options = st.text_input("String Options (comma-separated)", placeholder="e.g., ON, OFF, STANDBY")


            asset_types = st.multiselect("Asset Type(s)", asset_type_options) # Use dynamic list

            col_submit, col_cancel = st.columns(2)
            with col_submit:
                submitted = st.form_submit_button("Submit Data Point")
            with col_cancel:
                if st.form_submit_button("Cancel", use_container_width=True):
                    st.session_state.show_add_form = False
                    st.rerun()

            if submitted:
                identifiers = [identifier.strip() for identifier in dp_identifiers_str.split(',') if identifier.strip()]
                duplicate = check_identifier_uniqueness(identifiers)
                if duplicate:
                    st.error(f"Identifier '{duplicate}' is already in use. Please choose a unique identifier.")
                elif not dp_name or not data_type or not asset_types:
                    st.warning("Please fill in all required fields.")
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

    # --- DISPLAY EXISTING DATA POINTS ---
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
            row_cols[1].write(format_json_list_for_display(point['identifiers']))
            row_cols[2].write(format_json_list_for_display(point['asset_types']))
            row_cols[3].write(point['data_type'])
            
            if point['data_type'] in ['float', 'int']:
                range_str = f"{point['range_min']} - {point['range_max']}"
            elif point['data_type'] == 'string':
                # Safely access 'string_options' to prevent KeyError
                range_str = point['string_options'] if 'string_options' in point.keys() and point['string_options'] else 'N/A'
            else:
                range_str = 'N/A'
            row_cols[4].write(range_str)
            
            with row_cols[5]:
                if st.button("✏️", key=f"edit_{point['id']}", use_container_width=True):
                    st.session_state.editing_dp_id = point['id']
                    st.rerun()
            st.divider()


def generator_page():
    """This function defines the content for the 'Generator' page."""
    st.title("Sample Data Generator")
    st.header("Generate Mock JSON Data")

    # Fetch asset types dynamically
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
                        key = (json.loads(dp['identifiers'])[0] if dp['identifiers'] and json.loads(dp['identifiers']) else dp['name'].replace(" ", "_").lower())
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

def main():
    """This is the main function for the Streamlit app."""
    with st.sidebar:
        menu_selection = option_menu(
            "Main Menu", 
            ["Home", 'Data points', 'Generator'], 
            icons=['house', 'database-add', 'file-binary'], 
            menu_icon="cast", 
            default_index=1 
        )

    if menu_selection == "Home":
        home_page()
    elif menu_selection == "Data points":
        data_points_page()
    elif menu_selection == "Generator":
        generator_page()

if __name__ == '__main__':
    init_db()
    main()
