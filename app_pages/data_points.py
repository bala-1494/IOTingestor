import streamlit as st
import pandas as pd
import json
import logging
from database import (
    add_asset_type,
    get_all_asset_types,
    add_data_point,
    update_data_point,
    update_data_point_by_name,
    get_all_data_points,
    get_data_point_by_id,
    get_data_point_by_name,
    delete_all_data_points,
    check_identifier_uniqueness,
)
from utils import format_json_list_for_display
from bulk_upload import create_sample_excel, validate_bulk_upload

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="bulk_upload.log",
    filemode="w",
)

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
            logging.info(f"Uploaded file received: {uploaded_file.name}")
            try:
                df = pd.read_excel(uploaded_file)
                logging.info("Successfully read excel file into DataFrame.")
                errors, validated_df = validate_bulk_upload(df)

                if errors:
                    logging.error(f"Validation failed with errors: {errors}")
                    st.error("Validation failed. Please fix the following errors in your file:")
                    for error in errors:
                        st.write(f"- {error}")
                else:
                    st.success("File validation successful! Processing records...")
                    logging.info("File validation successful. Processing records...")
                    # Get existing data point names for update/add logic
                    existing_points, columns = get_all_data_points()
                    existing_points_df = pd.DataFrame(existing_points, columns=columns)
                    existing_names = []
                    if not existing_points_df.empty:
                        existing_names = existing_points_df['name'].tolist()
                    logging.info(f"Found {len(existing_names)} existing data points.")

                    for i, row in validated_df.iterrows():
                        name = row['name']
                        logging.info(f"Processing row {i+2}: {name}")
                        identifiers = [iden.strip() for iden in str(row['identifiers']).split(',')]
                        asset_types = [atype.strip() for atype in str(row['asset_types']).split(',')]
                        data_type = row['data_type']
                        range_min = row.get('range_min') if pd.notna(row.get('range_min')) else None
                        range_max = row.get('range_max') if pd.notna(row.get('range_max')) else None
                        string_options = row.get('string_options') if pd.notna(row.get('string_options')) else None

                        if name in existing_names:
                            logging.info(f"Updating existing data point: {name}")
                            # Update existing data point
                            update_data_point_by_name(name, identifiers, asset_types, data_type, range_min, range_max, string_options)
                        else:
                            logging.info(f"Adding new data point: {name}")
                            # Add new data point
                            add_data_point(name, identifiers, asset_types, data_type, range_min, range_max, string_options)

                    st.success("Bulk upload complete!")
                    logging.info("Bulk upload complete!")
                    st.rerun()

            except Exception as e:
                logging.error(f"An error occurred while processing the file: {e}", exc_info=True)
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
        st.subheader("Editing Data Point")
        dp_to_edit = get_data_point_by_id(st.session_state.editing_dp_id)

        data_type_options = ["float", "int", "boolean", "string"]
        data_type_index = data_type_options.index(dp_to_edit['data_type']) if dp_to_edit['data_type'] in data_type_options else 0

        # Selectbox is now outside the form to allow dynamic updates
        data_type = st.selectbox("Data Type", data_type_options, index=data_type_index, key=f"edit_data_type_{dp_to_edit['id']}")

        with st.form("edit_data_point_form"):
            dp_name = st.text_input("Data Point Name", value=dp_to_edit['name'], disabled=True)
            dp_identifiers_str = st.text_input("Identifiers (comma-separated)", value=format_json_list_for_display(dp_to_edit['identifiers']))

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
                asset_type_options,
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
                else:
                    update_data_point(st.session_state.editing_dp_id, dp_to_edit['name'], identifiers, asset_types, data_type, range_min, range_max, string_options)
                    st.success("Data point updated successfully!")
                    st.session_state.editing_dp_id = None
                    st.rerun()

    elif st.session_state.show_add_form:
        # --- ADD FORM ---
        st.subheader("Enter New Data Point Details")

        # Selectbox is now outside the form to allow dynamic updates
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
                existing_dp = get_data_point_by_name(dp_name)

                if not dp_name or not data_type or not asset_types:
                    st.warning("Please fill in all required fields.")
                elif existing_dp:
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
