import io
import openpyxl
from openpyxl.styles import Font
import pandas as pd
from database import get_all_asset_types

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
