import json
import datetime
import random

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
