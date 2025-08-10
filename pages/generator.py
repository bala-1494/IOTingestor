import streamlit as st
import datetime
import json
from database import get_all_asset_types, get_data_points_by_asset_type
from utils import format_timestamp, generate_mock_value

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
