import streamlit as st
import datetime
import json
import random
from database import get_all_asset_types, get_data_points_by_asset_type
from utils import format_timestamp, generate_mock_value

def multi_json_generator_page():
    """This function defines the content for the 'Multi JSON Generator' page."""
    st.title("Multi JSON Generator")
    st.header("Generate Mixed-Source Mock Data")

    asset_type_options = get_all_asset_types()
    if not asset_type_options:
        st.warning("No asset types found. Please add one on the 'Data points' page first.")
        return

    # Use session state to manage PLD inputs and frequencies
    if 'pld_inputs' not in st.session_state:
        st.session_state.pld_inputs = {}
    if 'pld_frequencies' not in st.session_state:
        st.session_state.pld_frequencies = {}

    with st.form("multi_generator_form"):
        st.subheader("Generation Parameters")

        selected_asset_types = st.multiselect("Select Asset Types", asset_type_options)

        for asset_type in selected_asset_types:
            if asset_type not in st.session_state.pld_inputs:
                st.session_state.pld_inputs[asset_type] = [""] # Start with one input field
            if asset_type not in st.session_state.pld_frequencies:
                st.session_state.pld_frequencies[asset_type] = 60 # Default frequency

        for asset_type in selected_asset_types:
            with st.container(border=True):
                st.markdown(f"**{asset_type} Configuration**")

                st.session_state.pld_frequencies[asset_type] = st.number_input(
                    "Frequency (minutes)",
                    min_value=1,
                    value=st.session_state.pld_frequencies[asset_type],
                    key=f"freq_{asset_type}"
                )

                st.markdown(f"**PLD IDs for {asset_type}**")
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

        for asset_type in selected_asset_types:
            plds = st.session_state.pld_inputs.get(asset_type, [])
            cleaned_plds = [pld.strip() for pld in plds if pld.strip()]

            if not cleaned_plds:
                st.error(f"Please provide at least one valid PLD ID for {asset_type}.")
                has_errors = True
                continue

            matching_dps = get_data_points_by_asset_type(asset_type)
            if not matching_dps:
                st.warning(f"No data points found for asset type '{asset_type}'. Skipping.")
                continue

            frequency_minutes = st.session_state.pld_frequencies.get(asset_type, 60)
            if frequency_minutes <= 0:
                st.error(f"Frequency for {asset_type} must be a positive number.")
                has_errors = True
                continue

            interval_seconds = frequency_minutes * 60
            if total_seconds < interval_seconds:
                # If the time range is smaller than the frequency, generate at least one packet
                num_packets = 1
            else:
                num_packets = int(total_seconds / interval_seconds)

            for pld_id in cleaned_plds:
                for i in range(num_packets):
                    # Distribute timestamps evenly
                    current_timestamp = start_datetime + datetime.timedelta(seconds=i * interval_seconds)

                    parameters = {}
                    for dp in matching_dps:
                        key = (json.loads(dp['identifiers'])[0] if dp['identifiers'] and json.loads(dp['identifiers']) else dp['name'].replace(" ", "_").lower())
                        parameters[key] = generate_mock_value(dp)

                    packet = {
                        "pld": pld_id,
                        "asset_type": asset_type,
                        "timestamp": format_timestamp(current_timestamp),
                        "parameters": parameters
                    }
                    all_packets.append(packet)

        if not has_errors and all_packets:
            # Sort the packets by timestamp to create a chronological stream
            all_packets.sort(key=lambda p: p['timestamp'])

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
