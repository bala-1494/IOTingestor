import streamlit as st
from streamlit_option_menu import option_menu

from database import init_db
from pages.home import home_page
from pages.data_points import data_points_page
from pages.generator import generator_page
from pages.multi_json_generator import multi_json_generator_page

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
    init_db()
    main()
