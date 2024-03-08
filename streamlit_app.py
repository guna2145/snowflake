
import snowflake.snowpark.functions
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import streamlit as st
from streamlit_extras.stateful_button import button
from streamlit_extras.switch_page_button import switch_page
from streamlit_extras.dataframe_explorer import dataframe_explorer


connection_parameters = {"account":"qgonqbx-ju39310",
"user":"GUNA2145",
"password": "jyzqup-suwcig-0Wujma",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"MANAGE_DB",
"schema":"EXTERNAL_STAGES"
}

#Create Snowflake Session
session = Session.builder.configs(connection_parameters).create()

#Streamlit App Title

st.set_page_config(layout="wide")
st.title("Data Management Application")

#Get Current Snowflake details
#print(session.sql("select current_warehouse(), current_database(), current_schema()").collect())


############################ Data Validation & Ingestion ##################################
def action_on_data():
    #Tab creation for Data Profiling and Ingestion
    tab2, tab3 = st.tabs(["Data Profiling","Data Ingestion"])
   
    #Data Profiling Tab
    with tab2:
        profile_select = st.multiselect("Select Column for profiling",["1","2"])
        st.button("Profile Data",use_container_width=True)
        
    #Data Ingestion Tab
    with tab3:
        ingestion_select = st.multiselect("Select Column for Ingestion",["1","2"])
        st.button("Ingest Data",use_container_width=True)


#1. List Stage from Snowflake
#2. Select the file from list
#3. Preview the selected file from the list
#4. Select a column profile the data
#5. Select rows and ingest the data

############################ Select Stage & Files List ##################################

def list_stage():
    df = session.sql("show stages").collect()

    stage_name = []

    for stage in df:
        if stage['type'] != 'INTERNAL':
            stage_name.append(stage['name'])

    stage_name = st.selectbox("SELECT STAGE", stage_name)
    date_format = st.radio("Select Date",["DATE RANGE","MULTISELECT"], horizontal=True, label_visibility="collapsed")

    if date_format == 'DATE RANGE':
        col1, col2 = st.columns(2)
        col1.date_input("START DATE")
        col2.date_input("END DATE")
    else:
        st.multiselect("SELECT DATE",[str(i['created_on']).split(' ')[0] for i in df])

    if stage_name:
        file_list = pd.DataFrame(session.sql(f"LIST @{stage_name}").collect())
        init_value = False
        df_with_selections = file_list.copy()
        df_with_selections.insert(0, "Select", init_value)

        # Get dataframe row-selections from user with st.data_editor
        edited_df = st.data_editor(
            df_with_selections,
            hide_index=True,
            column_config={"Select": st.column_config.CheckboxColumn(required=True)},
            disabled=file_list.columns,
            width=2000
        )
        
        # Filter the dataframe using the temporary column, then drop the column
        selected_rows = edited_df[edited_df.Select]
        filename = selected_rows['name'].to_string().split("/")[-1]

    if button("Preview/Reset",key="preview",use_container_width=True):
        #switch_page("data_validation")
        #preview = pd.DataFrame(session.sql('select * from people limit 100').collect())
        #preview = pd.DataFrame(session.file.get_stream("@MANAGE_DB.EXTERNAL_STAGES.HACKATHON_GUNA_S3/people-1000.csv", decompress=False))
        # Read a csv file with header and parse the header
        df_file = pd.DataFrame(session.read.option("INFER_SCHEMA", True).option("PARSE_HEADER", True).csv(f"@{stage_name}/{filename}").collect())
        

        df_file_selections = df_file.copy()
        df_file_selections.insert(0, "Select", init_value)

        # Get dataframe row-selections from user with st.data_editor


        edited1_df = st.data_editor(
            df_file_selections,
            hide_index=True,
            column_config={"Select": st.column_config.CheckboxColumn(required=True)},
            disabled=df_file.columns,
            width=2000,
            key='edited1_df'
        )

        if button("Edit Data",key="editData",use_container_width=True):
            st.write(st.session_state.edited1_df)
        if button("Ingest Data",key="ingestdata",use_container_width=True):
            st.write("test")

 
        selected_rows_data = edited1_df[edited1_df.Select]
        print(selected_rows_data)

        #filtered_df = dataframe_explorer(edited1_df, case=False)

        #st.dataframe(edited1_df, use_container_width=True)

list_stage()