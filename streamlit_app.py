
import snowflake.snowpark.functions
import datetime
import time
import re
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import streamlit as st
from streamlit_extras.stateful_button import button
from streamlit_extras.switch_page_button import switch_page
from streamlit_extras.dataframe_explorer import dataframe_explorer
from streamlit_pandas_profiling import st_profile_report
from ydata_profiling import ProfileReport


connection_parameters = {"account":"XXXXXX",
"user":"XXXXXXX",
"password": "XXXXXXXXX",
"role":"ACCOUNTADMIN",
"warehouse":"XXXXXX",
"database":"MANAGE_DB",
"schema":"EXTERNAL_STAGES"
}

#Create Snowflake Session
session = Session.builder.configs(connection_parameters).create()

#Streamlit App Title

st.set_page_config(layout="wide")
st.title("\t\tData Management Application")

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

    if button("Preview & Reset",key="preview",use_container_width=True):
        load_start = datetime.datetime.now()

        with st.spinner('Loading the dataframe, please wait....'):
            df_file = pd.DataFrame(session.read.option("INFER_SCHEMA", True).option("PARSE_HEADER", True).csv(f"@{stage_name}/{filename}").collect())
        load_end = datetime.datetime.now()
        st.success(f"Dataframe Load Time : {load_end-load_start}")
       
        df_file_selections = df_file.copy()
        df_file_selections.insert(0, "Select", init_value)

    
        #Dataframe_explorer code for Filtering the Dataframe


        # Get dataframe row-selections from user with st.data_editor
        edited_df_view = st.data_editor(
            dataframe_explorer(df_file_selections),
            hide_index=True,
            column_config={"Select": st.column_config.CheckboxColumn(required=True)},
            disabled=False,
            width=2000,
            key='edited_df_view'
        )

        st.warning("\tDouble click a cell to edit the data and click 'Ingest Data' to ingest to the DB. Please note the change will not be saved to the actual file in Amazon S3",icon="🚨")

        if st.button("Ingest Data",key="ingestdata",use_container_width=True):
            table_name_split = filename.split(".")[0]
            table_name = re.sub(r"[^a-zA-Z0-9 ]", "_", table_name_split)
            selected_data = session.create_dataframe(edited_df_view[edited_df_view.Select])
            selected_data.write.mode("append").save_as_table(table_name)
            st.info(f"Below records are ingested to the Database {table_name.upper()}")
            st.table(selected_data)

        with st.expander("Data Profiling"):
            profie_data_pr = pd.read_csv("people-1000.csv")
            pr = ProfileReport(profie_data_pr, title="Report",dark_mode=True)
            st_profile_report(pr)

list_stage()