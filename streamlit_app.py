import os
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

session = Session.builder.configs(connection_parameters).create()
st.title("Data Management Application")

#print(session.sql("select current_warehouse(), current_database(), current_schema()").collect())
df = session.sql("show stages").collect()


############################ Select Stage & Files List ##################################
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

############################ Data Validation & Ingestion ##################################
def data_action():
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
    
if stage_name:
    file_list = st.write(session.sql(f"LIST @{stage_name}").collect())

    if button("Preview/Reset",key="preview",use_container_width=True):
        #switch_page("data_validation")
        #preview = pd.DataFrame(session.sql('select * from people limit 100').collect())
        #preview = pd.DataFrame(session.file.get_stream("@MANAGE_DB.EXTERNAL_STAGES.HACKATHON_GUNA_S3/people-1000.csv", decompress=False))
        # Read a csv file with header and parse the header
        df_file = pd.DataFrame(session.read.option("INFER_SCHEMA", True).option("PARSE_HEADER", True).csv("@hackathon_guna_s3/people-1000.csv").collect())
        filtered_df = dataframe_explorer(df_file, case=False)
        filtered_df_index = dataframe_explorer(df_file, case=False).index()
        print(filtered_df_index)
        st.dataframe(filtered_df, use_container_width=True)
        data_action()
    