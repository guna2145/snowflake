# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.session import Session
import pandas as pd
import re
import humanize
from st_aggrid import AgGrid, JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from streamlit_pandas_profiling import st_profile_report
from ydata_profiling import ProfileReport

st.set_page_config(
     page_title="Simple Data Management Application",
     page_icon="🧊",
     layout="wide",
     initial_sidebar_state="expanded",
 )

# Get the current credentials
# session = get_active_session()

connection_parameters = {
    "account": "QGONQBX-JU39310",
    "user": "PRAVEEN",
    "password": "Prawin@276",
    "warehouse": "COMPUTE_WH",
    "role": "ACCOUNTADMIN",
    "database": "MANAGE_DB",
    "schema": "EXTERNAL_STAGES"
}

session = Session.builder.configs(connection_parameters).create()
# file_bytes = session.file.get_stream("@MANAGE_DB.EXTERNAL_STAGES.HACKATHONS3/people-500000.csv", decompress=False)
# #file_bytes.seek(0)
# df = pd.read_csv(file_bytes)
# st.dataframe(df.head(20))

def show_file(name):
    st.session_state['file_name'] = name


# st.write(session.sql('select current_account(), current_warehouse(), current_database(), current_schema()').collect())
stages = session.sql("show stages in MANAGE_DB.EXTERNAL_STAGES").collect()
stages_list = [x["name"] for x in stages if x["type"] != 'INTERNAL']
with st.sidebar:
    st.title("Simple Data Management Application")
    stage_name = st.selectbox("Please choose the stage", stages_list, index=None, placeholder="Please choose the stage")
    if stage_name:
        files = session.sql(f"LIST @MANAGE_DB.EXTERNAL_STAGES.{stage_name}").collect()
        if files:
            files_df = pd.DataFrame(files)
            files_df['full_name'] = files_df['name']
            files_df = files_df[['name', 'full_name', 'size', 'last_modified']]
            files_df['name'] = files_df['name'].apply(lambda x: x.split('/')[-1])

            st.subheader("Files:")
            # for i in range(0, 2):
            for i in range(len(files_df)):
                file = files_df.iloc[i]
                with st.expander(file['name']):
                    file_size = humanize.naturalsize(file['size'])
                    f_in = file['full_name'].rindex(file['name'])
                    st.caption(f"Path: :blue[{file['full_name'][:f_in]}]")
                    st.caption(f"Size: :blue[{file_size}]")
                    st.caption(f"Modified on: :blue[{file['last_modified']}]")
                    st.button("Show content", key=i, on_click=show_file, args=[file['name']])
    else:
        st.session_state['file_name'] = ""

if st.session_state.get('file_name'):
    with st.spinner("Getting data..."):
        filename = st.session_state.get('file_name')
        df_file = pd.DataFrame(session.read.option("INFER_SCHEMA", True).option("PARSE_HEADER", True).csv(f"@{stage_name}/{filename}").collect())
        gd = GridOptionsBuilder.from_dataframe(df_file)
        gd.configure_default_column(editable=True)
        gd.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=100)
        gd.configure_selection(selection_mode='multiple', use_checkbox=True)
        gd.configure_grid_options(pre_selected_rows=[])
        gridoptions = gd.build()

        return_value = AgGrid(
            df_file, 
            gridOptions = gridoptions, 
            allow_unsafe_jscode = True,
            theme = 'balham',
            # height = 200,
            fit_columns_on_grid_load = True
        )
        selected_rows = return_value["selected_rows"]
        if selected_rows:
            if st.button("Ingest Data",key="ingestdata",use_container_width=True):
                table_name_split = filename.split(".")[0]
                table_name = re.sub(r"[^a-zA-Z0-9 ]", "_", table_name_split)
                selected_data = session.create_dataframe(selected_rows)
                selected_data = selected_data.drop(selected_data.columns[0])
                selected_data.write.mode("append").save_as_table(table_name)
                st.info(f"Total number of records ingested to the {table_name.upper()} table: {len(selected_rows)}")
        

    with st.expander("Data Analysis"):
        col1, col2 = st.columns(2)
        with col1:
            options = st.multiselect(
                label="",
                options=df_file.columns.values,
                on_change = None,
                placeholder = "Please choose the columns..")
            if options:
                wt.write(options)
                profie_data = df_file.copy()
                profie_data = profie_data[options]
                pr = profie_data.profile_report()
                export=pr.to_html()
                with col2:
                    st.download_button(label="Download Full Report", data=export, file_name='report.html')

            # st_profile_report(pr)
    # st.write(f"@MANAGE_DB.EXTERNAL_STAGES.{stage_name}/{st.session_state.get('file_name')}")
    # exit()
    # file_bytes = session.file.get_stream("@MANAGE_DB.EXTERNAL_STAGES.HACKATHONS3/people-500000.csv", decompress=False)
    #file_bytes.seek(0)
    # df = pd.read_csv(file_bytes)
    # AgGrid(df)
    
