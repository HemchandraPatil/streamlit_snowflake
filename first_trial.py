# Import python packages
from snowflake.snowpark import Session
import pandas as pd
import requests
import snowflake.connector
from urllib.error import URLError
import streamlit as st
from snowflake.snowpark.context import get_active_session

def create_session():
    return Session.builder.configs(st.secrets["snowflake"]).create()

session = create_session()

# Write directly to the app
st.title("Welcome to Streamlit in Snowflake")
st.header("Saama Thunder's") 
status = st.radio("Select One of the Stage: ", ('Internal Satge', 'External Stage(S3)','Named Stage'))

# conditional statement to print 
if (status == 'Internal Satge'):
    st.success("You have selected Internal Stage")
elif (status=='External Stage(S3)'):
    st.success("You have selected External Stage(S3)")
elif(status=='Named Stage'):
    st.success("You have selected Named Stage")
else:
    st.info("You haven't selected anything")

#con=st.connection("snowflake")
#df=con.query("select 1 from dual")
if (status == 'Internal Satge'):
#   session = get_active_session()
    st.subheader("There are no files in Internal Stage")

if (status == 'Named Stage'):
#   session = get_active_session()
    st.subheader("There are no files in Named Stage")

if(status=='External Stage(S3)'):
    #session = get_active_session()

    st.subheader("External Stage Files:")
    
    sql = "LIST @STORE_DB.ATLAS.AWS_S3_STG;"
    
    df = session.sql(sql).collect()
    
    st.dataframe(data=df,use_container_width=True)

    #from pathlib import Path

    #file_path = df[0]['name']
    #st.success(Path(file_path).name)
    
    #files = st.selectbox("Please select any one file from the below list",(df))
    
    sql_filename = "select distinct METADATA$FILENAME from @STORE_DB.ATLAS.AWS_S3_STG"

    df_filename = session.sql(sql_filename).collect()

    files = st.selectbox("Please select any one file from the below list",(df_filename))
    #st.write("You select",files)
    
    #st.button("Preview",type="primary")
    #Write directly to the app
    if 'clicked' not in st.session_state:
        st.session_state.clicked = False

    def click_button():
        st.session_state.clicked = True

    st.button('PREVIEW', on_click=click_button, type = 'primary')

    #Preview code
    if st.session_state.clicked:
        # The message and nested widget will remain on the page
        st.write("You have selected",files)
        #extract the table name from file name
        table_name = files.replace(' ', '')[:-4].upper()

        #sql_pull=f" SELECT $1,$2,$3,$4,$5,$6,$7,$8 FROM @AWS_S3_STG/{files};"
        sql_truncate = f"TRUNCATE TABLE STORE_DB.ATLAS.{table_name};"
        session.sql(sql_truncate).collect()
        sql_copy = f"COPY INTO STORE_DB.ATLAS.{table_name} FROM @STORE_DB.ATLAS.AWS_S3_STG/{files} FILE_FORMAT=F1"
        session.sql(sql_copy).collect()
        sql_select = f"select * from STORE_DB.ATLAS.{table_name}"
        df_sql_select = session.sql(sql_select).collect()
        #st.write(df_sql_select)
        dataframe_select = st.dataframe(df_sql_select)

        #Column Names
        sql_columns = f"select column_name from information_schema.columns where table_name = '{table_name}' and table_schema = 'ATLAS';"
        df_sql_columns = session.sql(sql_columns).collect()

        #For practice purpose
        st.markdown('----------------------')

        if st.session_state.clicked:
            #Multiselect
            st.subheader("Data Profiling")
            column_names = st.multiselect('Please select any column:',df_sql_columns)
            #st.write("Selected Columns : ",column_names)

        # container = st.beta_container()
        # all = st.checkbox("Select all")
        # if all:
        #     selected_options = container.multiselect('Please select any column:',df_sql_columns)
        # else:
        #     selected_options = container.multiselect('Please select any column:',df_sql_columns)
                    




#df2 = pd.DataFrame(df_sql_select)
#st.write(df2.head(5))

#st.session_state.clicked = False
    
    
    
