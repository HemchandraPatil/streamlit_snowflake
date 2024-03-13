# Import python packages
from snowflake.snowpark import Session
import pandas as pd
import requests
import snowflake.connector
from urllib.error import URLError
import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Saama's Global Hackthon",
    page_icon=":wave:",
    layout="wide",
    initial_sidebar_state="expanded"
)

def create_session():
    return Session.builder.configs(st.secrets["snowflake"]).create()
session = create_session()

col1, col2,col3 = st.columns(3)

with col1:
    st.image('saama_logo.jpg',width = 150)

with col2:
    st.header("Saama Thunder's")

with col3:
    st.header("Hackathon 2024")

st.markdown("""<hr style="height:2px;border:none;color:#1E90FF;background-color:#1E90FF;" /> """, unsafe_allow_html=True)
# Write directly to the app
st.header("Simple Data Management Application using Snowflake, Streamlit, SnowPark")

with st.container(border=True):
    status = st.radio("Select One Stage: ", ('Internal Satge', 'External Stage(S3)','Named Stage'),index=None)

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

    #files = st.selectbox("Please select any one file from the below list",(df))

    sql_filename = "select distinct METADATA$FILENAME from @STORE_DB.ATLAS.AWS_S3_STG"
    df_filename = session.sql(sql_filename).collect()
    files = st.selectbox("Please select any one file from the below list",(df_filename))

    #Write directly to the app
    if 'clicked' not in st.session_state:
        st.session_state.clicked = False

    def click_button():
        st.session_state.clicked = True

    st.button('PREVIEW', on_click=click_button)

    #Preview code
    if st.session_state.clicked:
        # The message and nested widget will remain on the page
        st.write(f"You have selected {files} :")
        #extract the table name from file name
        table_name = files.replace(' ', '')[:-4].upper()

        #sql_pull=f" SELECT $1,$2,$3,$4,$5,$6,$7,$8 FROM @AWS_S3_STG/{files};"
        sql_truncate = f"TRUNCATE TABLE STORE_DB.ATLAS.{table_name};"
        session.sql(sql_truncate).collect()
        sql_copy = f"COPY INTO STORE_DB.ATLAS.{table_name} FROM @STORE_DB.ATLAS.AWS_S3_STG/{files} FILE_FORMAT=F1"
        session.sql(sql_copy).collect()
        sql_select = f"select * from STORE_DB.ATLAS.{table_name}"
        df_sql_select = session.sql(sql_select).collect()
        dataframe_select = pd.DataFrame(df_sql_select)
        #st.write(dataframe_select)

        # Show data
        @st.cache_data(show_spinner=False)
        def split_frame(input_df, rows):
            df3 = [input_df.loc[i : i + rows - 1, :] for i in range(0, len(input_df), rows)]
            return df3
        
        pagination = st.container()

        bottom_menu = st.columns((4,1,1))
        with bottom_menu[2]:
            batch_size = st.selectbox("Page Size", options=[10, 20, 50])
        with bottom_menu[1]:
            total_pages = (
                            int(len(dataframe_select) / batch_size) if int(len(dataframe_select) / batch_size) > 0 else 1
                        )
            current_page = st.number_input(
                                            "Page", min_value=1, max_value=total_pages, step=1
                                        )
        with bottom_menu[0]:
            st.markdown(f"Page **{current_page}** of **{total_pages}** ")

        pages = split_frame(dataframe_select, batch_size)
        pagination.dataframe(data=pages[current_page - 1], use_container_width=True)

        #Column Names
        sql_columns = f"select column_name from information_schema.columns where table_name = '{table_name}' and table_schema = 'ATLAS';"
        df_sql_columns = session.sql(sql_columns).collect()
        df_columns = pd.DataFrame(df_sql_columns)

        st.markdown('----------------------')
        #Data Profiling
        st.subheader("Data Profiling")
        st.caption("Data Profiling Table :")
        #code to show data profiling
        desc = dataframe_select.describe(include="all")
        st.write(desc.transpose())

        #Checkbox
        #Function to create checkbox
        st.caption('Please select any columns from the sidebar :')
        def checkbox_container(data):
            #select_column_box = st.text_input('Please select any column')
            cols = st.columns(5)
            if cols[0].button('Select All', type = 'primary'):
                for i in data['COLUMN_NAME']:
                    st.session_state['dynamic_checkbox_' + i] = True
                st.experimental_rerun()
            if cols[1].button('UnSelect All',type='primary'):
                for i in data['COLUMN_NAME']:
                    st.session_state['dynamic_checkbox_' + i] = False
                st.experimental_rerun()
            st.sidebar.caption("Column Names :")
            for i in data['COLUMN_NAME']:
                st.sidebar.checkbox(i, key='dynamic_checkbox_' + i)

        #Function to print selected columns    
        def get_selected_checkboxes():
            return [i.replace('dynamic_checkbox_','') for i in st.session_state.keys() if i.startswith('dynamic_checkbox_') and st.session_state[i]]

        checkbox_container(df_columns)
        new_data = st.text_input('You selected',get_selected_checkboxes())
        #st.write(get_selected_checkboxes())

        selected_column = get_selected_checkboxes()

        #to preview the data for selected columns
        try:
            appended_data = []
            for values in selected_column:
                #sql query to preview the data for selected columns
                # to show the distinct records
                select_sql = f"select distinct {values} from {table_name} limit 5"
                collect_select_sql = session.sql(select_sql).collect()
                df_select_sql = pd.DataFrame(collect_select_sql)
                appended_data.append(df_select_sql) 
            df2 = pd.concat(appended_data, axis=1,ignore_index=True)
            df2.columns = selected_column
            st.write(df2)
        except:
            st.write("Please select any one column from the sidebar checkbox")

         
        # col1, col2 = st.columns(2)
        # try:
        #     with col1:
        #         st.header("Distinct Record")
        #         select_sql = f"select distinct {values} from {table_name}"
        #         collect_select_sql = session.sql(select_sql).collect()
        #         df_select_sql = pd.DataFrame(collect_select_sql)
        #         st.write(df_select_sql)

        #     with col2:
        #         st.header("Counts")
        #         count_sql = f"select {values},count(*) as Total from {table_name} group by {values}"
        #         collect_count_sql = session.sql(count_sql).collect()
        #         df_count_sql = pd.DataFrame(collect_count_sql)
        #         st.write(df_count_sql)
        # except:
        #     st.write("No columns selected")  

        # null_sql = f"select 'Null count' as NULL_COUNT, (select count(*) from mocked_speaker_data where upper({values}) = 'NAN') as COUNT"
        # collect_null_sql = session.sql(null_sql).collect()
        # df_null_sql = pd.DataFrame(collect_null_sql)
        # st.write(collect_null_sql)

        #query for dtype
        # desc = dataframe_select.describe(include="all")
        # st.write(desc.transpose())

        edited_df = st.data_editor(dataframe_select)
            
        


        

       
        

        


#st.session_state.clicked = False 