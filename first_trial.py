# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

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
    session = get_active_session()
    st.subheader("There are no files in Internal Stage")

if (status == 'Named Stage'):
    session = get_active_session()
    st.subheader("There are no files in Named Stage")

if(status=='External Stage(S3)'):
    session = get_active_session()

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
        sql_pull=f" SELECT $1,$2,$3,$4,$5,$6,$7,$8 FROM @AWS_S3_STG/{files};"
        #st.write(sql_pull)
        df_pull = session.sql(sql_pull).collect()
        #st.dataframe(data=df_pull,use_container_width=True)
        st.write((df_pull))

        #Multiselect
        if st.session_state.clicked:
            session = get_active_session()
            #data_stored = st.write(df_pull[0][:])
            data_stored = df_pull[0][:]
            st.write(data_stored[1])
            ss=len(data_stored)
            column_names = st.multiselect('Please select any column:',data_stored)
            st.write(column_names)
            st.write(ss)

    
    #For practice purpose
    st.markdown('----------------------')

    #data_stored = st.dataframe(df_pull, use_container_width=True)
    #st.write((df_pull[0].tolist())
    #st.session_state.clicked = False
    
    
    
