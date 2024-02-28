
import streamlit as st

# Initialize connection.
conn = st.connection("snowflake")

# Load the table as a dataframe using the Snowpark Session.
@st.cache_data
def load_table():
    session = conn.session()
    return session.table("mytable").to_pandas()

df = load_table()

# Print results.
for row in df.itertuples():
    st.write(f"{row.NAME} has a :{row.PET}:")
