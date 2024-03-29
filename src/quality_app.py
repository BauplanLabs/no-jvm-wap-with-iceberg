"""

This is a streamlit app that interacts (once again, in pure Python) with the data lake.

The first section showcases the number of rows in the table in the main branch (the number should go up
everytime a successful upload is performed).

The second section allows the user to select a branch and count the number of nulls in that branch (this
is useful to verify that, when a quality check is failed, the data is actually wrong in the branch).

"""

import os
import streamlit as st
from pyiceberg.catalog import load_catalog
import pyarrow.compute as pc
# add the serverless directory to the path
# to re-use the code from app.py
import sys
sys.path.append('serverless')
import monkey_patch
from pyiceberg_patch_nessie import NessieCatalog
from dotenv import load_dotenv
load_dotenv('serverless/.env')


# this is the name of the table in the catalog, the same one
# to be found in app.py where we append the new rows etc.
TABLE_NAME = 'customer_data_log'
# this is the column we are interested in for the quality check
target_column = "my_col_1"
# initialize the Nessie catalog
catalog = load_catalog(
    name='default',
    type='nessie',
    endpoint=os.environ['NESSIE_ENDPOINT'],
    default_branch='main'
)    
# if the target table does not exist in main (first run), create it
datalake_location = 's3://{}'.format(os.environ['LAKE_BUCKET'])


def get_table_from_data_lake(
    table_name: str,
    branch_name: str,
):
    _table = catalog.load_table((branch_name, table_name))
    # NOTE that this will get the full table, so it's not recommended for large tables
    return _table.scan().to_arrow()


st.title('Data Quality App')

# the first section reads the data from the lakehouse in the main branch
st.subheader("Main branch")

table_in_main = get_table_from_data_lake(TABLE_NAME, 'main')
st.write("Number of rows in the main branch: ", table_in_main.num_rows)

st.subheader("Custom branch")

import_branch = st.text_input("Enter the branch name you'd like to visualize", '')

if not import_branch:
    st.stop()
    
table_in_branch = get_table_from_data_lake(TABLE_NAME, import_branch)
st.write("Number of rows in the branch: ", table_in_branch.num_rows)
st.write("Number of nulls in the branch for column {}: ".format(target_column), pc.count(
    table_in_branch[target_column], mode='only_null'
))