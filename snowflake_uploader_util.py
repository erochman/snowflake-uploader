# Snowpark for Python
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import month,year,col,sum
from snowflake.snowpark.version import VERSION

# Misc
import json
import pandas as pd
import logging 
logger = logging.getLogger("snowflake.snowpark.session")
logger.setLevel(logging.ERROR)

# Other
import os
import shutil
import time
from datetime import datetime

# Create Snowflake Session object
connection_parameters = json.load(open('connection.json'))
session = Session.builder.configs(connection_parameters).create()
session.sql_simplifier_enabled = True

snowflake_environment = session.sql('select current_user(), current_version()').collect()
snowpark_version = VERSION

# Current Environment Details
print('User                        : {}'.format(snowflake_environment[0][0]))
print('Role                        : {}'.format(session.get_current_role()))
print('Database                    : {}'.format(session.get_current_database()))
print('Schema                      : {}'.format(session.get_current_schema()))
print('Warehouse                   : {}'.format(session.get_current_warehouse()))
print('Snowflake version           : {}'.format(snowflake_environment[0][1]))
print('Snowpark for Python version : {}.{}.{}'.format(snowpark_version[0],snowpark_version[1],snowpark_version[2]))


stages = '@BANKOFENGLAND'

csv_files = [f for f in os.listdir('upload/') if f.endswith('.csv')]
txt_files = [f for f in os.listdir('upload/') if f.endswith('.txt')]
file_to_uploads = csv_files

for file_to_upload in file_to_uploads:
    try:
        session.file.put(f"upload/{file_to_upload}", stages, overwrite = True)
        print(f'success to upload {file_to_upload}')
        time.sleep(1)
        source_path = os.path.join('upload', file_to_upload)
        destination_path = os.path.join('done upload', file_to_upload)

        if os.path.exists(destination_path):
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            
            # Get the base name and extension of the source file
            base_name, ext = os.path.splitext(os.path.basename(source_path))
            
            # Create a new filename with the timestamp
            new_destination_name = f"{base_name}_{timestamp}{ext}"
            destination_path = os.path.join('done upload', new_destination_name)

        shutil.move(source_path, destination_path)
        print(f'success to move {file_to_upload}')
    except:
        print(f'failed to upload {file_to_upload}')