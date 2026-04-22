import pandas as pd
import numpy as np
import AnalyticsClient
import pandas as pd
import os
import time
from AnalyticsClient import AnalyticsClient
from sqlalchemy import create_engine

# API Credentials
CLIENTID = "1000.XF30WZLTGF5AQMEBFV3BKMMCPMH0VW"
CLIENTSECRET = "0cfc81ecb03133528d9ea2f5323799e9ea3f33c36a"
REFRESHTOKEN = "1000.5033c28e2cd0afe733c7010d32ca64ac.aac0d2e6e6e529db4119f9768e3dd2d1"
ORGID = "60010791267"
WORKSPACEID = "209206000000004002"
VIEWID = "209206000152093782"

# Initialize Analytics Client
ac = AnalyticsClient(CLIENTID, CLIENTSECRET, REFRESHTOKEN)

# Initiates the bulk export job
def initiate_bulk_export(ORGID, WORKSPACEID, VIEWID):
    response_format = "csv"
    bulk = ac.get_bulk_instance(ORGID, WORKSPACEID)
    result = bulk.initiate_bulk_export(VIEWID, response_format)  # Creates Job ID
    print(f"Bulk export initiated, Job ID: {result}")
    return result  # Return Job ID

# Exports bulk data and loads it into a DataFrame
def export_bulk_data_to_dataframe(job_id, ac, ORGID, WORKSPACEID):
    bulk = ac.get_bulk_instance(ORGID, WORKSPACEID)

    # Export bulk data to a temporary file path
    temp_file_path = "temp_export.csv"
    bulk.export_bulk_data(job_id, temp_file_path)
    print(f"Data successfully exported to {temp_file_path}")

    # Load the exported data into a DataFrame
    df = pd.read_csv(temp_file_path)
    print("Data successfully loaded into DataFrame.")

    # Clean up the temporary file
    os.remove(temp_file_path)
    return df

# Main execution
if __name__ == "__main__":
    try:
        # Step 1: Initiate the export
        job_id = initiate_bulk_export(ORGID, WORKSPACEID, VIEWID)

        # Optional: Add a delay if required by the API
        time.sleep(60)

        # Step 2: Fetch and process the data into a DataFrame
        df = export_bulk_data_to_dataframe(job_id, ac, ORGID, WORKSPACEID)

        # Display the DataFrame
        print(df.head())

    except Exception as e:
        print(f"An error occurred: {e}")

engine = create_engine("postgresql+psycopg2://bhushan:Ln6T*99cMxu>@70.70.3.136:5432/warehousedb")
engine.connect()
df.to_sql(
    'sample_table',
    con=engine,
    if_exists='append',
    index=False
)
print('Data Uploaded Sucessfully')
