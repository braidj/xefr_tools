"""
Functions related to updating the data sets required by PBI"
"""
import os
import sys
import utilities
import xefr_endpoints
import subprocess
import pandas as pd

XEFR_DATA_DIR = os.path.join('data', 'xefr')
TSP_DATA_DIR = os.path.join('data', 'tsp')
BULL_DATA_DIR = os.path.join('data', 'bullhorn')
XEFR_UPLOADS_DIR = os.path.join('data', 'xefr', 'uploads')
TEST_UPLOADS_DIR = os.path.join('data', 'test')

logging = utilities.MyLogger()
logger = logging.getLogger()

sort_orders = {
    'Metrics UK NFI Adjustments':'Candidate,Placement,InvoiceDate',
    'TSP UK Invoice Tracker':'Candidate,Placement,InvoiceDate',
    'Metrics GBP Forex Daily':'Year,Month,Day,Symbol',
    'Bullhorn Candidates':'Consultant',
    'Bullhorn Consultant Details':'Consultant'
}

def report_schemas(download,schema_list='not passed'):
    """
    Reports the schema name and id for requested schemas
    If download is True, the data is downloaded and saved to a file
    """

    format = 'csv'
    xefr = xefr_endpoints.EndPoints('ENDPOINTS_DEV',format,logger)

    all_schemas = xefr.get_schemas_details()
    sorted_schemas = sorted(all_schemas, key=lambda k: k['name'])
    sorted_names = [i['name'] for i in sorted_schemas] # used to check if schema is available

    if schema_list=='not passed':
        print(f'\nThe full {len(sorted_schemas)} schema(s) are:\n')
        filtered = sorted_schemas
    else:
        if type(schema_list) != list:
            print("Expected a list of schema names")
            raise TypeError
        else:

            missing_items = [item for item in schema_list if item not in sorted_names]
            if len(missing_items) > 0:
                print(f"The following schema(s) are not available in that instance: {missing_items}")
                sys.exit(1)
            
            filtered = [i for i in sorted_schemas if i['name'] in schema_list]

    for counter, i in enumerate(filtered,1):

            file = i['name']
            schema_id = i['id']

            if download:
                curl_cmd = xefr.get_endpoint_curl(i['id'],True)
                print(f"{counter}: {file} = {schema_id}, {curl_cmd}\n")
                persist_data(curl_cmd,file)
            else:
                print(f"{counter}: {file} = {schema_id}")

def persist_data(curl_command,schema_name):
    """Run the curl command and save the output to a file"""
    logger.debug('persist_data: %s', locals())

    curl_output = subprocess.check_output(curl_command, shell=True)

    output_file = os.path.join(XEFR_DATA_DIR,schema_name +'.csv')

    with open(output_file, 'wb') as file:
        file.write(curl_output)
    try:
        df = pd.read_csv(output_file)

        if schema_name in sort_orders: # Require data to be sorted
            utilities.data_frame_sort_and_save(df,output_file,sort_orders[schema_name],logger)

        record_count = len(df)
       # df.to_csv(output_file,index=False,encoding='utf-8') # ensure in same format for reconciliations
    except pd.errors.EmptyDataError:
        record_count = 0

    print(f"Data saved to {output_file} ({record_count} rows)\n")

def pbi_connection_details(curl_command,use_temp):
    """Returns the attributes required to set up a PBI endpoint"""

    import re

    if use_temp:
        api_key = re.search(r'apikey=([^"]+)', curl_command).group(1)
        url =  re.search(r'"(.*?)"', curl_command).group(1)
    else:
        api_key = re.search(r'X-API-KEY:\s*([^"]+)', curl_command).group(1)
        url = re.search(r'(https?://\S+)', curl_command).group(1)

    if use_temp:
        print(url)
    else:
        print('X-API-KEY')
        print(url)
        print(api_key,'\n')



def main():
    """Main"""

    logger.debug('main: %s', locals())

    # all_schemas = ['Metrics Candidate Splits','Data Bullhorn Consultant Actions','Metrics UK NFI Adjustments','Dashboard TSP UK Invoice Tracker','Metrics FOREX','Metrics Charge Codes','Metrics Bullhorn Placements']
    
    report_schemas(False,['TSP UK Placement Forecast','Metrics UK NFI Adjustments'])
    sys.exit(0)
 

if __name__ == "__main__":

    logging.reset_log()
    main()
    