"""Handle querying the XEFR endpoints either local or remote"""
import os
import subprocess
import common_funcs as cf   
import pandas as pd
import csv
import io
from prettytable import PrettyTable

#TODO - add a check to see if the endpoint is running when first called
class EndPoints(object):

    """Query the XEFR endpoints"""

    def __init__(self,conn,mongo,utilities,download_folder,logger):
        """Initialise the connector"""

        self.mongo = mongo
        self.utilities = utilities
        self.download_folder = download_folder
        self.logger = logger
        self.instance = conn['instance']
        self.database = conn['database']
        self.server = conn['server']
        self.api_key= conn['api_key']
        self.config = conn # all the details passed in from the ini file

        self.endpoints = {}
        self.endpoints["data"] = "api/data/format"
        self.endpoints["schemas" ]= "api/schemas"
        self.endpoints["portals"] = "api/portals"
        self.endpoints["data_upload"] = "api/data/format/csv"

        self.format = "csv"

        self.logger.info(f"Endpoints: initiated for {self.instance} instance on {self.database} database via {self.server}")

    def data_reconcilation(self):
        """
        Using the schemas listed in the ini file, download the data, and total on each numeric column. 
        """

        if 'data_recon' in self.config:
            schemas = self.config['data_recon'].split(',')
            cf.colour_text(f"Reconciling {len(schemas)} schemas","GREEN")
            for schema in schemas:
                cf.colour_text(f"Reconciling {schema.strip()}","GREEN")
                target = self.download_schemas_data(schema.strip())
                cf.sum_numeric_columns(target,self.config)
                # cf.generate_md5_checksum(target)
        else:
            cf.colour_text("No schemas to reconcile","RED")

    def download_all_schemas_data(self,show_detail=False):
        """
        Downloads the data from all schemas
        """
        schema_names = [x['name'] for x in self.mongo.all_schemas()]

        for schema_name in schema_names:
            self.download_schemas_data(schema_name,show_detail)

    def download_schemas_data(self,schema_name,show_detail=False):
        """
        Downloads the data from a single schema
        """

        output_file =f"{self.download_folder}{os.sep}{schema_name}.csv"

        schema_id = self.mongo.get_schema_id(schema_name)

        if schema_id != 0:
            curl_cmd = self.get_endpoint_curl(schema_id,'data',True)
            self.persist_data(curl_cmd,schema_name,output_file,show_detail)
        else:
            print(f"Schema {schema_name} not found")

        return output_file

    def display_data(self,curl_command,schema_name,show_detail=False,nos_rows=25):
        """
        Displays the first 10 rows of data nicely formatted
        """

        if show_detail:
            print(curl_command)
            curl_output = subprocess.check_output(curl_command, shell=True)
        else:
            curl_output = subprocess.check_output(curl_command,shell=True,stderr=subprocess.PIPE)

        csv_data_string = curl_output.decode('utf-8')

        # Create a StringIO object to work with the CSV module
        csv_data_io = io.StringIO(csv_data_string)

        # Create a CSV reader
        csv_reader = csv.reader(csv_data_io)

        # Create a PrettyTable instance and set the column names from the header
        header = next(csv_reader,[])

        if schema_name in cf.hide_columns: # Filter out columns if required
            columns_to_exclude = cf.hide_columns[schema_name].split(',')
        else:
            columns_to_exclude = []

        # Create a PrettyTable instance without the excluded columns
        table = PrettyTable([col for col in header if col not in columns_to_exclude])

        # Add the first <nos_rows> data rows to the table
        for i, row in enumerate(csv_reader):
            if i >= nos_rows:
                break
            # Create a filtered row without the excluded columns
            filtered_row = [cell for j, cell in enumerate(row) if header[j] not in columns_to_exclude]
            table.add_row(filtered_row)

        if header == []:
            cf.colour_text("Check endpoint service is running, no data","RED")
            cf.colour_text("API key is correct, no data","RED")
        else:
            print(table)

    def persist_data(self,curl_command,schema_name,output_file,show_detail=False):
        """
        Run the curl command and save the output to a file
        if show_detail is set to True will display the 
        download details.
        """
        self.logger.debug('persist_data: %s', locals())

        if show_detail:
            print(curl_command)
            curl_output = subprocess.check_output(curl_command, shell=True)
        else:
            curl_output = subprocess.check_output(curl_command,shell=True,stderr=subprocess.PIPE)


        with open(output_file, 'wb') as file:
            file.write(curl_output)
        try:
            df = pd.read_csv(output_file)

            if schema_name in cf.sort_orders: # Require data to be sorted
                self.utilities.data_frame_sort_and_save(df,output_file,cf.sort_orders[schema_name],self.logger)

            record_count = len(df)

        except pd.errors.EmptyDataError:
            record_count = 0

        print(f"{schema_name}: {record_count} rows downloaded to {output_file}")

    def get_endpoint_curl(self,schema_id,endpoint_name,use_temp=True):
        """
        Returns the curl command for a specific endpoint
        """

        endpoint = self.endpoints[endpoint_name]

        curl_command_header = f'curl -H "X-API-KEY: {self.api_key}" {self.server}/{endpoint}/{self.format}/{schema_id}'
        curl_command_temp = f'curl "{self.server}/{endpoint}/{self.format}/{schema_id}?apikey={self.api_key}"'

        if use_temp:
            curl_command = curl_command_temp
        else:
            curl_command = curl_command_header

        return curl_command

    def get_endpoints(self,schema_names,use_temp=True):
        """
        Returns the curl command to return data from the XEFR data API in the required format
        N.B. it does NOT run it, just returns the command
        Handles 2 versions, one with the header and one without for the token
        Header version was used when connecting to Power BI and is now redundant
        """
        results = []

        for schema in schema_names:

            schema_id = self.conn.schema_id_by_name(schema)

            curl_command_header = f'curl -H "X-API-KEY: {self.api_key}" {self.server}/{self.end_point}/{self.format}/{schema_id}'
            curl_command_temp = f'curl "{self.server}/{self.end_point}/{self.format}/{schema_id}?apikey={self.api_key}"'

            curl_command = self.get_endpoint_curl(schema_id,use_temp)

            row = {'schema':schema,'curl_command':curl_command,'schema_id':schema_id}
            results.append(row)

        self.conn.disconnect()

        return results

    def display_schema(self,schema_name):
        """
        Show the first 10 rows of data from a schema
        """
        schema_id = self.mongo.get_schema_id(schema_name)

        if schema_id != 0:
            curl_cmd = self.get_endpoint_curl(schema_id,'data',True)
            curl_output = self.display_data(curl_cmd,schema_name)
        else:
            print(f"Schema {schema_name} not found")
