"""Handle querying the XEFR endpoints either local or remote"""
import configparser
import os
import subprocess
import common_funcs as cf   
import pandas as pd


class EndPoints(object):

    """Query the XEFR endpoints"""

    def __init__(self,instance,database,mongo,utilities,logger):
        """Initialise the connector"""

        self.cfg = configparser.ConfigParser()
        self.cfg.read("xefr_tools.ini")
        self.mongo = mongo
        self.utilities = utilities
        self.logger = logger
        self.instance = instance
        self.database = database
        self.server =self.cfg[instance]['server']
        self.api_key= self.cfg[instance][database]
        self.endpoints = {}

        for option in self.cfg['ENDPOINTS']:
            self.endpoints[option] = self.cfg.get('ENDPOINTS', option)

        self.format = "csv"

        self.logger.info(f"Endpoints: initiated for {self.instance} instance on {self.database} database via {self.server}")

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

        output_file =f"{cf.get_xefr_directory()}{os.sep}{schema_name}.csv"
        self.utilities.check_csv_folder_exists(output_file,True,self.logger)

        schema_id = self.mongo.get_schema_id(schema_name)

        if schema_id != 0:
            curl_cmd = self.get_endpoint_curl(schema_id,'data',True)
            self.persist_data(curl_cmd,schema_name,output_file,show_detail)
        else:
            print(f"Schema {schema_name} not found")

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

        
