"""Handle querying the XEFR endpoints either local or remote"""
import configparser
import os
import sys


class EndPoints(object):
    """Query the XEFR endpoints"""

    def __init__(self,template,format,logger):
        """Initialise the connector"""
        logger.debug("Initialising EndPoints")
        self.cfg = configparser.ConfigParser()
        self.cfg.read("xefr_tools.ini")

        self.logger = logger
        self.server =self.cfg[template]['server']
        self.api_key = self.cfg[template]['api_key']
        self.endpoints = {}

        for option in self.cfg['ENDPOINTS']:
            self.endpoints[option] = self.cfg.get('ENDPOINTS', option)

        if format not in ['csv','json']:
            print(f"Format {format} not supported")
            sys.exit(1)
        else:
            self.format = format

        if 'xefr-signify' in self.server:
            self.server = self.cfg[template]['server']
            self.api_key = self.cfg[template]['api_key']
        else:
            if os.name == 'posix':
                self.api_key = self.cfg[template]['posix_api_key']
            else:
                self.api_key = self.cfg[template]['api_key']

        self.logger.debug("EndPoints initiated with: %s", self.server)

    def get_endpoint_curl(self,schema_id,endpoint_name,use_temp=True):
        """
        Returns the curk command for a specific endpoint
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