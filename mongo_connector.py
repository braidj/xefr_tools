import pymongo
import sys
import json
import os
import common_funcs as cf

class Mongo(object):

    def __init__(self,conn,download_directory,logger) -> None:

        self.target = conn['instance']
        self.uri = conn['uri']
        self.download = download_directory
        self.schemas_json = os.path.join(self.download,"schemas.json")
        self.portals_json = os.path.join(self.download,"portals.json")
        self.config = conn
        self.logger = logger

        if self.target == "LOCAL":
            self.__local_connect(conn['database'])
        else:
            self.__remote_connect(conn['database'])

    def __test_connection(self):
        """
        Check can access schemas and portals collections
        """
        pass

    def get_xefr_json(self,doc_type):
        """
        Extracts out the schemas/portals collection to <schemas/portals>.json file from the mongo database
        This is functionally equivalent to clicking download schemas from the XEFR UI
        and a lot easier
        N.B. portals collection name is portalDefinitions
        """

        if doc_type not in ['schemas','portals']:
            raise ValueError("schemas or portals only")
        
        doc_type_collection = {
            "schemas":"schemas",
            "portals":"portalDefinitions"
        }
        
        collection = self.database.get_collection(doc_type_collection[doc_type])
        documents = collection.find()
        document_list = [doc for doc in documents]

        # Define the path to the JSON file where you want to store the data
        if doc_type == 'schemas':
            output_file = self.schemas_json
        elif doc_type == 'portals':
            output_file = self.portals_json

        # Write the list of dictionaries to a JSON file
        try:
            with open(output_file, 'w') as json_file:
                json.dump(document_list, json_file, default=str, indent=4)
                self.logger.info(f"Mongo: downloaded {output_file}")
        except Exception as e:
            self.logger.error(f"Mongo: Failed to download {output_file} - {e}")
            print(f"Failed to download {output_file} - {e}")
            sys.exit(1)

    def all_schemas(self):
        """
        Returns key details of the schemas
        """
        schema_details = []
        collection = self.database.get_collection("schemas")
        documents = collection.find()
        for doc in documents:
            row = {'id':doc['id'],'name':doc['name'],'description':doc['description']}
            schema_details.append(row)

        self.logger.info(f"{Mongo.__name__}: {len(schema_details)} schemas found in {self.database}")

        return schema_details
    
    def get_schema_id(self,schema_name,bol_display=False):
        """
        Returns the schema id for a given schema name
        If bol_display is True then the schema ID is displayed
        """
        self.logger.info(f"{Mongo.__name__}: Getting schema id for {schema_name}")
        collection = self.database.get_collection("schemas")
        try:
            schema_id = collection.find_one({"name":schema_name})['id']
            self.logger.info(f"{Mongo.__name__}: Schema id for {schema_name} is {schema_id}")
            if bol_display:
                cf.colour_text(f"{schema_name} is {schema_id}","BLUE")
            return schema_id
        
        except TypeError as e:

            if "'NoneType' object is not subscriptable" in str(e):
                # Handle the specific TypeError here
                print(f"Schema {schema_name} does not exist in {self.database}")
                self.logger.warning(f"{Mongo.__name__}: Schema {schema_name} does not exist in {self.database}")
                return 0
            else:
                # Handle other TypeErrors
                print("Caught a different TypeError:", e)
                self.logger.error(f"{Mongo.__name__}: Caught a different TypeError: {e}")
                return 0
            
    def get_schema_details_by_id(self,id,bol_display=False):
        """
        Returns the schema details for a given schema id
        """
        collection = self.database.get_collection("schemas")
        schema_details = collection.find_one({"id":id})

        if type(schema_details) == type(None):
            self.logger.warning(f"{Mongo.__name__}: Schema {id} does not exist in {self.database}")
            print(collection)
            return 0
        else:
            self.logger.info(f"{Mongo.__name__}: Returned schema details for {id}")
            if bol_display:
                cf.colour_text(f"{id} = {schema_details['name']}","BLUE")
            return schema_details
   
    def get_schema_details(self,schema_name):
        """
        Returns the schema details for a given schema name
        """
        collection = self.database.get_collection("schemas")
        schema_details = collection.find_one({"name":schema_name})

        if type(schema_details) == type(None):
            self.logger.warning(f"{Mongo.__name__}: Schema {schema_name} does not exist in {self.database}")
            return 0
        else:
            self.logger.info(f"{Mongo.__name__}: Returned schema details for {schema_name}")
            return schema_details

    def __local_connect(self,database):
        """
        Local connection to  a specific client
        """
        mongo_host = 'localhost'  # The hostname of your MongoDB server
        mongo_port = 27017  # The default MongoDB port

        # MongoDB connection URI
        connection_uri = f"mongodb://{mongo_host}:{mongo_port}/{database}"

        # Connect to MongoDB
        try:
            # client = pymongo.MongoClient(connection_uri, username=username, password=password)
            self.client = pymongo.MongoClient(connection_uri) 
            self.database = self.client[database]
            self.logger.info(f"Mongo: LOCAL connection establised to {database}")
        except pymongo.errors.ConnectionFailure as e:
            self.logger.error(f"{Mongo.__name__} Failed to connect to MongoDB: {e}")
            print("Failed to connect to MongoDB:", e)
            sys.exit(1)

    def __remote_connect(self,database):
        """
        Remote connection to a specific client
        """
        # Connect to MongoDB
        try:
            # client = pymongo.MongoClient(connection_uri, username=username, password=password)
            self.client = pymongo.MongoClient(self.uri) 
            self.database = self.client[database]
            self.logger.info(f"Mongo: REMOTE connection establised to {database}")

        except pymongo.errors.ConnectionFailure as e:
            self.logger.error(f"{Mongo.__name__} Failed to connect to MongoDB: {e}")
            print("Failed to connect to MongoDB:", e)
            sys.exit(1)
    
    def disconnect(self):
        self.client.close()
        self.logger.info(f"Mongo: Disconnected from {self.target} instance")

