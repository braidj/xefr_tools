import os
import sys
import subprocess
import json
import data_folders as DATA_FOLDER
import xefr_endpoints
import pandas as pd

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, 'xerini_utils'))
sys.path.append(utils_path)

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, 'mongo'))
sys.path.append(utils_path)

import utilities
import mongo_connector

logging = utilities.MyLogger()
logging.reset_log()
logger = logging.getLogger()

database = 'xefr-signify-dev'
instance = "LOCAL"

xefr = xefr_endpoints.EndPoints(instance,database,logger)
mongo = mongo_connector.Mongo(instance,database,logger)

def persist_data(curl_command,schema_name,show_detail=False):
    """
    Run the curl command and save the output to a file
    if show_detail is set to True will display the 
    download details.
    """
    logger.debug('persist_data: %s', locals())

    if show_detail:
        curl_output = subprocess.check_output(curl_command, shell=True)
    else:
        curl_output = subprocess.check_output(curl_command,shell=True,stderr=subprocess.PIPE)

    output_file = os.path.join(DATA_FOLDER.XEFR,schema_name)
    utilities.check_csv_folder_exists(output_file,True,logger)

    with open(output_file, 'wb') as file:
        file.write(curl_output)
    try:
        df = pd.read_csv(output_file)

        if schema_name in DATA_FOLDER.sort_orders: # Require data to be sorted
            utilities.data_frame_sort_and_save(df,output_file,DATA_FOLDER.sort_orders[schema_name],logger)

        record_count = len(df)

    except pd.errors.EmptyDataError:
        record_count = 0

    print(f"\n{schema_name}: {record_count} rows downloaded to {output_file}\n")

def portal_backup(portal_name):
    """
    Creates a backup of a portal, and all its attributes into
    its own file
    """
    downloads = get_download_directory()
    source_file = f"{downloads}{os.sep}portals.json"
    destination_file = f"{downloads}{os.sep}{portal_name}.json"

    with open(source_file, 'r') as file:
        data = json.load(file)

    for portal in data:
        title = portal.get("title")

        if title == portal_name:
            target_portal = portal

    with open(destination_file, 'w') as json_file:
        try:
            json.dump([target_portal], json_file, indent=4)
            print(f"File {destination_file} created")
        except:
            print("Bad do do happened")

def report_dependencies(portal_name):
    """
    For a given Portal, report the schemas it references
    "portletHolder": {

      "childPortals": [
        {

          "portlet": {
            "title": "Retainers",
            "schemaId": "9ed7f42e-380d-490a-bd21-85e36ea7abbf",
    """

    downloads = get_download_directory()
    source_file = f"{downloads}{os.sep}portals.json"

    with open(source_file, 'r') as file:
        data = json.load(file)

    for portal in data:

        title = portal.get("title")
        

        if portal.get('isHome'):
            print(f"{title} is a parent Portal")
        else:
            print(f"\t{title} is a child Portal")

        portletHolder = portal.get("portletHolder")

        if "childPortals" not in portletHolder.keys():
            pass
        else:

            childPortals = portletHolder.get("childPortals")

            for tab in childPortals:
                portlet = tab.get("portlet")
                schema_name = portlet.get("title")
                schemaID = portlet.get("schemaId")
                print("\t\t",schema_name,":",schemaID)

def get_download_directory():
    """
    Get the user's home directory
    """
    home_directory = os.path.expanduser("~")

    download_directory = os.path.join(home_directory, "Downloads")

    return download_directory

def portlets_update(portal_name,formatting_details,skip_items):
    """
    Apply a standard set of formats against all portlets in a portal
    Will skip any tabbed items in the skip_items list
    Relationships between portals and portlets are:
    1 - 1 portal to portletHolder
        1 - M portletHolder to childPortals
    """

    downloads = get_download_directory()
    source_file = f"{downloads}{os.sep}portals.json"
    destination_file = f"{downloads}{os.sep}formatted_portal.json"

    with open(source_file, 'r') as file:
        data = json.load(file)

    for portal in data:
        title = portal.get("title")

        if title == portal_name:

            target_portal = portal
            portletHolder = portal.get("portletHolder")
            childPortals = portletHolder.get("childPortals")

            print(f"Portal {title} has {len(childPortals)} portlets")

            for tab in childPortals:
                portlet = tab.get("portlet")
                print("\t",portlet.get("title"))

                if portlet.get("title") in skip_items:
                    print(f"\t\tSkipping {portlet.get('title')}")
                else:
                    for key,value in formatting_details.items():
                        portlet[key] = value

    with open(destination_file, 'w') as json_file:
        try:
            json.dump([target_portal], json_file, indent=4)
            print(f"File {destination_file} created")
        except:
            print("Bad do do happened")

def schema_copy(input_file, details):
    """
    Duplicate the contents of the from_schema to the to_schema
    N.B.
    schema.attributes is the contents required to copy
    schema.pipelineText is required for Pivots
    schema.typeClass is the type of schema (e.g. "view")
    schema.autoGenerateId is not required for pivots
    """
    bol_has_pipeline = False

    print(f"Copying contents of {details['from_schema']} to {details['to_schema']}")

    with open(input_file, 'r') as file:
        data = json.load(file)

    # Assuming your JSON file is a list of schema objects
    for schema in data:
        title = schema.get("name")

        if title == details["from_schema"]:
            contents = schema.get("attributes")
            type_class = schema.get("typeClass")
            #auto_generate_id = schema.get("autoGenerateId")

            if "pipelineText" in schema.keys():
                pipeline = schema.get("pipelineText")
                bol_has_pipeline = True

        if title == details["to_schema"]:
            update_schema = schema

    update_schema["attributes"] = contents

    if bol_has_pipeline:
        update_schema["pipelineText"] = pipeline
        update_schema.pop("autoGenerateId")

    update_schema["typeClass"] = type_class
    
    return update_schema

def portal_copy_section(input_file, details):
    """
    Copies a portal section from one portal to another
    """

    if details["copy_type"] not in COPY_TYPES:
        raise ValueError(f"Copy type must be one of {COPY_TYPES}")
    
    if details["copy_type"] == "append" and "append_object" not in details.keys():
        raise ValueError("Copy type is append, but no append object specified")
    
    print(f"Copying contents of {details['from_portal']} to {details['to_portal']}")

    # Open the JSON file and read its contents
    with open(input_file, 'r') as file:
        data = json.load(file)

    # Assuming your JSON file is a list of portal objects
    for portal in data:
        title = portal.get("title")

        if title == details["from_portal"]:
            source_content = portal.get(details["copy_object"])
            source_portal = portal

        if title == details["to_portal"]:
            update_portal = portal

    if details["copy_type"] == "replace":
        update_portal[details["copy_object"]] = source_content

    if details["copy_type"] == "append":
        parent_items = source_portal.get(details["copy_object"])
        for item in parent_items[details["append_object"]]:
            print(item["portlet"]["title"])

    return update_portal

def update_defs(schema_or_portal,details):
    """
    Manages copying of schemas or migrating of tabs between portals
    Receives combined details for both scenarios
    Returns jason compatible array and the destination file
    """
    
    downloads = get_download_directory()
    source_file = f"{downloads}{os.sep}{schema_or_portal}.json"
    destination_file = f"{downloads}{os.sep}updated_{schema_or_portal}.json"

    data_list = []

    if schema_or_portal == "schemas":
        data_list = [schema_copy(source_file, details)]

    if schema_or_portal == "portals":
        data_list = [portal_copy_section(source_file, details)]

    return data_list, destination_file

def download_schemas_data(schema_list):
    """
    Downloads the data from specified schemas
    """

    for schema in schema_list:

        output_file = os.path.join(DATA_FOLDER.XEFR,schema +'.csv')
        schema_id = mongo.get_schema_id(schema)
        if schema_id != 0:
            curl_cmd = xefr.get_endpoint_curl(schema_id,'data',True)
            persist_data(curl_cmd,schema)
        else:
            print(f"Schema {schema} not found")

def print_schema_details(schema_tag,use_name=True):
    """
    Prints the details of a schema if they exist !
    """
    if use_name:
        details = mongo.get_schema_details(schema_tag)
    elif use_name == False:
        details = mongo.get_schema_details_by_id(schema_tag)
    else:
        raise ValueError("use_name must be True or False")

    if details != 0:
        print()
        print("{:<30} {:<10}".format(details['name'], details['id']),"\n")

        for attribute in details['attributes']:
            print("{:<20} {:<10} {:<10}".format(attribute['name'], attribute['type'], attribute['id']))

    else:
        print(f"\nSchema {schema_tag} not found.\n")

if __name__ == '__main__':

    run_config = f"Running from {instance} Mongo instance on {database} database"
    logger.info(run_config)
    print(run_config)

    # Working code tested
    #download_schemas_data(['TSP UK Placements Forecast'])
    #forecast_schemas = ['TSP UK Placements Forecast','Metrics Working Days']

    forecast_schemas = ['TSP UK Placements Forecast','Metrics Working Days']
    schema_ids = ['74633e84-deba-4720-8181-c07629a1c665','6d422fc3-3d24-4650-b069-def85c1f888c']


    for schema in forecast_schemas:
        #download_schemas_data([schema])
        print_schema_details(schema)

    for schema in schema_ids:
        #download_schemas_data([schema])
        print_schema_details(schema,False)

    # tsp_uk_placements_forecast_details = mongo.get_schema_details('TSP UK Placements Forecast')
    # metrics_working_days = mongo.get_schema_details('Metrics Working Days')

    # print_schema_details(tsp_uk_placements_forecast_details)
    # print_schema_details(metrics_working_days)

    mongo.disconnect()

    # COPY_TYPES = ["replace","append"]

    # portal_details = {
    #     "from_portal":"UK NFI",
    #     "to_portal" :"US NFI",
    #     "copy_object":"portletHolder",
    #     "copy_type":"replace",
    #     "append_object":"childPortals"
    # }


    # formatting_details = {
    #         "enableCsvUpload": True,
    #         "autosizeColumns": True,
    #         "density": "VERY_COMPACT",
    #         "banded": True,
    # }

    # schema_details = {
    #     "from_schema":"Metrics GBP Forex Daily",
    #     "to_schema" :"Metrics USD Forex Daily"
    # }

    # # Tested and works
    # # portlets_update
    #("FOREX",formatting_details, ['FOREX Issues','Missing Day Rates'])
    # # backup_portal("FOREX")
    # #report_dependencies("New Deals")


    # update_type = "schemas"

    # if update_type not in ['schemas','portals']:
    #     raise ValueError("schemas or portals only")

    # combined_details = {**portal_details, **schema_details}
    # data_list, new_json_file = update_defs(update_type,combined_details)

    # with open(new_json_file, 'w') as json_file:
    #     try:
    #         json.dump(data_list, json_file, indent=4)
    #         print(f"File {new_json_file} created")
    #     except:
    #         print("Bad do do happened")