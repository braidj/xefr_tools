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
logger.info('xefr_tools.p started')

xefr = xefr_endpoints.EndPoints('DEV',"csv",logger)
mongo = mongo_connector.Connector('xefr-signify-dev','local',logger)

def persist_data(curl_command,schema_name):
    """
    Run the curl command and save the output to a file
    """
    logger.debug('persist_data: %s', locals())

    curl_output = subprocess.check_output(curl_command, shell=True)

    output_file = os.path.join(DATA_FOLDER.XEFR,schema_name)
    utilities.check_csv_folder_exists(output_file,True,logger)

    with open(output_file, 'wb') as file:
        file.write(curl_output)
    try:
        df = pd.read_csv(output_file)

        if schema_name in DATA_FOLDER.sort_orders: # Require data to be sorted
            utilities.data_frame_sort_and_save(df,output_file,DATA_FOLDER.sort_orders[schema_name],logger)

        record_count = len(df)
       # df.to_csv(output_file,index=False,encoding='utf-8') # ensure in same format for reconciliations
    except pd.errors.EmptyDataError:
        record_count = 0

    print(f"Data saved to {output_file} ({record_count} rows)\n")

def backup_portal(portal_name):
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

def update_portlets(portal_name,formatting_details,skip_items):
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

def copy_schema(input_file, details):
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

def copy_portal_section(input_file, details):
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
        data_list = [copy_schema(source_file, details)]

    if schema_or_portal == "portals":
        data_list = [copy_portal_section(source_file, details)]

    return data_list, destination_file

def download_schemas_data(schema_list):
    """
    Downloads the data from specified schema
    """

    for schema in schema_list:

        output_file = os.path.join(DATA_FOLDER.XEFR,schema +'.csv')
        schema_id = mongo.get_schema_id(schema)
        if schema_id != 0:
            curl_cmd = xefr.get_endpoint_curl(schema_id,'data',True)
            persist_data(curl_cmd,output_file)
        else:
            print(f"Schema {schema} not found")

if __name__ == '__main__':


    all_schemas =[i['name'] for i in mongo.all_schemas()]

    download_schemas_data(all_schemas)

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
    # # update_portlets("FOREX",formatting_details, ['FOREX Issues','Missing Day Rates'])
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