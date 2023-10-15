import os
import sys
import json
import common_funcs
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

    downloads = common_funcs.get_download_directory()
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

def portlets_update(portal_name,formatting_details,skip_items):
    """
    Apply a standard set of formats against all portlets in a portal
    Will skip any tabbed items in the skip_items list
    Relationships between portals and portlets are:
    1 - 1 portal to portletHolder
        1 - M portletHolder to childPortals
    """

    downloads = common_funcs.get_download_directory()
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
    
    downloads = common_funcs.get_download_directory()
    source_file = f"{downloads}{os.sep}{schema_or_portal}.json"
    destination_file = f"{downloads}{os.sep}updated_{schema_or_portal}.json"

    data_list = []

    if schema_or_portal == "schemas":
        data_list = [schema_copy(source_file, details)]

    if schema_or_portal == "portals":
        data_list = [portal_copy_section(source_file, details)]

    return data_list, destination_file


def ignore():
    """
    Code block to be ignored for now
    """
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

    pass

if __name__ == '__main__':

    run_config = f"Running from {instance} Mongo instance on {database} database"
    logger.info(run_config)
    print(run_config)

