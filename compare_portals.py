"""
Report on schemas, names, titles and ids from a portal file
"""

import json
import sys
import os
import re
import common_funcs as cf

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, 'xerini_utils'))
sys.path.append(utils_path)

import mongo_connector
import utilities

database = 'xefr-signify-dev' #Set the database name here
instance = "LOCAL"
download_folder = cf.setup_local_folder(instance,database)

logging = utilities.MyLogger()
logging.reset_log()
logger = logging.getLogger()

mongo = mongo_connector.Mongo(instance,database,download_folder,logger)

def extract_attributes(label,pattern,json_str):
    """Return distinct values for a given pattern"""
    matches = re.findall(pattern, json_str)
    distinct_schema_ids = set(matches)

    return report_results(label, distinct_schema_ids)

def report_results(label, results):
    """Sort and report results"""
    output = []
    output.append(f"\n{label} [{len(results)}]")

    schemaName = ""

    results = sorted(results)

    for item in results:
        if label == "Schemas":
            schemaDetails = mongo.get_schema_details_by_id(item)

            output.append(f"{item} = {schemaDetails['name']}")
        else:
            output.append(f"{item}")

    return output
def create_report(source):
    """Create a report from a portal file"""
    report_name = os.path.splitext(os.path.basename(source))[0] + ".dat"

    with open(source, 'r') as file:
        json_str = file.read()

    data = []
    data.extend(extract_attributes( "Schemas",r'"schemaId":\s*"([^"]+)"',json_str))
    data.extend(extract_attributes( "Titles",r'"title":\s*"([^"]+)"',json_str))
    data.extend(extract_attributes( "Names",r'"name":\s*"([^"]+)"',json_str))
    data.extend(extract_attributes( "IDs",r'"id":\s*"([^"]+)"',json_str))

    with open(report_name, 'w') as file:
        file.writelines([f"{item}\n" for item in data])


def main():

    US = '/Users/jasonbraid/Downloads/xefr/LOCAL_xefr-signify-dev/15Nov23_1258_portals US NFI.json'
    UK = '/Users/jasonbraid/Downloads/xefr/LOCAL_xefr-signify-dev/15Nov23_1446_portals UK NFI.json'
    create_report(US)
    create_report(UK)

    print("Done")

if __name__ == "__main__":
    main()
