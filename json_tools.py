import common_funcs as cf
import json
import os
from tabulate import tabulate
import re

#TODO: Add logging

def schema_report(schema_name):
    """
    Outputs a formatted report displaying the key information for a schema
    """

    item_type = 'schemas'
    schema = schema_name.strip()
    source_file = cf.get_source_json(item_type)
    print(source_file)

    with open(source_file, 'r') as file:
        data = json.load(file)

        for item in data:

            if item.get("name") == schema:

                cf.colour_text(f'Schema Name: {schema}',"BLUE")
                cf.colour_text(f'Description: {item.get("description")}',"BLUE")

                raw_attributes = item.get("attributes")
                table_data = [[col["name"], col["type"], col["id"]] for col in raw_attributes]
                print(tabulate(table_data, headers=["Name", "Type", "ID"], tablefmt="grid"))

def report_items(item_type):
    """
    Returns alpahbetical list of items of type item_type
    """
    if item_type not in cf.permitted_types:
        raise Exception(f"Type {item_type} not permitted, only {cf.permitted_types}")
    
    source_file = cf.get_source_json(item_type)
    results = {}

    with open(source_file, 'r') as file:
        data = json.load(file)

        for item in data:

            id = item.get("id")

            if item_type == 'schemas':
                title = item.get("name")
            
            if item_type == 'portals':
                title = item.get("title")
            
            results[id] = title

    sorted_results = sorted(results.items(), key=lambda x: x[1])
    print(f"\nAll {item_type} items\n")
    for i, stuff in enumerate(sorted_results,1):
        id, name = stuff
        print("{:<5} {:<35} {:<20}".format(i,name, id))

def extract_json(object_name,item_type,backup=True):
    """
    Handle extracting the schema(s) or portal(s) from the 
    source json files and writing them to individual files
    If backup is set as True, will add a timestamp to the name
    """
    if item_type not in cf.permitted_types:
        raise Exception(f"Type {item_type} not permitted, only {cf.permitted_types}")
    
    source_file = cf.get_source_json(item_type)

    with open(source_file, 'r') as file:
        data = json.load(file)

        for item in data:

            if item_type == 'schemas':
                title = item.get("name")

            if item_type == 'portals':
                title = item.get("title")

            if title in object_name:
                
                outputfile = cf.get_output_json(item_type, title)

                if backup:
                    outputfile = cf.add_ts_prefix(outputfile)

                with open(outputfile, 'w') as file:
                    try:
                        json.dump([item], file, indent=4)
                        print("{:<4} {:<20} {:<5} {:<20}".format(item_type,title, '---->',outputfile))

                    except Exception as e:
                        print(f"Error writing {title} to {outputfile}")
                        print(e)

def copy_schema(source_name, new_name, new_name_id):
    """
    Copy an existing schema to a new schema, can handle mongo schemas
    N.B. The new schema must already exist.
    Results in new file being created
    """

    source_file = cf.get_source_json("schemas")

    with open(source_file, 'r') as file:
        data = json.load(file)

        for item in data:

            id = item.get("id")
            title = item.get("name")

            if title == source_name:

                item["id"] = new_name_id
                item["name"] = new_name

                if "pipelineText" in item:
                    pipeline_str = item.get("pipelineText")
                    pipeline_str = pipeline_str.replace(source_name, new_name)
                    item["pipelineText"] = pipeline_str

                outputfile = cf.get_output_json("schemas", new_name)

                with open(outputfile, 'w') as file:
                    try:
                        json.dump([item], file, indent=4)
                        print("{:<20} {:<20} {:<5} {:<20}".format(new_name,new_name_id, '---->',outputfile))

                    except Exception as e:
                        print(f"Error writing {title} to {outputfile}")
                        print(e)

def get_pipeline_text(schema_name):
    """
    Returns the pipeline text for a given schema
    """
    source_file = cf.get_source_json("schemas")

    with open(source_file, 'r') as file:
        data = json.load(file)

        for item in data:

            title = item.get("name")

            if title == schema_name:

                if "pipelineText" in item:
                    pipeline_str = item.get("pipelineText")

                    return pipeline_str

def get_pipeline_columns(schema_name):
    """
    Returns a sorted, distinct list of columns referenced in the pipeline
    """
    schema = schema_name.strip()
    pipeline = get_pipeline_text(schema)
    pattern = r'\{([^}]+)\}'

    matches = re.findall(pattern, pipeline)
    distinct_sorted_matches = sorted(set(matches))# Remove duplicates, sort the matches

    column_details = [x for x in distinct_sorted_matches if "$" not in x]
    all_tables = [x.split('.')[0] for x in column_details]
    distinct_tables = sorted(set(all_tables))

    cf.colour_text(f"{schema} pipeline depends on {len(distinct_tables)} tables","BLUE")
    for table_nos, table in enumerate(distinct_tables,1):
        print()
        cf.colour_text(f"{table_nos}: {table}","BLUE")

        for col in column_details:
            if col.startswith(table):
                print(f"\t\t{col}")

if __name__ == '__main__' :

    get_pipeline_columns("UK Forecast View")
