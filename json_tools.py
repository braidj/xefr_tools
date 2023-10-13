import common_funcs as cf
import json
#TODO: Add logging
#TODO: Set download path in single location

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

def extract_json(item_list,item_type):
    """
    Handle extracting the schema(s) or portal(s) from the 
    source json files and writing them to individual files
    """
    if item_type not in cf.permitted_types:
        raise Exception(f"Type {item_type} not permitted, only {permitted_types}")
    
    source_file = cf.get_source_json(item_type)

    with open(source_file, 'r') as file:
        data = json.load(file)

        for item in data:

            if item_type == 'schemas':
                title = item.get("name")

            if item_type == 'portals':
                title = item.get("title")

            if title in item_list:
                
                outputfile = cf.get_output_json(item_type, title)

                with open(outputfile, 'w') as file:
                    try:
                        json.dump([item], file, indent=4)
                        print("{:<4} {:<20} {:<5} {:<20}".format(item_type,title, '---->',outputfile))

                    except Exception as e:
                        print(f"Error writing {title} to {outputfile}")
                        print(e)

def nw_schema_copy(input_file, details):
    #TODO not working for pivot views
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


if __name__ == '__main__' :

    report_items('schemas')
    #report_items('portals')

    # extract_json(['US NFI View'],'schemas')
    # extract_json(['US NFI'],'portals')
