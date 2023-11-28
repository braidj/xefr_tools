import common_funcs as cf
import os

def parse_schema(file_path):
    """
    converts a schema file into a dictionary of schema names and fields
    """

    schema = {}
    current_schema = None
    with open(file_path, 'r') as file:
        for line in file:
            if line.startswith("Schema Name:"):
                current_schema = line.split("Schema Name:")[1].strip()
                schema[current_schema] = {}
            elif line.strip() and current_schema and not line.startswith("Name") and not line.startswith("Description"):
                parts = line.split()
                field = ' '.join(parts[:-1])  # Field name
                field_type = parts[-1]  # Field type
                schema[current_schema][field] = field_type
    return schema

def compare_schema_types(schema1, schema2):
    """
    Reports on any differences between attribute types in matching schemas
    """

    type_differences = {}
    for schema_name, fields in schema1.items():
        if schema_name in schema2:
            for field, type1 in fields.items():
                type2 = schema2[schema_name].get(field)
                if type2 and type1 != type2:
                    type_differences.setdefault(schema_name, []).append((field, type1, type2))

    return type_differences
def schema_summary(label,schema):
    """Returns a summary of the schema"""
    print(f"\t{label} contains: {len(schema)} schemas")
    print(f"\t\tTotal nos of attributes:")

def report_schema_differences(left_file, right_file):
    """
    Reports on any differences between attribute types in matching schemas
    """

    left_schema = parse_schema(left_file)
    right_schema = parse_schema(right_file)

    print(f"Comparing:\n{left_file}\n{right_file}\n")
    schema_summary("Left file",left_schema)
    schema_summary("Right file",right_schema)
    
    matched_schemas = set(left_schema.keys()) & set(right_schema.keys())
    print(f"\nMatching schemas: {len(matched_schemas)}")

    differences = compare_schema_types(left_schema, right_schema)

    print(f"\nMatching schemas with differences: {len(differences)}")
    for i in differences:
        print(i)
        print(differences[i])
        print()

    unmatched_schemas = set(left_schema.keys()) ^ set(right_schema.keys())
    print(f"\nUnmatched schemas: {len(unmatched_schemas)}")
    print(f"\n{unmatched_schemas}")

def get_file_path(instance,database):
    """
    Returns the file path for the supplied instance and database
    """
    download_folder = cf.setup_local_folder(instance,database)
    schema_file = os.path.join(download_folder,f"{instance}_recon_schema_defs.txt")
    return schema_file


if __name__ == '__main__':

    LOCAL = get_file_path("LOCAL","xefr-signify-dev")
    DEV = get_file_path("DEV","xefr-signify-dev")
    PROD = get_file_path("PROD","xefr-signify")

    report_schema_differences(LOCAL,PROD)


