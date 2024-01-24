import os
import json
from collections import namedtuple

SchemaDetails = namedtuple("SchemaDetails", "name, keys, values, data")

class Schema:

    def __init__(self, source,root_folder = "/Users/jasonbraid/Downloads/XEFR_TOOLS"):

        self.source = source

        if not os.path.isfile(source):
            raise FileNotFoundError(f"{source} does not exist.")
        
        self.file_name = os.path.basename(source)
        self.instance = source.replace("/Users/jasonbraid/Downloads/XEFR_TOOLS/", "").replace(self.file_name, "").split("/")[0]
        self.details = {}
        self.data = self.__load_json(source)
        self.__load_details(self.data)
        self.set_keys = set(self.details.keys())
        self.schema_names = list(self.details.keys())

        self.environment = self.instance.split("_")[0]
        self.mongod = self.instance.split("_")[1]

        print(f"Loaded Environment: {self.environment} for {self.mongod} ({len(self.details)} schemas)")
    
    def get_attributes(self,schema_name):
        """Returns the attributes for the specified schema
        with the _id removed as it's not relevant to the comparison"""

        if schema_name in self.details:
            without_id = {k: v for k, v in self.details[schema_name].items() if k != '_id'}
            data = without_id
            keys = list(data.keys())
            values = list(data.values())
            return SchemaDetails(schema_name, keys, values, data)
        else:
            raise KeyError(f"Schema '{schema_name}' not found for {self.environment}")

    def properties(self):
        for i in self.__dict__:
            print(f"{i} : {self.__dict__[i]}")

    def __load_json(self,file_path: str):

        with open(file_path, "r") as f:
            return json.load(f)
        
    def __load_details(self,data):
        """Load all the keys, but remove '_id' as it's not relevant to the comparison"""

        for item in data: 
            self.details[item['name']] = item

class Compare:
    
        def __init__(self, data_a: list, data_b: list):

            self.data_a = data_a
            self.data_b = data_b

            self.compare()
    
        def compare(self):

            if len(self.data_a) != len(self.data_b):
                raise ValueError("Cannot compare lists of different lengths")
            
            for i in range(len(self.data_a)):
                if self.data_a[i] != self.data_b[i]:
                    print(f"Index {i}")
                    print(self.data_a[i])
                    print(self.data_b[i])

def compare_dictionaries(schema_a: Schema, schema_b: Schema):

    unique_to_schema_a = schema_a.set_keys - schema_b.set_keys 
    unique_to_schema_b = schema_b.set_keys - schema_b.set_keys 

    # Report on schemas
    if unique_to_schema_a:
        print(f"Schemas unique to {schema_a.environment}: {unique_to_schema_a}")
    if unique_to_schema_b:
        print(f"Schemas unique to {schema_b.environment}:  {unique_to_schema_b}")

    if schema_a.set_keys == schema_b.set_keys:
        print(f"\nEnvironments {schema_a.environment} and {schema_b.environment} have the same Schemas\n")

    # Compare details for schemas present in both environments
    common_schemas = schema_a.set_keys.intersection(schema_b.set_keys )

    for item in common_schemas:

        item_a = schema_a.get_attributes(item)
        item_b= schema_b.get_attributes(item)

        # Check keys first
        if item_a.keys != item_b.keys:
            print(f"Schema '{item}' has different keys")
            print(f"{schema_a.environment} : {item_a.keys}")
            print(f"{schema_b.environment} : {item_b.keys}")
        else:
            print(f"Schema '{item}' has the same attribute keys")

        # Check values
        if item_a.values != item_b.values:
            print(f"Schema '{item}' has different values\n\n")

            mismatch_indexes = [i for i, (x, y) in enumerate(zip(item_a.values, item_b.values)) if x != y]
            for i in mismatch_indexes:
                check = Compare(item_a.values[i], item_b.values[i])
                print()
        else:
            print(f"Schema '{item}' has the same attribute values")

if __name__ == "__main__":

    local_schema = "/Users/jasonbraid/Downloads/XEFR_TOOLS/LOCAL_xefr-zhero-dev/schemas.json"
    schema_a = Schema(local_schema)

    dev_schema = "/Users/jasonbraid/Downloads/XEFR_TOOLS/DEV_zhero-dev/schemas.json"
    schema_b = Schema(dev_schema)

    compare_dictionaries(schema_a, schema_b)
