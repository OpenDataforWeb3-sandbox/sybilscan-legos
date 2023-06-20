import json
import os
from datetime import datetime
import re
import yaml

def load_json(file_name):
    """
    """
    try:
        with open(file_name, "r") as f:
            file_data = f.read()

        return json.loads(file_data)
    except Exception as e:
        logger.info(e)
        return None

def list_files_ending_with(folder_path, pattern):
    """ """
    file_list = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith(pattern):
            file_list.append(os.path.join(folder_path,file_name))
    return file_list

def timestamp_to_utc_string(timestamp):
    """
    converts timestamp to utc datetime string
    """
    utc_datetime = datetime.utcfromtimestamp(timestamp)
    return utc_datetime.strftime("%Y-%m-%d %H:%M:%S")

def parse_methods(class_obj, pattern):
    """
    retrieves methods in a class that starts with the given pattern 
    and replaces the pattern with empty string
    """
    method_list = []
    for attr_name in dir(class_obj):
        attr_value = getattr(class_obj, attr_name)
        if callable(attr_value) and re.search(pattern, attr_name):
                method_list.append(re.sub(pattern, '', attr_name))

    return method_list

def split(rows, chunk_size):
    """
    splits given list into chunks
    """
    for i in range(0, len(rows), chunk_size):
        yield rows[i:i + chunk_size]

def read_yaml(yaml_file):
    """
    This function takes in a yaml file path and returns the data loaded from the file
    """
    with open(yaml_file, 'r') as content:
        yaml_data = yaml.safe_load(content)

    return yaml_data