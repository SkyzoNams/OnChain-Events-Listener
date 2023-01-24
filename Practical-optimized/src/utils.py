import json

"""
    @Notice: This function will return the json in a file
    @Dev:   We open the file at the file path and return it as a json using json.load
"""
def get_dict_file(file_path: str):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

