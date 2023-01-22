import json
import os

"""
    @Notice: This function will return the first line of a file
    @Dev:   We open and read the first line of the file path given in parameter
"""
def read_first_line(file_path: str):
    if not os.path.exists(file_path):
        return ''
    with open(file_path, "r") as file:
        return file.readline()

"""
    @Notice: This function will write to a file
    @Dev:   We open and write the content to the file path
"""
def write_file(file_path: str, content: str):
    with open(file_path, 'w') as file:
        file.write(content)

"""
    @Notice: This function will return the json in a file
    @Dev:   We open the file at the file path and return it as a json using json.load
"""
def get_dict_file(file_path: str):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

