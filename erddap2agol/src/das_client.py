import sys, os, requests, datetime 
import json
from collections import OrderedDict
from . import erddap_client as ec

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def parseDasResponse(response_text):
    data = OrderedDict()
    current_section = None
    section_name = None

    for line in response_text.strip().splitlines():
        line = line.strip()

        if line.startswith("Attributes {"):
            continue

        if line.endswith("{"):
            section_name = line.split()[0]
            current_section = OrderedDict()
            data[section_name] = current_section
            continue

        if line == "}":
            section_name = None
            current_section = None
            continue

        if current_section is not None:
            parts = line.split(maxsplit=2)
            if len(parts) == 3:
                datatype, description, value = parts
                current_section[description] = {
                    "datatype": datatype,
                    "value": value.strip('";')
                }

    return data

def getConfDir():

    agol_home = os.getenv('AGOL_HOME', '/arcgis/home')
    base_dir = agol_home
    das_conf_dir = os.path.join(base_dir, 'e2a_das_conf')

    os.makedirs(das_conf_dir, exist_ok=True)
    return das_conf_dir

def checkForJson(datasetid: str) -> bool:
    das_conf_dir = getConfDir()
    filepath = os.path.join(das_conf_dir, f'{datasetid}.json')
    return os.path.exists(filepath)


#need this function to convert OrderedDict to dict for json
def convertToDict(data):
    if isinstance(data, OrderedDict):
        return {k: convertToDict(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convertToDict(i) for i in data]
    else:
        return data

def saveToJson(data, datasetid: str) -> str:
    das_conf_dir = getConfDir()
    filepath = os.path.join(das_conf_dir, f'{datasetid}.json')
    with open(filepath, 'w') as json_file:
        json.dump(data, json_file, indent=4)
    return filepath

def openDasJson(datasetid):
    das_conf_dir = getConfDir()
    filepath = os.path.join(das_conf_dir, f'{datasetid}.json')
    try:
        with open(filepath, 'r') as json_file:
            data = json.load(json_file)
            if "error" in data and data["error"]["Found"] is not None:
                print(f"File {filepath} does not contain data.")
                return None
            else:
                return data
    except FileNotFoundError:
        print(f"File {filepath} not found.")
        return None

def getTimeFromJson(datasetid):
    das_conf_dir = getConfDir()
    filepath = os.path.join(das_conf_dir, f'{datasetid}.json')
    with open(filepath, 'r') as json_file:
        data = json.load(json_file)
    
    try:
        time_str = data['time']['actual_range']['value']
        start_time_str, end_time_str = time_str.split(', ')
        start_time = int(float(start_time_str))
        end_time = int(float(end_time_str))
        return start_time, end_time
    except Exception as e:
        print(f"Error getting time from JSON: {e}")
        return None
    
# This function doesn't go anywhere yet
# should be used to check for core attributes (lat lon time) in the dataset
def checkDataValidity(dasJson) -> bool:
    for key, value in dasJson.items():
        if isinstance(value, dict):
            if {"latitude", "longitude", "time"} not in key:
                return False
            else:
                return True
    
def convertFromUnix(time):
    try:
        start = datetime.datetime.utcfromtimestamp(time[0]).strftime('%Y-%m-%dT%H:%M:%S') 
        end = datetime.datetime.utcfromtimestamp(time[1]).strftime('%Y-%m-%dT%H:%M:%S')
        return start, end
    except Exception as e:
        print(f"Error converting from Unix, probably a bad server response: {e}")
        return None

def convertFromUnixDT(time_tuple):
    start_unix, end_unix = time_tuple
    start_datetime = datetime.datetime.utcfromtimestamp(start_unix)
    end_datetime = datetime.datetime.utcfromtimestamp(end_unix)
    return start_datetime, end_datetime


    
#Expand this function to check the dtype of attributes 
# Add string handling
# def getActualAttributes(dasJson, erddapObject: ec.ERDDAPHandler) -> list:
#     attributes_set = set() 
#     for key, value in dasJson.items():
#         if isinstance(value, dict):
#             if "actual_range" in value and "_qc_" not in key and key not in {"latitude", "longitude", "time"}:
#                 if "coverage_content_type" in value and value["coverage_content_type"].get("value") == "qualityInformation":
#                     continue
#                 attributes_set.add(key)

#     setattr(erddapObject, "attributes", list(attributes_set))
#     return list(attributes_set)

def displayAttributes(timeintv: int , attributes: list) -> None:
    print(f"\nThere are {timeintv} days worth of records")
    print(f"\nAttributes: {attributes}")



def getActualAttributes(dasJson, erddapObject: ec.ERDDAPHandler) -> list:
    attributes_set = set()
    for var_name, var_attrs in dasJson.items():
        if not isinstance(var_attrs, dict):
            continue
            # Added qartod to ignore (12/2) 
        if "_qc_" in var_name or "qartod_" in var_name or var_name in {"latitude", "longitude", "time"}:
            continue

        coverage_content_type_entry = var_attrs.get('coverage_content_type', {})
        coverage_content_type = coverage_content_type_entry.get('value', '')
        if coverage_content_type == 'qualityInformation':
            continue

        # Include if 'actual_range' exists
        if 'actual_range' in var_attrs:
            attributes_set.add(var_name)
            continue

        # Include string variables with more than one attribute
        if len(var_attrs) == 1:
            attributes_set.add(var_name)

    setattr(erddapObject, "attributes", list(attributes_set))
    return list(attributes_set)
