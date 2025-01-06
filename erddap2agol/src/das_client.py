import sys, os, datetime 
from datetime import datetime, timedelta
import json
from collections import OrderedDict
from . import erddap_wrangler as ec
from . import data_wrangler as dw
from typing import Any, List

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def parseDasResponse(response_text):
    """Parse the DAS response text into an ordered dictionary for conversion to JSON"""
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



def getTimeFromJson(datasetid) -> tuple:
    """Gets time from JSON file, returns max and min time as a tuple"""
    def convertFromUnix(time):
        """Convert from unix tuple to datetime tuple"""
        #Now this is some elementary programming 
        try:
            if time[0] < 0:
                start = datetime(1970, 1, 1) + timedelta(seconds=time[0])
            else:
                start = datetime.fromtimestamp(time[0]).strftime('%Y-%m-%dT%H:%M:%S')  
            if time[1] < 0:
                end = datetime(1970, 1, 1) + timedelta(seconds=time[0])
            else:
                end = datetime.fromtimestamp(time[1]).strftime('%Y-%m-%dT%H:%M:%S')
                            
            return start, end
        except Exception as e:
            print(f"Error converting from Unix: {e}")
            return None
    
    das_conf_dir = getConfDir()
    filepath = os.path.join(das_conf_dir, f'{datasetid}.json')
    with open(filepath, 'r') as json_file:
        data = json.load(json_file)
    
    try:
        time_str = data['time']['actual_range']['value']
        start_time_str, end_time_str = time_str.split(', ')
        start_time = (float(start_time_str))
        end_time = (float(end_time_str))
        time_tup = start_time, end_time
        return convertFromUnix(time_tup)
    except Exception as e:
        print(f"Error getting time from JSON: {e}")
        return None
    

def convertFromUnixDT(time_tuple):
    start_unix, end_unix = time_tuple
    start_datetime = datetime.datetime.utcfromtimestamp(start_unix)
    end_datetime = datetime.datetime.utcfromtimestamp(end_unix)
    return start_datetime, end_datetime


def displayAttributes(timeintv: int , attributes: list) -> None:
    print(f"\nThere are {timeintv} days worth of records")
    #print(f"\nAttributes: {attributes}")

def getActualAttributes(data_Obj: Any) -> List[str]:
    """Load DAS JSON file and extract relevant attributes while filtering out QC variables.
    datasetid (str): The dataset identifier
        
    Returns list[str]: List of valid attribute names or None if error
    """
    dataset_id = data_Obj.dataset_id
    has_lat = False
    has_lon = False

    das_conf_dir = getConfDir()
    filepath = os.path.join(das_conf_dir, f'{dataset_id}.json')
    
    try:
        with open(filepath, 'r') as json_file:
            data = json.load(json_file)
            if "error" in data and data["error"]["Found"] is not None:
                print(f"File {filepath} does not contain data.")
                return None
                
            attributes_set = set()
            for var_name, var_attrs in data.items():
                if not isinstance(var_attrs, dict):
                    continue

                if var_name == "latitude":
                    has_lat = True
                elif var_name == "longitude":
                    has_lon = True

                # Skip QC and coordinate variables
                if ("_qc_" in var_name or 
                    "qartod_" in var_name or 
                    var_name.endswith("_qc") or
                    var_name in {"latitude", "longitude", "time"}):
                    continue

                # Check coverage content type
                coverage_content_type_entry = var_attrs.get('coverage_content_type', {})
                coverage_content_type = coverage_content_type_entry.get('value', '')
                if coverage_content_type in ('qualityInformation', 'other'):
                    continue

                # Include variables with actual_range or single attribute
                if 'actual_range' in var_attrs or len(var_attrs) == 1:
                    attributes_set.add(var_name)
            
            if not (has_lat and has_lon):
                data_Obj.has_error = True

            return list(attributes_set)
            
    except FileNotFoundError:
        print(f"File {filepath} not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error decoding JSON from {filepath}")
        return None
    
# This function doesn't go anywhere yet
# should be used to check for core attributes (lat lon time) in the dataset
# def checkDataValidity(dasJson) -> bool:
#     for key, value in dasJson.items():
#         if isinstance(value, dict):
#             if {"latitude", "longitude"} not in key:
#                 return False
#             else:
#                 return True