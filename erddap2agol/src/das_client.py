import sys, os, datetime 
from datetime import datetime, timedelta, timezone
import json
from collections import OrderedDict
from . import erddap_wrangler as ec
from . import data_wrangler as dw
from typing import Any, List

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def parseDasResponse(response_text) -> OrderedDict:
    """
    Parse the DAS response text into an OrderedDict for conversion to JSON.
    multi-line handling for NC_Global attributes
    """
    data = OrderedDict()
    current_section = None
    section_name = None

    inNcGlobal = False
    inMultilineValue = False
    mline_attr_name = None
    mline_attr_type = None
    mline_value_lines = []

    # A state for if we see a license"
    check_for_quotes = False

    lines = response_text.strip().splitlines()

    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Skip 'Attributes {' line
        if line.startswith("Attributes {"):
            continue
        
        # Check if line ends with '{' => new section
        if line.endswith("{"):
            section_name = line.split()[0]  # e.g. "NC_GLOBAL"
            current_section = OrderedDict()
            data[section_name] = current_section
            
            # if it's NC_GLOBAL, we allow multi-line
            inNcGlobal = (section_name == "NC_GLOBAL")
            continue
        
        # Check if line == '}' => end of section
        if line == "}":
            # end current section
            inNcGlobal = False
            section_name = None
            current_section = None
            check_for_quotes = False
            continue
        
        # If we're in a multline joining inside NC_GLOBAL
        if inNcGlobal and inMultilineValue:
            mline_value_lines.append(line)
            # check if we reached the end: line ends with '";'
            if line.endswith('";'):
                inMultilineValue = False
                combined_text = "\n".join(mline_value_lines)
                combined_text = combined_text.rstrip('";').strip()
                
                current_section[mline_attr_name] = {
                    "datatype": mline_attr_type,
                    "value": combined_text
                }
                # reset
                mline_attr_name = None
                mline_attr_type = None
                mline_value_lines = []
            continue
        
        
        if inNcGlobal and check_for_quotes:
            check_for_quotes = False
            inMultilineValue = True
            mline_value_lines = [line]
            continue
        
        # Normal one-line parse
        if current_section is not None:
            parts = line.split(maxsplit=2)
            if len(parts) == 3:
                datatype, description, value = parts
                
                # if we're in NC_GLOBAL and it's a string
                if inNcGlobal and datatype == "String":
                    # Check if value includes quotes
                    if value.startswith('"') and not value.endswith('";'):
                        # => multi-line starts on same line
                        inMultilineValue = True
                        mline_attr_type = datatype
                        mline_attr_name = description
                        mline_value_lines = [value]
                    elif not value.startswith('"'):
                        # This means we have something like "String license" with no quotes
                        # so the next line(s) will be the quotes
                        mline_attr_type = datatype
                        mline_attr_name = description
                        check_for_quotes = True
                    else:
                        # single-line string
                        val_clean = value.strip('";')
                        current_section[description] = {
                            "datatype": datatype,
                            "value": val_clean
                        }
                else:
                    # typical single-line attribute
                    val_clean = value.strip('";')
                    current_section[description] = {
                        "datatype": datatype,
                        "value": val_clean
                    }

    return data


def getConfDir():

    agol_home = os.getenv('AGOL_HOME', '/arcgis/home')
    base_dir = agol_home
    das_conf_dir = os.path.join(base_dir, 'e2a_das_conf')

    os.makedirs(das_conf_dir, exist_ok=True)
    return das_conf_dir

def cleanConfDir():
    agol_home = os.getenv('AGOL_HOME', '/arcgis/home')
    base_dir = agol_home
    filepath = os.path.join(base_dir, 'e2a_das_conf')
    if os.path.exists(filepath):
        for filename in os.listdir(filepath):
            if filename.endswith(".json"):
                full_path = os.path.join(filepath, filename)
                try:
                    os.remove(full_path)
                except Exception as e:
                    print(f"An unexpected error occurred while deleting {full_path}: {e}")
    else:
        print(f"The directory {filepath} does not exist.")

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
        #Now this is programming 
        try:
            if time[0] < 0:
                start = datetime(1970, 1, 1) + timedelta(seconds=time[0])
            else:
                start = datetime.fromtimestamp(time[0], tz=timezone.utc) 
            if time[1] < 0:
                end = datetime(1970, 1, 1) + timedelta(seconds=time[0])
            else:
                end = datetime.fromtimestamp(time[1], tz=timezone.utc)
                            
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
                    var_name in {"latitude", "longitude"}):
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