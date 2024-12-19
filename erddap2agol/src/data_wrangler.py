from . import erddap_client as ec
from . import das_client as dc
from logs import updatelog as ul
from src.utils import OverwriteFS
from arcgis.gis import GIS

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, List

import datetime, requests, re, math
from datetime import timedelta, datetime

################## Experimenting with new class ##################

@dataclass
class DatasetWrangler:
    """Represents a single ERDDAP dataset with metadata and time params"""
    def __init__(self, dataset_id):
        self.dataset_id = dataset_id
        self.server = None
        self.rowCount = None
        self.attribute_list = None
        self.startTime = None
        self.endTime = None
        self.needs_Subset = None
        self.subsetDict = None
        self.is_processed = False
        self.DAS_filepath = None
        self.DASresponse = None
    
    def __post_init__(self):
        """Sets attribute self.server to current ec.ERDDAPHandler instance"""
        self.server = ec.ERDDAPHandler.server
    
    # These methods will need to be wrapped for list input
    def getDas(self) -> None:
        """Fetch DAS for dataset and write attributes. Returns nothing, sets attributes for DAS_response and DAS_filepath"""
        def parseDas(self) -> None:
            """sets attributes for attributes, startTime, endTime"""
            if self.DASresponse is False or self.DASresponse is None:
                print(f"\nInvalid dataset due to bad response and/or filepath")
                pass
            else:
                attribute_list = dc.getActualAttributes(self.dataset_id)
                setattr(self, "attributes", attribute_list)
                time_tup = dc.getTimeFromJson(self.dataset_id)
                start = time_tup[0]
                end = time_tup[1]
                setattr(self, "startTime", start)
                setattr(self, "endTime", end)

        url = f"{self.server}{self.dataset_id}.das"
        response = requests.get(url)
        
        # if bad response set DASresponse attr to false
        # can access later
        # instead of returning fp we assigned it as an obj attribute

        if response.status_code != 200:
            setattr(self, 'DASresponse', False)
        
        else:
            setattr(self, 'DASresponse', True)
            DAS_Dict = dc.convertToDict(dc.parseDasResponse(response.text))
            outpath = dc.saveToJson(DAS_Dict)
            setattr(self, "DAS_filepath", outpath)
            parseDas(self)

    def getDatasetSizes(self) -> None:
        """Gets row count for dataset from ERDDAP ncHeader response, sets to attribute"""
        if not self.DASresponse:
            return None
            
        base_url = f"{self.server}{self.dataset_id}.ncHeader?"
        print(f"Requesting headers for {self.dataset_id}")
        
        try:
            response = requests.get(base_url)
            response.raise_for_status()
            
            match = re.search(r'dimensions:\s*(.*?)\s*variables:', response.text, re.DOTALL)
            if not match:
                return None
                
            for line in match.group(1).split('\n'):
                line = line.strip()
                if line.startswith('row'):
                    if row_match := re.match(r'row\s*=\s*(\d+);', line):
                        self.rowCount = int(row_match.group(1))
                elif line.startswith('obs'):
                    if obs_match := re.match(r'obs\s*=\s*(\d+);', line):
                        self.rowCount = int(obs_match.group(1))
                        
        except requests.RequestException as e:
            print(f"Error fetching dataset size: {e}")
            
        return None
    
    @property
    def needsSubsetting(self) -> bool:
        """Check if dataset needs to be split into chunks"""
        if self.rowCount > 45000:
            self.needs_Subset = True
        else:
            self.needs_Subset = False

    def generateUrl(self, dataformat="csvp") -> str:
        additionalAttr = self.attribute_list

        # the attribute list
        attrs = []

        if additionalAttr and 'depth' in additionalAttr:
            additionalAttr.remove('depth')
            attrs.append('depth')

        attrs.extend(["longitude", "self.latitude"])

        if additionalAttr:
            attrs.extend(additionalAttr)

        # Finally, add 'time'
        attrs.append(self.time)

        # Join the attributes into the URL
        attrs_encoded = '%2C'.join(attrs)

        # Construct time constraints
  
        time_constraints = (
            f"&time%3E%3D{self.start_time}Z"
            f"&time%3C%3D{self.end_time}Z"
        )

        # Construct the full URL
        url = (
            f"{self.server}{self.datasetid}.{dataformat}?"
            f"{attrs_encoded}"
            f"{time_constraints}"
        )
        
        print(f"\nGenerated URL: {url}")

        return url

    # Below are example functions that can be used to manipulate the dataset object
    def add_time_subset(self, subset_name: str, start: str, end: str) -> None:
        """Add time subset for chunked processing"""
        if not self.subsets:
            self.subsets = {}
        self.subsets[subset_name] = {'start': start, 'end': end}
    
    
    def calculateTimeSubset(self, row_count: int) -> dict:
        """Calculate time subsets based on row count.
        Returns Subset_N: {'start': time, 'end': time}
        """
        try:
            # Use start_time and end_time directly if they are datetime objects
            start = self.start_time
            end = self.end_time

            # If start_time or end_time are strings, parse them into datetime objects
            if isinstance(start, str):
                start = datetime.fromisoformat(start)
            if isinstance(end, str):
                end = datetime.fromisoformat(end)

            # Calculate total days and required chunks
            total_days = (end - start).days
            chunks_needed = max(1, math.ceil(row_count / 45000))

            days_per_chunk = total_days / chunks_needed

            time_chunks = {}
            chunk_start = start
            chunk_num = 1

            while chunk_start < end:
                chunk_end = min(chunk_start + timedelta(days=days_per_chunk), end)
                time_chunks[f'Subset_{chunk_num}'] = {
                    'start': chunk_start.strftime('%Y-%m-%dT%H:%M:%S'),
                    'end': chunk_end.strftime('%Y-%m-%dT%H:%M:%S'),
                }
                chunk_start = chunk_end
                chunk_num += 1

            return time_chunks

        except Exception as e:
            print(f"Error calculating time subset: {e}")
            return None


################## NRT Functions ##################

#This function returns the start and end time of the moving window
def movingWindow(isStr: bool):
    if isStr == True:
        start_time = datetime.now() - timedelta(days=7)
        end_time = datetime.now()
        return start_time.isoformat(), end_time.isoformat()
    else:
        start_time = datetime.now() - timedelta(days=7)
        end_time = datetime.now()
        return start_time, end_time

#This function checks if the dataset has data within the last 7 days
def checkDataRange(datasetid) -> bool:
    startDas, endDas = dc.convertFromUnixDT(dc.getTimeFromJson(datasetid))
    window_start, window_end = movingWindow(isStr=False)
    if startDas <= window_end and endDas >= window_start:
        return True
    else:
        return False  

#This function returns all datasetIDs that have data within the last 7 days
#Maybe request a fresh json everytime?
def batchNRTFind(ERDDAPObj: ec.ERDDAPHandler) -> list:
    ValidDatasetIDs = []
    DIDList = ec.ERDDAPHandler.getDatasetIDList(ERDDAPObj)
    for datasetid in DIDList:
        if dc.checkForJson(datasetid) == False:
            das_resp = ec.ERDDAPHandler.getDas(ERDDAPObj, datasetid=datasetid)
            parsed_response = dc.parseDasResponse(das_resp)
            parsed_response = dc.convertToDict(parsed_response)
            dc.saveToJson(parsed_response, datasetid)
        

            if checkDataRange(datasetid) == True:
                ValidDatasetIDs.append(datasetid)
        else:
            if checkDataRange(datasetid) == True:
                ValidDatasetIDs.append(datasetid)
    
    print(f"Found {len(ValidDatasetIDs)} datasets with data within the last 7 days.")
    return ValidDatasetIDs

def NRTFindAGOL() -> list:
    nrt_dict  = ul.updateCallFromNRT(1)
    return nrt_dict




