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
    dataset_id: str
    server: str
    row_count: Optional[int] = None
    attributes: List[str] = None
    time_params: Dict[str, datetime] = None
    das_metadata: Dict = None

    def __post_init__(self):
        self.subsets = {}
        self.is_processed = False
    
    def get_das(self) -> Dict:
        """Fetch and parse DAS metadata"""
        url = f"{self.server}{self.dataset_id}.das"
        response = requests.get(url)
        if response.status_code != 200:
            return None
        return dc.parseDasResponse(response.text)



    # Below are example functions that can be used to manipulate the dataset object
    def add_time_subset(self, subset_name: str, start: str, end: str) -> None:
        """Add time subset for chunked processing"""
        if not self.subsets:
            self.subsets = {}
        self.subsets[subset_name] = {'start': start, 'end': end}

    @property
    def needs_chunking(self) -> bool:
        """Check if dataset needs to be split into chunks"""
        return self.row_count > 45000 if self.row_count else False
    
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

def getDatasetSizes(datasetList: list, erddapObj: ec.ERDDAPHandler) -> dict:
    """Gets row counts for multiple datasets and spits out into a dictionary datasetid, rowNumber"""
    
    def _parse_header(content: str) -> int:
        """Parse ncHeader content to find row count using fancy regex"""
        match = re.search(r'dimensions:\s*(.*?)\s*variables:', content, re.DOTALL)
        if not match:
            return None
            
        dimensions_section = match.group(1)
        for line in dimensions_section.split('\n'):
            line = line.strip()
            if line.startswith('row'):
                row_match = re.match(r'row\s*=\s*(\d+);', line)
                if row_match:
                    return int(row_match.group(1))
            elif line.startswith('obs'):
                obs_match = re.match(r'obs\s*=\s*(\d+);', line)
                if obs_match:
                    return int(obs_match.group(1))
        return None
    
    def _get_row_count(dataset: str) -> int:
        """Get row count for single dataset"""
        base_url = f"{erddapObj.server}{dataset}"
        ncheader_url = f"{base_url}.ncHeader?"
        
        print(f"Requesting headers for {dataset}")
        response = requests.get(ncheader_url)
        if response.status_code != 200:
            return None
            
        return _parse_header(response.text)
    
    return {dataset: _get_row_count(dataset) for dataset in datasetList}


