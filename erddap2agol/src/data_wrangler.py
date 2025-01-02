from . import erddap_client as ec
from . import das_client as dc
from src.utils import OverwriteFS
from arcgis.gis import GIS

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, List
from io import StringIO
import datetime, requests, re, math, os, pandas as pd
from datetime import timedelta, datetime

################## Experimenting with new class ##################

@dataclass
class DatasetWrangler:
    dataset_id: str
    server: str
    row_count: Optional[int] = None
    attribute_list: Optional[List[str]] = field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    needs_Subset: Optional[bool] = None
    DAS_response: Optional[bool] = None
    is_glider: bool = False
    subsetDict: Optional[Dict] = field(default_factory=dict)
    is_processed: bool = False
    is_nrt: bool = None
    nc_global: Dict = field(default_factory=dict)
    DAS_filepath: Optional[os.PathLike] = None
    data_filepath:Optional[os.PathLike | list[os.PathLike]] = None
    url_s:Optional[str | list[str]] = None
    has_error: Optional[bool] = False
    
    
    def __post_init__(self):
        """Building the dataset objects"""
        if self.server == "https://gliders.ioos.us/erddap/tabledap/":
            self.is_glider = True

        # For NRT we bypass the dataset size step
        if self.is_nrt == True:
            self.needs_Subset = False
            self.getDas()
            self.nrtTimeSet()
        
        else:
            self.getDas()
            self.getDatasetSizes()
            self.needsSubsetting()
            if self.needs_Subset == True:
                self.subsetDict = self.calculateTimeSubset()

    def requireTime(func):
        """Require time decorator"""
        def wrapper(self, *args, **kwargs):
            if not self.start_time or self.end_time:
                print(f"Skipping {func.__name__} - No time for {self.dataset_id}")
                self.has_error = True
                return None
            return func(self, *args, **kwargs)
        return wrapper
    
    def skipFromError(func):
        """General skip error decorator that will be applied to all dataset methods"""
        def wrapper(self, *args, **kwargs):
            if self.has_error == True:
                print(f"Skipping {func.__name__} - due to processing error {self.dataset_id}")
                return None
            return func(self, *args, **kwargs)
        return wrapper

    def getDas(self) -> None:
        """Fetch and parse DAS metadata"""
        url = f"{self.server}{self.dataset_id}.das"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            self.DAS_response = True
            
            # Parse DAS response
            DAS_Dict = dc.convertToDict(dc.parseDasResponse(response.text))
            self.DAS_filepath = dc.saveToJson(DAS_Dict, self.dataset_id)
            
            # Extract NC_GLOBAL
            if "NC_GLOBAL" in DAS_Dict:
                self.nc_global = DAS_Dict["NC_GLOBAL"]
            
            # Get attributes and time range
            self.attribute_list = dc.getActualAttributes(self.dataset_id)

            try:
                time_range = dc.getTimeFromJson(self.dataset_id)
                if time_range:
                    self.start_time, self.end_time = time_range
            except Exception as e:
                self.has_error = True
                pass

        except requests.RequestException as e:
            print(f"\nError fetching DAS for {self.dataset_id}: {e}")
            self.DAS_response = False
        except Exception as e:
            print(f"\nError parsing DAS for {self.dataset_id}: {e}")
            self.DAS_response = False


    def getDatasetSizes(self, timeOut_time = 120) -> None:
        """Gets row count for dataset from ERDDAP ncHeader response, sets to attribute"""
        if not self.DAS_response:
            return None
        
        # Bypass for glider datasets
        if self.is_glider is True:
            return None
        
        if self.has_error is True:
            return None
            
        base_url = f"{self.server}{self.dataset_id}.ncHeader?"
        print(f"Requesting headers @ {base_url}")
        
        try:
            response = requests.get(base_url, timeout= timeOut_time)
            
            
            match = re.search(r'dimensions:\s*(.*?)\s*variables:', response.text, re.DOTALL)
            if not match:
                return None
                
            for line in match.group(1).split('\n'):
                line = line.strip()
                if line.startswith('row'):
                    if row_match := re.match(r'row\s*=\s*(\d+);', line):
                        self.row_count = int(row_match.group(1))
                elif line.startswith('obs'):
                    if obs_match := re.match(r'obs\s*=\s*(\d+);', line):
                        self.row_count = int(obs_match.group(1))
                        
        except requests.exceptions.Timeout:
            print(f"Request timed out after 90 seconds for {self.dataset_id}, skipping")

        except Exception as e:
            print(f"Error fetching dataset size: {e}")
            
        return None
    
    def needsSubsetting(self, record_limit = 50000) -> bool:
        """Check if dataset needs to be split into chunks"""
        if self.row_count is not None:
            #bypass for glider datasets
            if self.row_count > record_limit and self.is_glider != True:
                self.needs_Subset = True
                print(f"\nUh oh! {self.dataset_id} is too big ({self.row_count} records) and needs additional processing!")
            else:
                self.needs_Subset = False

    def calculateTimeSubset(self, record_limit = 50000) -> dict:
        """Calculate time subsets based on row count.
        Returns Subset_N: {'start': time, 'end': time}
        """
        if not self.needs_Subset:
            return None
        
        try:
            # Ensure datetime objects
            start = self.start_time if isinstance(self.start_time, datetime) else datetime.fromisoformat(self.start_time)
            end = self.end_time if isinstance(self.end_time, datetime) else datetime.fromisoformat(self.end_time)

            # Calculate exact chunks needed
            total_records = self.row_count
            records_per_chunk = record_limit
            chunks_needed = math.ceil(total_records / records_per_chunk)
            
            # Calculate time per chunk
            total_seconds = (end - start).total_seconds()
            seconds_per_record = total_seconds / total_records
            seconds_per_chunk = seconds_per_record * records_per_chunk

            # Create equal-sized chunks
            time_chunks = {}
            chunk_start = start
            
            for i in range(chunks_needed):
                chunk_end = chunk_start + timedelta(seconds=seconds_per_chunk)
                if i == chunks_needed - 1:  # Last chunk
                    chunk_end = end
                    
                time_chunks[f'Subset_{i+1}'] = {
                    'start': chunk_start.strftime('%Y-%m-%dT%H:%M:%S'),
                    'end': chunk_end.strftime('%Y-%m-%dT%H:%M:%S')
                }
                chunk_start = chunk_end
                
            print(f"{self.dataset_id}: {len(time_chunks)} subsets required")
            return time_chunks

        except Exception as e:
            print(f"Error calculating time subset: {e}")
            return None
            
    def add_time_subset(self, subset_name: str, start: str, end: str) -> None:
        """Add time subset for chunked processing"""
        if not self.subsets:
            self.subsets = {}
        self.subsets[subset_name] = {'start': start, 'end': end}

    def generateUrl(self, dataformat="csvp") -> list[str]:
        """Builds request URLs for data, special approach for subsetting data"""
        urls = []
        additionalAttr = self.attribute_list
        
        # Prepare attributes
        attrs = []
        if additionalAttr and 'depth' in additionalAttr:
            additionalAttr.remove('depth')
            attrs.append('depth')
        attrs.extend(["longitude", "latitude"])
        if additionalAttr:
            attrs.extend(additionalAttr)
        attrs_encoded = '%2C'.join(attrs)

        if not self.needs_Subset:
            # Single URL for datasets not requiring subsetting
            time_constraints = (
                f"&time%3E%3D{self.start_time}Z"
                f"&time%3C%3D{self.end_time}Z"
            )
            url = (
                f"{self.server}{self.dataset_id}.{dataformat}?"
                f"time%2C{attrs_encoded}"
                f"{time_constraints}"
            )
            urls.append(url)
        else:
            # Multiple URLs for subsetted datasets
            for subset_name, times in self.subsetDict.items():
                time_constraints = (
                    f"&time%3E%3D{times['start']}Z"
                    f"&time%3C%3D{times['end']}Z"
                )
                url = (
                    f"{self.server}{self.dataset_id}.{dataformat}?"
                    f"{attrs_encoded}"
                    f"{time_constraints}"
                )
                urls.append(url)
        
        self.url_s = urls
        return urls
    

    def writeErddapData(self) -> str | list[str]:
        """Write ERDDAP data to CSV files"""
        filepaths = []
        
        def process_url(url: str, subset_num: int = None) -> str:
            try:
                response = requests.get(url)
                response.raise_for_status()
                
                csvData = StringIO(response.text)
                df = pd.read_csv(csvData, header=None, low_memory=False)
                
                temp_dir = ec.getTempDir()
                if subset_num is not None:
                    filename = f"{self.dataset_id}_subset_{subset_num}.csv"
                else:
                    filename = f"{self.dataset_id}.csv"
                    
                file_path = os.path.join(temp_dir, filename)
                df.to_csv(file_path, index=False, header=False)
                return file_path
                
            except Exception as e:
                print(f"Error processing URL {url}: {e}")
                return None
        
        if not self.needs_Subset:
            print(f"\nDownloading data for {self.dataset_id}")
            filepath = process_url(self.url_s[0])
            if filepath:
                self.data_filepath = filepath
                return filepath
        else:
            print(f"\nDownloading data for {self.dataset_id}")
            for i, url in enumerate(self.url_s, 1):
                print(f"Downloading subset {i}/{len(self.url_s)}")
                filepath = process_url(url, i)
                if filepath:
                    filepaths.append(filepath)
            
            if filepaths:
                self.data_filepath = filepaths
                return filepaths
                
        return None

            
    def calculateTimeRange(self, intervalType=None) -> int:
        start = datetime.fromisoformat(self.start_time)
        end = datetime.fromisoformat(self.end_time)
        
        if intervalType is None:
            return (end - start).days
        
        elif intervalType == "months":
            year_diff = end.year - start.year
            month_diff = end.month - start.month
            
            total_months = year_diff * 12 + month_diff
            return total_months

        else:
            raise ValueError("Invalid interval type.")


################## NRT Functions ##################

#This function returns the start and end time of the moving window
    def nrtTimeSet(self):
        """Sets start_time/end_time in ISO format (e.g., 2023-09-29T14:05:12)"""
        now = datetime.now()
        seven_days_ago = now - timedelta(days=7)

        self.start_time = seven_days_ago.strftime('%Y-%m-%dT%H:%M:%S')
        self.end_time = now.strftime('%Y-%m-%dT%H:%M:%S')
            
#This function checks if the dataset has data within the last 7 days
    def checkDataRange(datasetid) -> bool:
        def movingWindow(self):        
            self.start_time = datetime.now() - timedelta(days=7)
            self.end_time = datetime.now()
        startDas, endDas = dc.convertFromUnixDT(dc.getTimeFromJson(datasetid))
        window_start, window_end = movingWindow(isStr=False)
        if startDas <= window_end and endDas >= window_start:
            return True
        else:
            return False  

#This function returns all datasetIDs that have data within the last 7 days
#Maybe request a fresh json everytime?
# def batchNRTFind(ERDDAPObj: ec.ERDDAPHandler) -> list:
#     ValidDatasetIDs = []
#     DIDList = ec.ERDDAPHandler.getDatasetIDList(ERDDAPObj)
#     for datasetid in DIDList:
#         if dc.checkForJson(datasetid) == False:
#             das_resp = ec.ERDDAPHandler.getDas(ERDDAPObj, datasetid=datasetid)
#             parsed_response = dc.parseDasResponse(das_resp)
#             parsed_response = dc.convertToDict(parsed_response)
#             dc.saveToJson(parsed_response, datasetid)
        

#             if checkDataRange(datasetid) == True:
#                 ValidDatasetIDs.append(datasetid)
#         else:
#             if checkDataRange(datasetid) == True:
#                 ValidDatasetIDs.append(datasetid)
    
#     print(f"Found {len(ValidDatasetIDs)} datasets with data within the last 7 days.")
#     return ValidDatasetIDs

# def NRTFindAGOL() -> list:
#     nrt_dict  = ul.updateCallFromNRT(1)
#     return nrt_dict




