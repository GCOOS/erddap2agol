from . import erddap_wrangler as ec
from . import das_client as dc
from src.utils import OverwriteFS
from arcgis.gis import GIS
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, List
from io import StringIO
import datetime, requests, re, math, os, json, pandas as pd
from datetime import timedelta, datetime

#---------------------DatasetWrangler---------------------

@dataclass
class DatasetWrangler:
    dataset_id: str
    dataset_title: dict
    server: str
    no_time_range: Optional[bool] = None
    row_count: Optional[int] = None
    chunk_size: Optional[int] = 49000
    attribute_list: Optional[List[str]] = field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    needs_Subset: Optional[bool] = None
    DAS_response: Optional[bool] = None
    is_glider: bool = False
    subsetDict: Optional[Dict] = field(default_factory=dict)
    is_processed: bool = False
    is_nrt: bool = None
    moving_window_days: int = 7
    nc_global: Dict = field(default_factory=dict)
    DAS_filepath: Optional[os.PathLike] = None
    data_filepath:Optional[os.PathLike | list[os.PathLike]] = None
    url_s:Optional[str | list[str]] = None
    has_error: Optional[bool] = False
    ignore_error: Optional[bool] = False
    has_time: Optional[bool] = True
    time_str: Optional[str] = None
    
    
    def __post_init__(self):
        """Building the dataset objects"""
        if self.is_glider == True:
            self.getDas()
            # improved glider optimizations will go here
            pass

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
    
    def skipFromError(func):
        """General skip error decorator that will be applied to all dataset methods"""
        def wrapper(self, *args, **kwargs):
            if self.has_error == True:
                print(f"\nSkipping {func.__name__} - due to processing error {self.dataset_id}")
                return None
            return func(self, *args, **kwargs)
        return wrapper
    
    def skipFromNoTime(func):
        """General skip error decorator that will be applied to all dataset methods"""
        def wrapper(self, *args, **kwargs):
            if self.has_time == False:
                print(f"Skipping {func.__name__} - time not present for {self.dataset_id}")
                return None
            return func(self, *args, **kwargs)
        return wrapper
    
    def skipFromNoRange(func):
        """General skip error decorator that will be applied to all dataset methods"""
        def wrapper(self, *args, **kwargs):
            if self.no_time_range == True:
                print(f"Skipping {func.__name__} - no time range for {self.dataset_id}")
                return None
            return func(self, *args, **kwargs)
        return wrapper

    def getDas(self) -> None:
        """Fetch and parse DAS metadata.
            Sets major attributes for the dataset"""
        url = f"{self.server}{self.dataset_id}.das"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            self.DAS_response = True
            
            # reduce this to one step
            DAS_Dict = dc.convertToDict(dc.parseDasResponse(response.text))
            self.DAS_filepath = dc.saveToJson(DAS_Dict, self.dataset_id)
            
            # Extract NC_GLOBAL
            if "NC_GLOBAL" in DAS_Dict:
                self.nc_global = DAS_Dict["NC_GLOBAL"]
            
            # Get attributes and time range
            self.attribute_list = dc.getActualAttributes(self)

            if self.has_time:
                time_range = dc.getTimeFromJson(self)
                if time_range:
                    self.start_time, self.end_time = time_range
                    if self.start_time.tzinfo is None:
                        self.start_time = self.start_time.replace(tzinfo=timezone.utc)
                    if self.end_time.tzinfo is None:
                        self.end_time = self.end_time.replace(tzinfo=timezone.utc)
                    
                    now_utc = datetime.now(timezone.utc)

        except requests.RequestException as e:
            print(f"\nError fetching DAS for {self.dataset_id}: {e}")
            self.has_error = True
            self.DAS_response = False
        except Exception as e:
            print(f"\nError parsing DAS for {self.dataset_id}: {e}")
            self.DAS_response = False

    @skipFromNoTime
    @skipFromNoRange
    def getDatasetSizes(self, timeOut_time: int = 120) -> None:
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
            self.has_error = True

        except Exception as e:
            print(f"Error fetching dataset size: {e}")
            
        return None
    
    @skipFromNoTime
    @skipFromNoRange
    def needsSubsetting(self) -> bool:
        """Check if dataset needs to be split into chunks"""
        if self.row_count is not None:
            #bypass for glider datasets
            if self.row_count > self.chunk_size and self.is_glider != True:
                print(f"\nUh oh! {self.dataset_title} is too big ({self.row_count} records) and needs to be chunked!")
                if self.row_count > 500000:
                    # if the row count is greater than 500,000 change chunk size from 49999 to 100000
                    self.chunk_size = 100000
                    self.needs_Subset = True
                else:
                    self.needs_Subset = True
            else:
                self.needs_Subset = False
    
    
    @skipFromNoTime
    @skipFromError
    @skipFromNoRange
    def calculateTimeSubset(self) -> dict:
        """Calculate time subsets based on row count.
            Method applies if self.needs_Subset is True """
        chunk_size = self.chunk_size
        if not self.needs_Subset:
            return None
        
        try:
    
            start = self.start_time
            end = self.end_time

            # Calculate exact chunks needed
            total_records = self.row_count
            records_per_chunk = chunk_size
            chunks_needed = math.ceil(total_records / records_per_chunk)
            
            # Calculate time per chunk
            total_seconds = (end - start).total_seconds()
            seconds_per_record = total_seconds / total_records
            seconds_per_chunk = seconds_per_record * records_per_chunk

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
            self.has_error = True
            return None
            
    def add_time_subset(self, subset_name: str, start: str, end: str) -> None:
        """Add time subset for chunked processing"""
        if not self.subsets:
            self.subsets = {}
        self.subsets[subset_name] = {'start': start, 'end': end}
    
    # this function is a nightmare to read
    @skipFromError
    def generateUrl(self, dataformat: str="csvp", nrt_update:bool = False) -> list[str]:
        """Builds request URLs for data, special approach for subsetting data"""
        urls = []
        additionalAttr = self.attribute_list
        
        # Prepare attributes
        attrs = []
        # add z here
        if additionalAttr and 'depth' in additionalAttr:
            additionalAttr.remove('depth')
            attrs.append('depth')
        if additionalAttr:
            attrs.extend(additionalAttr)

        # commenting this code out as we handle time differently now
        if self.time_str in attrs:
            attrs.remove(self.time_str)
        #     print(f"\n debug dw.248: time removed")
        
        # print(f"{self.time_str}")
        # print(f"{attrs}")
        attrs_encoded = '%2C'.join(attrs)
        if self.no_time_range:
            url = (
                    f"{self.server}{self.dataset_id}.{dataformat}?"
                    # hard coded time here- not anymore
                    f"{attrs_encoded}")
            urls.append(url)
        
        #----------- Single file URL build -----------
        if not self.needs_Subset and not self.no_time_range:
            # Single URL for datasets not requiring subsetting
            if self.has_time:
                # nrt time set is returning a string instead of dt obj 
                # time constraints
                if nrt_update or self.is_nrt:
                    start = self.start_time
                    end = self.end_time
                    time_constraints = (
                        f"&{self.time_str}%3E%3D{start}Z"
                        f"&{self.time_str}%3C%3D{end}Z"
                    )
                # other time constraints
                else:
                    start = self.start_time.strftime('%Y-%m-%dT%H:%M:%S')
                    end = self.end_time.strftime('%Y-%m-%dT%H:%M:%S')
                    time_constraints = (
                        f"&{self.time_str}%3E%3D{start}Z"
                        f"&{self.time_str}%3C%3D{end}Z"
                    )
                url = (
                    f"{self.server}{self.dataset_id}.{dataformat}?"
                    # hard coded time here- not anymore
                    f"{self.time_str}%2C{attrs_encoded}"
                    f"{time_constraints}"
                )
                urls.append(url)
            #----------- Multiple file URL build -----------
            else:
                url = (
                    f"{self.server}{self.dataset_id}.{dataformat}?"
                    # remove time param and time_constraints
                    f"{attrs_encoded}"
                )
                urls.append(url)
        else:
            # Multiple URLs for subsetted datasets
            for i, (subset_name, times) in enumerate(self.subsetDict.items()):
                # not the final chunch, < for upper bound
                if i < (len(self.subsetDict) -1):
                    time_constraints = (
                        f"&{self.time_str}%3E%3D{times['start']}Z"
                        f"&{self.time_str}%3C{times['end']}Z"
                    # f"&time%3C%3D{times['end']}Z"
                    )

                elif i == (len(self.subsetDict) -1):
                    # the final chunk, <=
                    time_constraints = (
                        f"&{self.time_str}%3E%3D{times['start']}Z"
                        f"&{self.time_str}%3C%3D{times['end']}Z"
                    )
                # else:
                #     time_constraints = (f"&{self.time_str}=NaN")

                url = (
                    f"{self.server}{self.dataset_id}.{dataformat}?"
                    f"{self.time_str}%2C{attrs_encoded}"
                    f"{time_constraints}"
                )
                urls.append(url)
                # We need a better way to identify if there are NaNs in the dataset
                if self.no_time_range:
                    time_constraints = (f"&{self.time_str}=NaN") 
                    url = (
                            f"{self.server}{self.dataset_id}.{dataformat}?"
                            f"{self.time_str}%2C{attrs_encoded}"
                            f"{time_constraints}"
                        )
                    urls.append(url)
            
        
        self.url_s = urls
        return urls
    
    # Come back and review this function. This might need some unit tests for bad responses.
    @skipFromError
    def writeErddapData(self, connection_attempts: int= 3, timeout_time: int= 120) -> str | list[str]:
        """
        Write ERDDAP data to CSV files.
        
        :param connection_attempts: How many times to try fetching a URL before giving up.
        :param timeout_time: Seconds before requests time out.
        """
        filepaths = []
        
        def process_url(url: str, subset_num: int = None) -> str | None:
            """
            Attempt to download data from a single URL and write to CSV.
            Returns the file path on success, or None on failure.
            """
            try:
                # Make the request with specified timeout
                response = requests.get(url, timeout=timeout_time)
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

            except requests.exceptions.Timeout as e:
                print(f"\nTimeout for URL: {url} | Error: {e}")
                return None
            except requests.exceptions.RequestException as e:
                print(f"\nRequest Exception | Error: {e}")
                # This catches all other request errors (e.g., 4xx, 5xx).
                return None
            except Exception as e:
                # Catch any other unforeseen errors
                print(f"\nError processing URL | Exception: {e}")
                return None

# ----------------------------------------------------------------------------

        # Individual file download (no subsets)
        if not self.needs_Subset:
            #print(f"\nDownloading data for {self.dataset_id}")

            # Track how many attempts we’ve made on this single URL
            url = self.url_s[0]
            attempts = 0
            filepath = None

            while attempts < connection_attempts and not filepath:
                attempts += 1
                print(f"\nDownloading data from {url} (Attempt: {attempts}/{connection_attempts})")
                filepath = process_url(url)
                if not filepath:
                    # Sleep or just continue; your choice
                    pass
            
            if filepath:
                self.data_filepath = filepath
                return filepath
            else:
                # If we failed after all attempts, set the error flag
                self.has_error = True
                return None

# ----------------------------------------------------------------------------

        # Subset (chunked) file download
        else:
            print(f"\nDownloading data for {self.dataset_id}")

            # Use a queue to hold all subset URLs
            urls_queue = deque(self.url_s)
            # Dictionary to track attempts per URL
            attempts_dict = {u: 0 for u in self.url_s}

            # We also want to preserve original index for naming: i.e., subset_i
            # One way: store (url, index) in the queue
            urls_queue = deque([(url, i+1) for i, url in enumerate(self.url_s)])
            
            while urls_queue:
                url, subset_index = urls_queue.popleft()
                attempts_dict[url] += 1
                attempt_num = attempts_dict[url]
                
                print(
                    f"\nDownloading subset {subset_index}/{len(self.url_s)} "
                    # f"({self.url_s[subset_index-1]})"
                    f"(Attempt: {attempt_num}/{connection_attempts}) "
                )
                
                filepath = process_url(url, subset_num=subset_index)

                if filepath:
                    # Success: add to final filepaths
                    filepaths.append(filepath)
                 # Failure or timeout
                else:
                    is_nan_url = (url == self.url_s[-1])
                    if is_nan_url:
                        print(f"\n No NaN time values returned, continuing")
                    if attempt_num < connection_attempts:
                        # Skip now, but come back later by pushing to end of queue
                        urls_queue.append((url, subset_index))
                    
                    # in this condition, we assume the dataset will continue to fail
                    # the first element failed for a second time, and we assume all failed because the last element 
                    # At this point in the loop the iterator should be 2 if the first element failed above  
                    # if attempts_dict[url][0] == 2 and attempts_dict[url][-1] == 1:
                    if attempts_dict[self.url_s[0]] == 2 and attempts_dict[self.url_s[-1]] == 1:
                        self.has_error = True
                        print(f"\nIt appears all subset download attempts failed, we will not try again: has_error = True")
                    
                    elif attempt_num > connection_attempts:
                        print(f"\nMax retries exceeded for subset num {subset_index} URL: {url}")
                        pass

                    else:
                        if filepaths:
                            print(f"\nSome downloads failed for {self.dataset_id}")
                        else:
                            print(f"\nError in download queue, no filepaths detected")
                            self.has_error = True
                            

                        # Exceeded attempts
                        
                        # At this point, we can either:
                        # 1) Decide to continue processing other URLs
                        # 2) Break entirely
                        # For now, we let other chunks attempt to download:
                        # so we do not re-append it to the queue.
                        pass

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
        now_utc = datetime.now(timezone.utc)
        seven_days_ago = now_utc - timedelta(days=self.moving_window_days)

        self.start_time = seven_days_ago.strftime('%Y-%m-%dT%H:%M:%S')
        self.end_time = now_utc.strftime('%Y-%m-%dT%H:%M:%S')
            


# ---------------------------------
#This function checks if the dataset has data within the last 7 days
    # def checkDataRange(datasetid) -> bool:
    #     def movingWindow(self):        
    #         self.start_time = datetime.now() - timedelta(days=self.moving_window_days)
    #         self.end_time = datetime.now()
    #     startDas, endDas = dc.convertFromUnixDT(dc.getTimeFromJson(datasetid))
    #     window_start, window_end = movingWindow(isStr=False)
    #     if startDas <= window_end and endDas >= window_start:
    #         return True
    #     else:
    #         return False  

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


    # We can enhance this function. If a url returns a bad response, we should come back to it.
    # We also might want to dump some sort of log to help the user pick up where they left off.
    # @skipFromError
    # def writeErddapData(self) -> str | list[str]:
    #     """Write ERDDAP data to CSV files"""
    #     filepaths = []
        
    #     def process_url(url: str, subset_num: int = None) -> str:
    #         try:
    #             if not self.has_error:
    #                 response = requests.get(url)
    #                 response.raise_for_status()
                    
    #                 csvData = StringIO(response.text)
    #                 df = pd.read_csv(csvData, header=None, low_memory=False)
                    
    #                 temp_dir = ec.getTempDir()
    #                 if subset_num is not None:
    #                     filename = f"{self.dataset_id}_subset_{subset_num}.csv"
    #                 else:
    #                     filename = f"{self.dataset_id}.csv"
                        
    #                 file_path = os.path.join(temp_dir, filename)
    #                 df.to_csv(file_path, index=False, header=False)
    #                 return file_path
                
    #         except Exception as e:
    #             print(f"Error processing URL {url}: {e}")
    #             self.has_error = True
    #             return None
        
    #     # ----------------------------------------------------
    #     # Individual file download
    #     if not self.needs_Subset:
    #         print(f"\nDownloading data for {self.dataset_id}")
    #         filepath = process_url(self.url_s[0])
    #         if filepath:
    #             self.data_filepath = filepath
    #             return filepath
    #     # Subset file download
    #     else:
    #         print(f"\nDownloading data for {self.dataset_id}")
    #         for i, url in enumerate(self.url_s, 1):
    #             print(f"Downloading subset {i}/{len(self.url_s)}\t({self.dataset_id})")
    #             filepath = process_url(url, i)
    #             if filepath:
    #                 filepaths.append(filepath)
            
    #         if filepaths:
    #             self.data_filepath = filepaths
    #             return filepaths
                
    #     return None



