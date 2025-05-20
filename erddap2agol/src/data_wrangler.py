from . import erddap_wrangler as ec
from . import das_client as dc
from . import core
from src.utils import OverwriteFS
from arcgis.gis import GIS
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, List, Union
from io import StringIO
import datetime, requests, re, math, os, json, pandas as pd
from datetime import timedelta, datetime
from urllib.parse import quote

#---------------------DatasetWrangler---------------------

@dataclass
class DatasetWrangler:
    dataset_id: str
    dataset_title: dict
    server: str
    griddap: bool = False
    no_time_range: Optional[bool] = None
    row_count: Optional[int] = None
    chunk_size: Optional[int] = None
    attribute_list: Optional[List[str]] = field(default_factory=list)
    
    # date time attributes
    # added user and req (request) start time to reconcile any differences between 
    # what the user requested and what is available within the data
    # This applies to griddap only, tabledap simply uses data start time
    griddap_args: dict = None
    mult_dim: bool = False
    data_start_time: Optional[datetime] = None
    data_end_time: Optional[datetime] = None
    user_start_time: Optional[datetime] = None
    user_end_time: Optional[datetime] = None
    req_start_time: Optional[datetime] = None
    req_end_time: Optional[datetime] = None
    
    needs_Subset: Optional[bool] = None
    DAS_response: Optional[bool] = None
    is_glider: bool = False
    subsetDict: Optional[Dict] = field(default_factory=dict)
    # is_processed: bool = False
    is_nrt: bool = None
    moving_window_days: int = 7
    nc_global: Dict = field(default_factory=dict)
    DAS_filepath: Optional[os.PathLike] = None
    data_filepath: Optional[Union[os.PathLike, List[os.PathLike]]] = None
    url_s: Optional[Union[str, List[str]]] = None
    nan_url: Optional[str] = None
    has_error: Optional[bool] = False
    ignore_error: Optional[bool] = False
    has_time: Optional[bool] = True
    time_str: Optional[str] = None
    
    
    def __post_init__(self):
        if not self.griddap:
            # always figure out your chunk_size
            self.chunk_size = int(core.user_options.chunk_size) \
                if core.user_options.chunk_size else 100_000

            # now a single exclusive branch:
            if self.is_glider:
                self.getDas()
                return

            elif self.is_nrt:
                self.needs_Subset = False
                self.getDas()
                self.nrtTimeSet()
                return

            elif core.user_options.bypass_chunking_bool:
                # bypass chunking entirely
                self.needs_Subset = False
                self.getDas()
                return

            else:
                # the real “default” chunking path
                self.getDas()
                self.getDatasetSizes()
                self.needsSubsetting()
                if self.needs_Subset:
                    self.subsetDict = self.calculateTimeSubset()
                return
        else:
            print("griddap init")
            self.getDas()

    
    def skipFromError(func):
        """General skip error decorator that will be applied to all dataset methods"""
        def wrapper(self, *args, **kwargs):
            if self.has_error:
                print(f"\nSkipping {func.__name__} - due to processing error {self.dataset_id}")
                return None
            return func(self, *args, **kwargs)
        return wrapper
    
    def skipFromNoTime(func):
        """General skip error decorator that will be applied to all dataset methods"""
        def wrapper(self, *args, **kwargs):
            if not self.has_time:
                print(f"Skipping {func.__name__} - time not present for {self.dataset_id}")
                return None
            return func(self, *args, **kwargs)
        return wrapper
    
    def skipFromNoRange(func):
        """General skip error decorator that will be applied to all dataset methods"""
        def wrapper(self, *args, **kwargs):
            if self.no_time_range:
                print(f"Skipping {func.__name__} - no time range for {self.dataset_id}")
                return None
            return func(self, *args, **kwargs)
        return wrapper

    def getDas(self) -> None:
        """Fetch and parse DAS metadata.
        Sets major attributes for the dataset"""
        default_url = f"{self.server}{self.dataset_id}.das"
        if self.griddap:
            if "tabledap" in self.server:
                new_url = default_url.replace("tabledap", "griddap")
                url = new_url
        else:
            url = default_url
        try:
            # agnostic of protocol
            print(url)
            response = requests.get(url)
            response.raise_for_status()
            self.DAS_response = True
            DAS_Dict = dc.convertToDict(dc.parseDasResponse(response.text))
            self.DAS_filepath = dc.saveToJson(DAS_Dict, self.dataset_id)
            
            #check for NC_Global and add to the nc_global attribute
            if "NC_GLOBAL" in DAS_Dict:
                self.nc_global = DAS_Dict["NC_GLOBAL"]
            
            if self.griddap:
                self.attribute_list = dc.getGriddapDimensions(self)
                

            # following logic does not apply to griddap
            else:
                if core.user_options.all_attributes_bool:
                    self.attribute_list = dc.getActualAttributes(self, return_all=True)
                else:
                    self.attribute_list = dc.getActualAttributes(self)

            # but this does, we still need to get time    
            if self.has_time:
                time_range = dc.getTimeFromJson(self)
                if time_range:
                    self.data_start_time, self.data_end_time = time_range
                    if self.data_start_time.tzinfo is None:
                        self.data_start_time = self.data_start_time.replace(tzinfo=timezone.utc)
                    if self.data_end_time.tzinfo is None:
                        self.data_end_time = self.data_end_time.replace(tzinfo=timezone.utc)
        except requests.RequestException as e:
            print(f"\nError fetching DAS for {self.dataset_id}: {e}")
            self.has_error = True
            self.DAS_response = False
        except Exception as e:
            print(f"\nError parsing DAS for {self.dataset_id}: {e}")
            self.DAS_response = False

    # def setGriddapTimes(self):
    #     # user
    #     # data
    #     # req
    #     if self.griddap_args["latest_bool"] == True:
    #         self.user_start_time, self.user_end_time = None, None
    #         self.req_start_time
    #         self.req_end_time = self.data_end_time

    #     elif self.griddap_args["user_data_end_time"] > self.data_end_time:
    #         print(f"\nThe requested end time {self.griddap_args["user_data_end_time"]} exceeds the datasets end time")


    @skipFromNoTime
    @skipFromNoRange
    def getDatasetSizes(self, timeOut_time: int = 120) -> None:
        """Gets row count for dataset from ERDDAP ncHeader response, sets to attribute"""
        if not self.DAS_response:
            return None
        if self.is_glider:
            return None
        if self.has_error:
            return None
        base_url = f"{self.server}{self.dataset_id}.ncHeader?"
        print(f"Requesting headers @ {base_url}")
        try:
            response = requests.get(base_url, timeout=timeOut_time)
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
            print(f"Request timed out after {timeOut_time} seconds for {self.dataset_id}, skipping")
            self.has_error = True
        except Exception as e:
            print(f"Error fetching dataset size: {e}")
        return None

    @skipFromNoTime
    @skipFromError
    @skipFromNoRange
    def needsSubsetting(self) -> bool:
        """Check if dataset needs to be split into chunks"""
        if self.row_count is not None:
            if self.row_count > self.chunk_size and not self.is_glider:
                print(f"\nUh oh! {self.dataset_title} is too big ({self.row_count} records) and needs to be chunked!")
                print("Calculating subsets...")
                self.needs_Subset = True
            else:
                self.needs_Subset = False

    @skipFromNoTime
    @skipFromError
    def calculateTimeSubset(self) -> dict:
        """Calculate time subsets based on row count.
        Method applies if self.needs_Subset is True"""
        chunk_size = self.chunk_size
        if not self.needs_Subset:
            return None
        try:
            start = self.data_start_time
            end = self.data_end_time
            total_records = self.row_count
            records_per_chunk = chunk_size
            chunks_needed = math.ceil(total_records / records_per_chunk)
            total_seconds = (end - start).total_seconds()
            seconds_per_record = total_seconds / total_records
            seconds_per_chunk = seconds_per_record * records_per_chunk
            time_chunks = {}
            chunk_start = start
            for i in range(chunks_needed):
                chunk_end = chunk_start + timedelta(seconds=seconds_per_chunk)
                if i == chunks_needed - 1:
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

    #---------------------URL Generation---------------------
    @skipFromError
    def generateUrl(self, dataformat: str = "csvp", nrt_update: bool = False) -> List[str]:
        """
        Build request URLs for data.
        Special handling for subsetting data.
        """

        urls = []
        # Prepare attributes
        attrs = []
        additionalAttr = self.attribute_list.copy() if self.attribute_list else []
        if additionalAttr and 'depth' in additionalAttr:
            additionalAttr.remove('depth')
            attrs.append('depth')
        if additionalAttr:
            attrs.extend(additionalAttr)
        if self.time_str in attrs:
            attrs.remove(self.time_str)
        attrs_encoded = '%2C'.join(attrs)
        
        if self.no_time_range:
            url = f"{self.server}{self.dataset_id}.{dataformat}?{attrs_encoded}"
            urls.append(url)
        elif not self.needs_Subset:
            urls = self.generateUrl_idv(dataformat, nrt_update, attrs_encoded)
        else:
            urls = self.generateUrl_sub(dataformat, attrs_encoded)
        
        self.url_s = urls
        return urls
    
    def generateUrl_idv(self, dataformat: str, nrt_update: bool, attrs_encoded: str) -> List[str]:
        """Generate URL for datasets not requiring subsetting."""
        urls = []
        try:
            if self.has_time and self.data_start_time:
                if nrt_update or self.is_nrt:
                    start = self.data_start_time
                    end = self.data_end_time
                else:
                    start = self.data_start_time.strftime('%Y-%m-%dT%H:%M:%S')
                    end = self.data_end_time.strftime('%Y-%m-%dT%H:%M:%S')
                time_constraints = f"&{self.time_str}%3E%3D{start}Z&{self.time_str}%3C%3D{end}Z"
                url = f"{self.server}{self.dataset_id}.{dataformat}?{self.time_str}%2C{attrs_encoded}{time_constraints}"
                urls.append(url)
            else:
                url = f"{self.server}{self.dataset_id}.{dataformat}?{attrs_encoded}"
                urls.append(url)
            return urls
        except Exception as e:
            print(f"\nThere was an error generating the URL for the dataset: {self.dataset_title}")
            self.has_error == True
            pass

    def generateUrl_sub(self, dataformat: str, attrs_encoded: str) -> List[str]:
        """Generate URLs for chunked (subset) datasets."""
        urls = []
        for i, (subset_name, times) in enumerate(self.subsetDict.items()):
            time_constraints = (
                f"&{self.time_str}%3E%3D{times['start']}Z"
                f"&{self.time_str}%3C%3D{times['end']}Z"
            )
            url = f"{self.server}{self.dataset_id}.{dataformat}?{self.time_str}%2C{attrs_encoded}{time_constraints}"
            urls.append(url)
        return urls
    
    # https://coastwatch.noaa.gov/erddap/griddap/noaacwBLENDEDsshDaily.nc?sla
    # %5B(2024-03-01T00:00:00Z)%5D%5B(-76.875):(54.125)%5D%5B(-135.875):(116.125)
    # %5D&.draw=surface&.vars=longitude%7Clatitude%7Csla
    
    def generateUrl_griddap(self, dataformat: str, attrs_encoded: str) -> str:
        urls = []
        additionalAttr = self.attribute_list.copy if self.attribute_list else []
        attrs = []
        common_vars = ["altitude", "latitiude", "longitude", "NC_GLOBAL"]
        dataformat = "nc"
        base_url = self.server

        # take tabledap out of the URL if relevant 
        if "tabledap" in base_url:
                new_url = base_url.replace("tabledap", "griddap")
                base_url = new_url

    def generateGriddap_url(self) -> List[str]:
        """
        Build ERDDAP griddap request URL(s).

        Uses self.griddap_args for time options and core.user_options for
        optional spatial bounds.  Returns the list of URL strings and
        sets self.url_s.
        """
        urls: List[str] = []

        
        # Establish base-URL and variables to request
        base_url = self.server.replace("tabledap", "griddap")
        dataformat = "nc"                                # griddap → netCDF
        # remove obvious coordinate / metadata names
        dim_tokens = {"time", "lat", "latitude", "lon", "longitude",
                    "altitude", "depth", "NC_GLOBAL"}
        variables = [v for v in (self.attribute_list or []) if v not in dim_tokens]

        if not variables:
            print(f"No data variables detected for {self.dataset_id}")
            return []

        # ERDDAP needs at least one variable.  We'll construct one URL per variable
        # if we need to change this we can join them with commas in one string.
        #Time selector
        def _iso_z(dt: datetime) -> str:
            """YYYY-MM-DDTHH:MM:SSZ, always forced to UTC."""
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # A. defaults fall back to full native range
        start_dt = self.data_start_time
        end_dt   = self.data_end_time
        time_sel = ""                     # will be filled below

        if self.griddap_args:

            # ----------  Case 1 : latest ----------------------------------
            if self.griddap_args.get("latest_bool", True):
                start_dt =  self.data_end_time
                end_dt = self.data_end_time
                time_sel = f"%5B({_iso_z(end_dt)})%5D"

            # ----------  Case 2 : user single date ------------------------
            elif self.griddap_args.get("user_single_date"):
                dt = self.griddap_args["user_single_date"]
                if isinstance(dt, str):
                    dt = datetime.fromisoformat(dt.replace("Z", ""))
                # clip to data range
                dt = max(self.data_start_time, min(dt, self.data_end_time))
                start_dt = end_dt = dt
                time_sel = f"%5B({_iso_z(dt)})%5D"

            # ----------  Case 3 : user time range -------------------------
            else:
                usr_s = self.griddap_args.get("user_start_time")
                usr_e = self.griddap_args.get("user_end_time")

                if isinstance(usr_s, str):
                    usr_s = datetime.fromisoformat(usr_s.replace("Z", ""))
                if isinstance(usr_e, str):
                    usr_e = datetime.fromisoformat(usr_e.replace("Z", ""))

                if usr_s:  start_dt = max(self.data_start_time, usr_s)
                if usr_e:  end_dt   = min(self.data_end_time,   usr_e)

                time_sel = f"%5B({_iso_z(start_dt)}):1:({_iso_z(end_dt)})%5D"

        self.req_start_time = start_dt
        self.req_end_time   = end_dt

        # # Build encoded time selector
        # if start_dt == end_dt:   # single timestep
        #     time_sel = f"%5B({_iso_z(end_dt)})%5D"
        # else:                    # range – 1-step stride
        #     time_sel = f"%5B({_iso_z(start_dt)}):1:({_iso_z(end_dt)})%5D"

        # 2.  Latitude / longitude selector  (optional)
        lat_sel = lon_sel = "%5B%5D"         # empty []  → full extent

        bounds = getattr(core.user_options, "bounds", None)
        if bounds and len(bounds) == 2:
            # bounds = [[lon_min, lat_min], [lon_max, lat_max]]
            lon_min, lat_min = bounds[0]    
            lon_max, lat_max = bounds[1]    

            
            lat_sel = f"%5B({lat_min}):({lat_max})%5D"
            lon_sel = f"%5B({lon_min}):({lon_max})%5D"

        # Assemble URLs   (one per variable)
        # que es multidimensional?
        for var in variables:
            var_enc = quote(var, safe="")                   # tidy variable name
            query = f"{var_enc}{time_sel}{lat_sel}{lon_sel}"
            url = f"{base_url}{self.dataset_id}.{dataformat}?{query}"
            urls.append(url)

        # Record and return
        self.url_s = urls
        return urls

    #---------------------Data Download---------------------
    @skipFromError
    def writeErddapData(self, connection_attempts: int = 3, timeout_time: int = 120) -> Union[str, List[str], None]:
        """
        Write ERDDAP data to CSV files.
        
        :param connection_attempts: Number of attempts to fetch a URL before giving up.
        :param timeout_time: Seconds before requests time out.
        """
        if not self.griddap:
            if not self.needs_Subset:
                return self._writeData_idv(connection_attempts, timeout_time)
            else:
                return self._writeData_sub(connection_attempts, timeout_time)
        else:
            return self._writeData_idv(connection_attempts, timeout_time=180)
    
    def _downloadUrl(self, url: str, timeout_time: int, subset_num: Optional[int] = None) -> Optional[str]:
        """
        Download `url` and save it to a temporary file.

        • tabledap  → CSV : parsed into a DataFrame, then written *.csv
        • griddap   → NetCDF: raw bytes written directly *.nc

        Returns the absolute file-path on success, or None on failure.
        """
        try:
            response = requests.get(url, timeout=timeout_time)
            response.raise_for_status()                    # 4xx / 5xx → exception

            # one call, common to both branches
            temp_dir = ec.getTempDir()

            # ----------------------  GRIDDAP  (NetCDF)  ----------------------
            if self.griddap:
                # choose file-name
                if self.needs_Subset and subset_num is not None:
                    filename = f"{self.dataset_id}_subset_{subset_num}.nc"
                else:
                    filename = f"{self.dataset_id}.nc"

                file_path = os.path.join(temp_dir, filename)

                # binary write
                with open(file_path, "wb") as f:
                    f.write(response.content)

                return file_path

            # ----------------------  TABLEDAP  (CSV)  -----------------------
            else:
                csv_data = StringIO(response.text)
                df = pd.read_csv(csv_data, header=None, low_memory=False)

                if self.needs_Subset and subset_num is not None:
                    filename = f"{self.dataset_id}_subset_{subset_num}.csv"
                else:
                    filename = f"{self.dataset_id}.csv"

                file_path = os.path.join(temp_dir, filename)
                df.to_csv(file_path, index=False, header=False)

                return file_path

        except requests.exceptions.Timeout as e:
            print(f"\nTimeout for URL: {url} | Error: {e}")
        except requests.exceptions.RequestException as e:
            print(f"\nRequest Exception | Error: {e}")
        except Exception as e:
            print(f"\nError processing URL | Exception: {e}")

        return None
    
    def _writeData_idv(self, connection_attempts: int, timeout_time: int) -> Optional[str]:
        """
        Download data from a single URL (non-subset case).
        Returns the file path on success, or None on failure.
        """
        url = self.url_s[0]
        attempts = 0
        filepath = None
        while attempts < connection_attempts and not filepath:
            attempts += 1
            print(f"\nDownloading data data for {self.dataset_title} (Attempt: {attempts}/{connection_attempts})")
            filepath = self._downloadUrl(url, timeout_time)
        if filepath:
            self.data_filepath = filepath
            return filepath
        else:
            self.has_error = True
            return None
    
    def _writeData_sub(self, connection_attempts: int, timeout_time: int) -> Optional[List[str]]:
        """
        Download data in subsets (chunked case).
        Returns a list of file paths on success, or None on failure.
        """
        print(f"\nDownloading data for {self.dataset_id}")
        filepaths = []
        urls_queue = deque([(url, i + 1) for i, url in enumerate(self.url_s)])
        attempts_dict = {url: 0 for url in self.url_s}
        
        while urls_queue:
            url, subset_index = urls_queue.popleft()
            attempts_dict[url] += 1
            attempt_num = attempts_dict[url]
            print(
                f"\nDownloading subset {subset_index}/{len(self.url_s)} "
                f"(Attempt: {attempt_num}/{connection_attempts})"
            )
            filepath = self._downloadUrl(url, timeout_time, subset_index)
            if filepath:
                filepaths.append(filepath)
            else:
                if attempt_num < connection_attempts:
                    urls_queue.append((url, subset_index))
                else:
                    print(f"\nMax retries exceeded for subset {subset_index} URL: {url}")
                    self.has_error = True
        if filepaths:
            self.data_filepath = filepaths
            return filepaths
        return None

    def calculateTimeRange(self, intervalType=None) -> int:
        start = datetime.fromisoformat(self.data_start_time)
        end = datetime.fromisoformat(self.data_end_time)
        if intervalType is None:
            return (end - start).days
        elif intervalType == "months":
            year_diff = end.year - start.year
            month_diff = end.month - start.month
            total_months = year_diff * 12 + month_diff
            return total_months
        else:
            raise ValueError("Invalid interval type.")

    def nrtTimeSet(self):
        """Sets data_start_time/data_end_time in ISO format (e.g., 2023-09-29T14:05:12)"""
        now_utc = datetime.now(timezone.utc)
        seven_days_ago = now_utc - timedelta(days=self.moving_window_days)
        self.data_start_time = seven_days_ago.strftime('%Y-%m-%dT%H:%M:%S')
        self.data_end_time = now_utc.strftime('%Y-%m-%dT%H:%M:%S')



# ---------------------------------
#This function checks if the dataset has data within the last 7 days
    # def checkDataRange(datasetid) -> bool:
    #     def movingWindow(self):        
    #         self.data_start_time = datetime.now() - timedelta(days=self.moving_window_days)
    #         self.data_end_time = datetime.now()
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



