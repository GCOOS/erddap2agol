import sys, os, requests, json, pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import tempfile
from . import data_wrangler as dw
from . import das_client as dc
from erddap2agol import run


#--------------------------------------------------------------------------------


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def getTempDir() -> os.PathLike:
    # Check if running in AGOL Notebook environment
    if os.path.exists('/arcgis/home'):
        temp_dir = os.path.join('/arcgis/home', 'e2a_temp')
    else:
        # Use the system's temporary directory
        temp_dir = tempfile.gettempdir()
        temp_dir = os.path.join(temp_dir, 'erddap_temp')
    os.makedirs(temp_dir, exist_ok=True)
    return temp_dir

def cleanTemp() -> None:
    dc.cleanConfDir()
    filepath = os.path.join('/arcgis/home', 'e2a_temp')
    if os.path.exists(filepath):
        for filename in os.listdir(filepath):
            if filename.endswith(".csv") or filename.endswith("geojson") or filename.endswith("nc") or filename.endswith("xml"):
                full_path = os.path.join(filepath, filename)
                try:
                    os.remove(full_path)
                except Exception as e:
                    print(f"An unexpected error occurred while deleting {full_path}: {e}")
    else:
        print(f"The directory {filepath} does not exist.")


#--------------------------------------------------------------------------------

#Sometimes the directory or file isnt created 
def getErddapConfDir():
    agol_home = os.getenv('AGOL_HOME', '/arcgis/home')
    base_dir = agol_home
    erddap_conf_dir = os.path.join(base_dir, 'e2a_erddap_conf')
    os.makedirs(erddap_conf_dir, exist_ok=True)

    return os.path.join(erddap_conf_dir, 'active_erddaps.json')

def getErddapList() -> None:
    url = "https://raw.githubusercontent.com/IrishMarineInstitute/awesome-erddap/master/erddaps.json"
    response = requests.get(url)
    
    if response.status_code == 200:
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            print(f"Error decoding ERDDAP list from {url}")
            print(f"Error: {e}")
            return
        
        filepath = getErddapConfDir()

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)
        
        return filepath
    else:
        print(f"\nFailed to fetch ERDDAP List from {url}.") 
        print(f"Status code: {response.status_code}")
        return None

def showErddapList() -> None:
    filepath = getErddapList()
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    for index, erddap in enumerate(data, start=1):
        print(f"{index}. ERDDAP Server: {erddap['name']}")



#--------------------------------------------------------------------------------
class ERDDAPHandler:
    """
    Lightweight wrapper for an ERDDAP server (griddap / tabledap).

    You can instantiate it with no arguments and later call
    `setErddap(idx)` (your existing helper).  If you *do* pass a
    `server` URL now, the object is immediately usable.
    """

    def __init__(
        self,
        server: str | None = None,
        serverInfo: str | None = None,
        protocol: str | None = "griddap",
        datasetid: str | None = None,
        fileType: str | None = None,
        geoParams: dict | None = None,
    ):
        # core connection details -------------------------------------------------
        self.server       = server                      
        self.protocol     = (protocol or "griddap").lower()
        self.datasetid    = datasetid
        self.fileType     = fileType
        self.geoParams    = geoParams or {
            "locationType":      "coordinates",
            "latitudeFieldName": "latitude",
            "longitudeFieldName": "longitude",
            "timeFieldName":     "time",
        }

        # choose a sensible default for serverInfo if possible -------------------
        if serverInfo is not None:
            self.serverInfo = serverInfo
        elif server is not None:
            self.serverInfo = f"{server.rstrip('/')}/erddap/search/index.json"
        else:
            # will be filled in by setErddap()
            self.serverInfo = None

        # per-query working copies 
        self.datasets:       list[str]                  = []
        self.dataset_titles: dict[str, str]             = {}
        self.dataset_dates:  dict[str, tuple[str, str]] = {}

        # date cache
        self.date_range_cache: dict[str, tuple[str, str]] = {}
        self._date_cache_ready: bool                      = False

        # NRT attributes
        self.is_nrt             = False
        self.moving_window_days = 7

        self._availData = None

    @classmethod            
    def setErddap(cls, erddapIndex: int):
        """ 
        Important: This method is used to intialize an erddap object instance from the IMI list of erddaps

        Loads the json list from IMI and defines server url attributes of ERDDAPHandler
        based upon the index of a user input.
        
        Returns ERDDAPHandler obj.
        """
        # hardcoding protocol here
        protocol = "tabledap"
        filepath = getErddapList()
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        if erddapIndex > len(data) or erddapIndex < 1:
            print(f"\nOf the {len(data)} options, input:{erddapIndex} was not one of them.")
            run.cui()            
        else:
            erddap_dict = data[erddapIndex - 1]
            print(f"\nSelected ERDDAP Server: {erddap_dict['name']}")

            # server_obj = custom_server
            baseurl = erddap_dict['url']

            # Remove index.html and trailing slashes
            if baseurl.endswith("index.html"):
                baseurl = baseurl[:-10].rstrip('/')

            try:
                serv_check = requests.get(baseurl)
                serv_check.raise_for_status()

                server_url = f"{baseurl}/{protocol}/"
              

                # Set server info URL 
                # https://www.ncei.noaa.gov/erddap/tabledap/allDatasets.json
                server_info_url = f"{baseurl}tabledap/allDatasets.json"
                # server_info_url = f"{baseurl}/info/index.json?itemsPerPage=100000"
                

                return cls(
                    server= server_url,
                    serverInfo= server_info_url,
                    protocol= protocol,
                    datasetid=None,
                    fileType = None,
                    geoParams = {
                    "locationType": "coordinates",
                    "latitudeFieldName": "latitude__degrees_north_",
                    "longitudeFieldName": "longitude__degrees_east_",
                    "timeFieldName": "time__UTC_",
                    },
                )
            
            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error {http_err} occurred when connecting to {baseurl}")
                return None
    @property
    def availData(self): 
        if self._availData is None:
            self._availData = self.getDatasetIDList()
        return self._availData

    @availData.setter 
    def availData(self, value):
        """Set available datasets"""
        self._availData = value   

    def reset(self) -> None:
        """
        Reset all class attributes to none.

        Called after postAndPublish in the CUI
        """
        self.availData = None
        self.server = None
        self.serverInfo = None
        self.datasetid = None
        self.fileType = None
        self.geoParams = None
        self.datasets = []
        self.is_nrt = False

    def __iter__(self):
        """Make ERDDAPHandler directly iterable"""
        return iter(self.datasets)
    
    def __len__(self):
        """Get number of datasets"""
        return len(self.datasets)
    
    def __getitem__(self, index):
        """Allow index access to datasets"""
        return self.datasets[index]
    
    def buildDateCache(self) -> None:
     
        if self._date_cache_ready:
            return  # already built

        # Choose an endpoint that actually includes min/max dates
        original_info = self.serverInfo or ""
        if original_info.endswith("allDatasets.json"):
            # the handler was already initialised with the right URL
            cache_url = original_info
        else:
            if not self.server:
                raise ValueError(
                    "server URL not set.  Call setErddap() or pass `server=` "
                    "when instantiating ERDDAPHandler."
                )
            cache_url = f"{self.server.rstrip('/')}{self.protocol}/allDatasets.json"

        self.serverInfo = cache_url

        # `getDatasetIDList()` will now fill self.dataset_dates w/t ranges
        _ = self.getDatasetIDList()

        # take a snapshot for later restores
        self.date_range_cache  = self.dataset_dates.copy()
        self._date_cache_ready = True

        # restore the caller's URL (important for subsequent calls)
        self.serverInfo = original_info or cache_url
    
  
    def getDatasetIDList(self) -> list:
        """
        Return a list of datasetIDs available from the current serverInfo URL
        and populate self.dataset_titles / self.dataset_dates.
        """

        def _find_idx(name_list, *candidates):
            """Return the index of the first candidate column name (case-insensitive)."""
            low_names = [n.lower() for n in name_list]
            for cand in candidates:
                if cand.lower() in low_names:
                    return low_names.index(cand.lower())
            return None
        # ---------------------------------------------------------

        try:
            resp = requests.get(self.serverInfo, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            cols  = data["table"]["columnNames"]
            rows  = data["table"]["rows"]

            idx_id   = _find_idx(cols, "datasetID", "Dataset ID")
            idx_ttl  = _find_idx(cols, "title", "Title")
            idx_proto= _find_idx(cols, self.protocol)
            idx_min  = _find_idx(cols, "minTime", "Min Time")
            idx_max  = _find_idx(cols, "maxTime", "Max Time")

            if idx_id is None or idx_proto is None:
                print("Couldn’t locate mandatory columns in ERDDAP response.")
                return []

            id_list = []
            self.dataset_titles.clear()
            self.dataset_dates.clear()

            for row in rows:
                ds_id   = row[idx_id]
                proto   = row[idx_proto]

                if idx_proto is not None and row[idx_proto] == "":
                    continue
                if ds_id == "allDatasets":
                    continue

                title = row[idx_ttl] if idx_ttl is not None else ""
                min_t = row[idx_min] if idx_min is not None else ""
                max_t = row[idx_max] if idx_max is not None else ""

                id_list.append(ds_id)
                self.dataset_titles[ds_id] = title
                self.dataset_dates[ds_id]  = (min_t, max_t)

            return id_list

        except Exception as exc:
            print(f"Error fetching dataset ID list: {exc}")
            return []
        
    def createDatasetObjects(self, dataset_ids: list, griddap_kwargs: dict= None) -> None:
        """Creates DatasetWrangler objects for each dataset ID from the attributes of the selected data"""
        if "gliders.ioos.us" in self.server:
            gliderBool = True
        else:
            gliderBool = False
            
        if self.protocol == "griddap":
            griddap_bool = True
            kwargs = griddap_kwargs

            
        else:
            griddap_bool = False
            kwargs = None

        for dataset_id in dataset_ids:
            dataset = dw.DatasetWrangler(
                dataset_id= dataset_id,
                dataset_title=(self.dataset_titles.get(dataset_id)),
                server= self.server,
                griddap= griddap_bool,
                is_nrt= self.is_nrt,
                is_glider= gliderBool,
                griddap_args=kwargs
            )
            self.datasets.append(dataset)
    
    
       
    def getDatasetsFromSearch(self, search: str) -> list:
        url = f"{self.serverInfo}"
        try:
            responseObj = requests.get(url)
        
            if responseObj.status_code != 200:
                print(f"Error fetching dataset list: {responseObj.status_code}")
                return None


        except Exception as e:
            print(f"Error using getDatasetsFromSearch: {e}")
            return None
                
    def fetchData(self, url):
        response, responseCode = self.return_response(url)
        if responseCode != 200:
            print(f"Error fetching data: {responseCode}")
            return None  
        return pd.read_csv(StringIO(response))

    def filterAttributesWithData(self, data, attributes):
        valid_attributes = []
        for attr in attributes:
            if attr in data.columns and data[attr].notna().any():
                valid_attributes.append(attr)
        return valid_attributes
    

    #Works
    def attributeRequest(self, attributes: list) -> list:
        oldStart = self.start_time
        oldEnd = self.end_time

        time_list = self.iterateTime("days", 7)

        self.start_time = time_list[0]
        self.end_time = time_list[-1]
        
        generated_url = self.generate_url(isSeed=True, additionalAttr=attributes)

        data = self.fetchData(generated_url)

        if data is None:
            return None

        valid_attributes = self.filterAttributesWithData(data, attributes)

        self.start_time = oldStart
        self.end_time = oldEnd

        return valid_attributes

    # Creates a list of time values between start and end time
    def iterateTime(self, incrementType: str, increment: int) -> list:
        timeList = []
        start = datetime.fromisoformat(self.start_time)
        end = datetime.fromisoformat(self.end_time)
        current = start
        if incrementType == "days":
            while current <= end:
                timeList.append(current.isoformat())
                current += datetime.timedelta(days=increment)
        elif incrementType == "hours":
            while current <= end:
                timeList.append(current.isoformat())
                current += datetime.timedelta(hours=increment)
        return timeList
    


    @staticmethod
    def return_response(generatedUrl: str):
        try:
            response = requests.get(generatedUrl)
            response.raise_for_status()
            return response.text, response.status_code
        except Exception as err:
            print(f"Unexpected error occurred: {err}")
            return None, None

