import sys, os, requests, json, pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import tempfile
from . import data_wrangler as dw

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
    filepath = os.path.join('/arcgis/home', 'e2a_temp')
    if os.path.exists(filepath):
        for filename in os.listdir(filepath):
            if filename.endswith(".csv") or filename.endswith("geojson"):
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
    def __init__(self, server, serverInfo, datasetid, fileType, geoParams):
        self.availData = None
        self.server = server
        self.serverInfo = serverInfo
        self.datasetid = datasetid
        self.fileType = fileType
        self.geoParams = geoParams
        self.datasets = []
        self.is_nrt = False
        
    
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
    
    # def get_unprocessed(self) -> List[dw.DatasetWrangler]:
    #         return [d for d in self.datasets if not d.is_processed]
    

    def getDatasetIDList(self) -> list:
        """Fetches a list of dataset IDs from the ERDDAP server.
           spits out list of dataset IDs"""
        url = f"{self.serverInfo}"
        try:
            response = requests.get(url)
            data = response.json()
            
            if not data.get('table') or 'columnNames' not in data['table']:
                print(f"Invalid response format from {url}")
                return []

            column_names = data['table']['columnNames']
            dataset_id_index = column_names.index("Dataset ID") if "Dataset ID" in column_names else None
            
            if dataset_id_index is None:
                print("Dataset ID column not found in response")
                return []

            rows = data['table']['rows']
            dataset_id_list = [row[dataset_id_index] for row in rows if row[dataset_id_index] != "allDatasets"]
            
            return dataset_id_list

        except Exception as e:
            print(f"Error fetching dataset ID list: {e}")
            return []
        
    #This was occuring later than I thought, and it might not be nessecary 
    def addDatasets_list(self, dataset_ids: list) -> None:
        """Creates DatasetWrangler objects for each dataset ID"""
        for dataset_id in dataset_ids:
            dataset = dw.DatasetWrangler(
                dataset_id= dataset_id,
                server= self.server,
                is_nrt= self.is_nrt
            )
            self.datasets.append(dataset)
    
    
    #Gets dataset DAS    

        
    def setErddap(self, erddapIndex: int):
        filepath = getErddapList()
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        if erddapIndex > len(data) or erddapIndex < 1:
            print(f"Please select a number between 1 and {len(data)}.")
            return None
        else:
            erddap_dict = data[erddapIndex - 1]
            print(f"\nSelected ERDDAP Server: {erddap_dict['name']}")

            server_obj = custom_server
            baseurl = erddap_dict['url']

            # Remove index.html and trailing slashes
            if baseurl.endswith("index.html"):
                baseurl = baseurl[:-10].rstrip('/')

            try:
                serv_check = requests.get(baseurl)
                serv_check.raise_for_status()

                server_url = f"{baseurl}/tabledap/"
                setattr(server_obj, 'server', server_url)

                # Set server info URL 
                server_info_url = f"{baseurl}/info/index.json?itemsPerPage=100000"
                setattr(server_obj, 'serverInfo', server_info_url)

                return server_obj
            
            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error {http_err} occurred when connecting to {baseurl}")
                return None

        
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


    #--------------------------------------------------------------------------------

    # def responseToCsv(self, response: any) -> str:
    #     csvResponse = response[0]
    #     responseCode = response[1]
    #     if responseCode != 200:
    #         return None
    #     try:
    #         csvData = StringIO(csvResponse)

    #         df = pd.read_csv(csvData, header=None, low_memory=False)

    #         temp_dir = getTempDir()
    #         file_path = os.path.join(temp_dir, f"{self.datasetid}.csv")

    #         df.to_csv(file_path, index=False, header=False)

    #         return file_path
    #     except Exception as e:
    #         print(f"Error converting response to CSV: {e}")
    #         return None


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

custom_server = ERDDAPHandler(
    server = None,
    serverInfo = None,
    datasetid = None,
    fileType = None,
    geoParams = {
    "locationType": "coordinates",
    "latitudeFieldName": "latitude__degrees_north_",
    "longitudeFieldName": "longitude__degrees_east_",
    "timeFieldName": "time__UTC_",
    },
)

# # class ERDDAPHandler:
#     def __init__(self, server, serverInfo, datasetid, attributes, fileType, longitude, latitude, time, start_time, end_time, geoParams):
#         self.availData = None
#         self.server = server
#         self.serverInfo = serverInfo
#         self.datasetid = datasetid
#         self.attributes = attributes
#         self.fileType = fileType
#         self.geoParams = geoParams
#         self.datasets = []
#         self.is_nrt = False

