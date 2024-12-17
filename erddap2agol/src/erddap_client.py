#ERDDAP stuff is handled here with the ERDDAPHandler class.
import sys, os, requests, json
from datetime import datetime, timedelta
import pandas as pd
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from io import StringIO
import tempfile

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
            if filename.endswith(".csv"):
                full_path = os.path.join(filepath, filename)
                try:
                    os.remove(full_path)
                except Exception as e:
                    print(f"An unexpected error occurred while deleting {full_path}: {e}")
    else:
        print(f"The directory {filepath} does not exist.")

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
    def __init__(self, server, serverInfo, datasetid, attributes, fileType, longitude, latitude, time, start_time, end_time, geoParams):
        self.server = server
        self.serverInfo = serverInfo
        self.datasetid = datasetid
        self.attributes = attributes
        self.fileType = fileType
        self.longitude = longitude
        self.latitude = latitude
        self.time = time
        self.start_time = start_time
        self.end_time = end_time
        self.geoParams = geoParams

   
    def getDatasetIDList(self) -> list:
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

        
    #Gets dataset DAS    
    def getDas(self, datasetid: str) -> str:
        dataset_id_list = self.getDatasetIDList()
        if datasetid not in dataset_id_list:
            print(f"\nDataset ID {datasetid} not found in the list of available datasets.")
            return None
        else:
            url = f"{self.server}{datasetid}.das"
            response = requests.get(url)
            return response.text
        
        
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
                
    # Generates URL for ERDDAP request based on class object attributes
    def generate_url(self, isSeed: bool, additionalAttr: list = None, dataformat="csvp", seedDays = 7) -> str:
        # the attribute list
        attrs = []

        if additionalAttr and 'depth' in additionalAttr:
            additionalAttr.remove('depth')
            attrs.append('depth')

        attrs.extend([self.longitude, self.latitude])

        if additionalAttr:
            attrs.extend(additionalAttr)

        # Finally, add 'time'
        attrs.append(self.time)

        # Join the attributes into the URL
        attrs_str = '%2C'.join(attrs)

        # Construct time constraints
        if isSeed:
            if isinstance(self.start_time, str):
                self.start_time = datetime.strptime(self.start_time, '%Y-%m-%dT%H:%M:%S')
            endtime_seed = self.start_time + timedelta(days= seedDays)
            endtime_seed_str = endtime_seed.strftime('%Y-%m-%dT%H:%M:%S')
            start_time_str = self.start_time.strftime('%Y-%m-%dT%H:%M:%S')
            time_constraints = (
                f"&{self.time}%3E%3D{start_time_str}Z"
                f"&{self.time}%3C%3D{endtime_seed_str}Z"
            )
            print(f"Start Time: {start_time_str}", f"End Time: {endtime_seed_str}")
        else:
            time_constraints = (
                f"&{self.time}%3E%3D{self.start_time}Z"
                f"&{self.time}%3C%3D{self.end_time}Z"
            )

        # Construct the full URL
        url = (
            f"{self.server}{self.datasetid}.{dataformat}?"
            f"{attrs_str}"
            f"{time_constraints}"
        )

        
        print(f"\nGenerated URL: {url}")

        return url
        
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



    #Works and important. Breaks when no lat or lon lol. 
    def responseToCsv(self, response: any) -> str:
        csvResponse = response[0]
        responseCode = response[1]
        if responseCode != 200:
            return None
        try:
            csvData = StringIO(csvResponse)

            df = pd.read_csv(csvData, header=None, low_memory=False)

            temp_dir = getTempDir()
            file_path = os.path.join(temp_dir, f"{self.datasetid}.csv")

            df.to_csv(file_path, index=False, header=False)

            return file_path
        except Exception as e:
            print(f"Error converting response to CSV: {e}")
            return None

    #Works and important
    def responseToJson(self, response: any) -> str:
        jsonResponse = response
        jsonData = StringIO(jsonResponse)

        df = pd.read_json(jsonData, orient='records')

        currentpath = os.getcwd()
        directory = "/temp/"
        file_path = f"{currentpath}{directory}{self.datasetid}.json"
        print(file_path)

        df.to_json(file_path, orient='records')

        return file_path

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
    
    # We will use this to decide how to chunk the dataset
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


    # Creates a seed URL to download a small amount of data. There are probably better ways to just grab the first record.
    def createSeedUrl(self, additionalAttr: list = None) -> str:
        oldStart = self.start_time
        oldEnd = self.end_time

        time_list = self.iterateTime("hours", 3)

        self.start_time = time_list[0]
        self.end_time = time_list[1]
        generated_url = self.generate_url(True, additionalAttr)

        self.start_time = self.end_time
        self.end_time = oldEnd
        return generated_url

    #Last update is read from database, currentTime is from current time function
    @staticmethod
    def generateUpdateUrl(full_url: str, last_update: str, currentTime: str) -> str:
        if '?' in full_url:
            base_url, query_string = full_url.split('?', 1)
        else:
            base_url = full_url
            query_string = ""

        # Split along encoding
        params = query_string.split('&')

        updated_params = []

        #Note: time params are hardcoded here.
        for param in params:
            if param.startswith('time%3E%3D'):
                updated_params.append(f"time%3E%3D{last_update}Z")
            elif param.startswith('time%3C%3D'):
                updated_params.append(f"time%3C%3D{currentTime}Z")
            else:
                updated_params.append(param)


        # Join the updated parameters back into a query string
        updated_query_string = '&'.join(updated_params)

        updated_url = f"{base_url}?{updated_query_string}"

        return updated_url

    @staticmethod
    def updateObjectfromParams(erddapObject: "ERDDAPHandler", params: dict) -> None:
        for key, value in params.items():
            setattr(erddapObject, key, value)


    @staticmethod
    def return_response(generatedUrl: str):
        try:
            response = requests.get(generatedUrl)
            response.raise_for_status()
            return response.text, response.status_code
        except Exception as err:
            print(f"Unexpected error occurred: {err}")
            return None, None


    @staticmethod
    def get_current_time() -> str:
        return str(datetime.datetime.now().isoformat())
    



# Below we can specify different configurations for the ERDDAP object.

# Since lat/lon and time are essentially default parameters, we can set them here.

erddapGcoos = ERDDAPHandler(
    server='https://erddap.gcoos.org/erddap/tabledap/',
    serverInfo = 'https://erddap.gcoos.org/erddap/info/index.json',
    datasetid = None,
    attributes=None,
    fileType = None,
    longitude = "longitude",
    latitude = "latitude",
    time = 'time',
    start_time = None,
    end_time = None,
    geoParams = {"locationType": "coordinates",
        "latitudeFieldName": "latitude (degrees_north)",
        "longitudeFieldName": "longitude (degrees_east)"}
)

coastwatch = ERDDAPHandler(
    server='https://coastwatch.pfeg.noaa.gov/erddap/tabledap/',
    serverInfo = 'https://coastwatch.pfeg.noaa.gov/erddap/info/index.json',
    datasetid = None,
    attributes=None,
    fileType = None,
    longitude = "longitude",
    latitude = "latitude",
    time = 'time',
    start_time = None,
    end_time= None,
    geoParams = {"locationType": "coordinates",
        "latitudeFieldName": "latitude (degrees_north)",
        "longitudeFieldName": "longitude (degrees_east)"}
    )

custom_server = ERDDAPHandler(
    server= None,
    serverInfo = None,
    datasetid = None,
    attributes=None,
    fileType = None,
    longitude = "longitude",
    latitude = "latitude",
    time = 'time',
    start_time = None,
    end_time= None,
    geoParams = {
    "locationType": "coordinates",
    "latitudeFieldName": "latitude__degrees_north_",
    "longitudeFieldName": "longitude__degrees_east_",
    "timeFieldName": "time__UTC_",
    }
)

