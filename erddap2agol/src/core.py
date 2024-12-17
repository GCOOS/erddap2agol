#Runtime logic consolidated here
import sys, os, requests, json
from . import erddap_client as ec
from . import das_client as dc
from . import ago_wrapper as aw
from . import level_manager as lm
from erddap2agol import run
from logs import updatelog as ul
from src.utils import OverwriteFS

from arcgis.gis import GIS

###################################
###### CUI Wrapper Functions ######
###################################

# Disable print
def blockPrint():
    sys.stdout = open(os.devnull, 'w')

# Restore print
def enablePrint():
    sys.stdout = sys.__stdout__



def checkInputForList(user_input):
    return ',' in user_input

def inputToList(user_input) -> list:
    dataset_list = [dataset.strip() for dataset in user_input.split(',')]
    return dataset_list

 # Show erddap menu and define gcload with selection
def erddapSelection(GliderServ = False) -> ec.ERDDAPHandler:
    if GliderServ == True:
        erddapObj = ec.ERDDAPHandler.setErddap(ec.custom_server, 15)
        return erddapObj
    else:
        ec.getErddapList()
        ec.showErddapList()
        uc = input("\nSelect an ERDDAP server to use: ")
        if uc:
            erddapObj = ec.ERDDAPHandler.setErddap(ec.custom_server, int(uc))
            print(f"\nSelected server: {erddapObj.server}")
            uc = input("Proceed with server selection? (y/n): ")

            if uc.lower() == "y":
                print("\nContinuing with selected server...")
                return erddapObj
            else:
                print("\nReturning to main menu...")
                return None
        else:
            print("\nInput cannot be none")
            return None
        
# Select dataset from list and return list of datasets
# This includes logic not found elsewhere, not a wrapper like other core funcs.
# need to handle misinputs
def selectDatasetFromList(erddapObj, dispLength=50) -> list:
    def _updateDatasetList(erddapObj, search_term=None):
        if search_term:
            original_info = erddapObj.serverInfo
            # Extract the base URL (remove everything after /erddap/)
            base_url = original_info.split('/erddap/')[0] + '/erddap'
            # Construct the proper search URL
            search_url = f"{base_url}/search/index.json?searchFor={search_term}&page=1&itemsPerPage=100000&protocol=tabledap"
            erddapObj.serverInfo = search_url
            
            #print(f"Searching using URL: {search_url}")  # Debug print
            dataset_id_list = erddapObj.getDatasetIDList()
            erddapObj.serverInfo = original_info
            return dataset_id_list
        return erddapObj.getDatasetIDList()

    dataset_id_list = _updateDatasetList(erddapObj)
    
    if len(dataset_id_list) < dispLength:
        dispLength = len(dataset_id_list)
    
    print(f"\nDatasets are shown {dispLength} at a time.")
    print("Enter the number of the datasets you want and press enter.")
    print("To search datasets, type 'search:keyword'")
    print("To move forward one page type 'next', to move backwards type 'back'.")
    
    import math
    if len(dataset_id_list) == 0:
        print("No datasets found.")
        exit()
    num_pages = math.ceil(len(dataset_id_list) / dispLength)
    current_page = 1
    input_list = []
    
    def clear_screen():
        import os
        os.system('cls' if os.name == 'nt' else 'clear')
    
    while True:
        clear_screen()
        print(dataset_id_list)
        start_index = (current_page - 1) * dispLength
        end_index = min(start_index + dispLength, len(dataset_id_list))
        current_page_datasets = dataset_id_list[start_index:end_index]
        
        print(f"\nPage {current_page} of {num_pages}")
        print(f"Cart: {len(input_list)} datasets")

        for index, dataset in enumerate(current_page_datasets):
            # Split the dataset info to get just the ID for the cart
            dataset_id = dataset.split(' - ')[0] if ' - ' in dataset else dataset
            print(f"{start_index + index + 1}. {dataset}")

        print("\nEnter the number(s) of the dataset(s) you want to select")
        print("Commands: 'next', 'back', 'addAll', 'addPage', 'done', 'exit', or 'search:keyword'")
        idx_select = input(": ")
        
        if idx_select.startswith('search:'):
            search_term = idx_select.split(':', 1)[1]

            print(f"\nSearching for: {search_term}")

            dataset_id_list = _updateDatasetList(erddapObj, search_term)

            if not dataset_id_list:
                print("No datasets found matching your search.")
                input("Press Enter to continue...")
            num_pages = math.ceil(len(dataset_id_list) / dispLength)
            current_page = 1
            continue

        if idx_select == "next":
            if current_page < num_pages:
                current_page += 1
            else:
                print("No more pages.")
                input("Press Enter to continue...")

        elif idx_select == "back":
            if current_page > 1:
                current_page -= 1
            else:
                print("Already at the first page.")
                input("Press Enter to continue...")
        elif idx_select == "exit":
            run.cui()

        elif idx_select == "done":
            print("\nPassing the following datasets to the next step...")
            print(f"{input_list}")
            return input_list
        
        elif idx_select == "addAll":
            for dataset in dataset_id_list:  # Changed from current_page_datasets to dataset_id_list
                if dataset not in input_list:
                    input_list.append(dataset)
            print(f"Added all {len(dataset_id_list)} datasets to the list.")
            input("Press Enter to continue...")

        elif idx_select == "addPage":
            for dataset in current_page_datasets:
                if dataset not in input_list:
                    input_list.append(dataset)
            print(f"Added all datasets on page {current_page} to the list.")
            input("Press Enter to continue...")
        # If user input is int, theyre adding datasets
        else:
            try:
                indices = [i.strip() for i in idx_select.split(',')]
                valid_selection = False
                for idx in indices:
                    if idx.isdigit():
                        idx_int = int(idx)
                        if start_index < idx_int <= start_index + len(current_page_datasets):
                            selected_dataset = dataset_id_list[idx_int - 1]
                            if selected_dataset not in input_list:
                                input_list.append(selected_dataset)
                                print(f"Added {selected_dataset} to the list.")
                            else:
                                print(f"{selected_dataset} is already in the list.")
                            valid_selection = True
                        else:
                            print(f"Invalid input. Number {idx_int} out of range for this page.")
                    else:
                        print(f"Invalid input '{idx}'. Please enter valid numbers or commands.")
                if not valid_selection:
                    input("Press Enter to continue...")
            except Exception as e:
                print("An unexpected error occurred:", e)
                input("Press Enter to continue...")

    
        


# DAS parsing and attribute definitions for non-NRT datasets
# Wraps getDas, parseDasResponse, convertToDict, saveToJson, openDasJson, getActualAttributes, convertFromUnix, displayAttributes
def parseDas(erddapObj, dataset):
    das_resp = ec.ERDDAPHandler.getDas(erddapObj, dataset)
    
    if das_resp is None:
        print(f"\nNo data found for dataset {dataset}.")
        return None
    
    parsed_response = dc.convertToDict(dc.parseDasResponse(das_resp))
    dc.saveToJson(parsed_response, dataset)
    print(f"\nDas converted to JSON successfully")

    
    attribute_list = dc.getActualAttributes(dc.openDasJson(dataset), erddapObj)

    unixtime = (dc.getTimeFromJson(dataset))
    start, end = dc.convertFromUnix(unixtime)
    
    setattr(erddapObj, "start_time", start)
    setattr(erddapObj, "end_time", end)
    setattr(erddapObj, "datasetid", dataset)
    setattr(erddapObj, "attributes", attribute_list)

    timeintv = ec.ERDDAPHandler.calculateTimeRange(erddapObj)
    dc.displayAttributes(timeintv, attribute_list)
    
    return attribute_list

# DAS parsing and attribute definitions for NRT datasets
def parseDasNRT(erddapObj, dataset) -> list:
    das_resp = ec.ERDDAPHandler.getDas(erddapObj, dataset)
    if das_resp is None:
        print(f"\nNo data found for dataset {dataset}.")
        return None
    
    parsed_response = dc.convertToDict(dc.parseDasResponse(das_resp))
    fp = dc.saveToJson(parsed_response, dataset)
    print(f"\nDas converted to JSON successfully")

    
    attribute_list = dc.getActualAttributes(dc.openDasJson(dataset), erddapObj)

    window_start, window_end = lm.movingWindow(isStr=True)

    overlapBool = lm.checkDataRange(dataset)
    
    if overlapBool == False:
        print(f"\nNo data found for dataset {dataset} within the last 7 days.")
        return None
    
    else:
        setattr(erddapObj, "start_time", window_start)
        setattr(erddapObj, "end_time", window_end)
        setattr(erddapObj, "datasetid", dataset)
        setattr(erddapObj, "attributes", attribute_list)

        timeintv = ec.ERDDAPHandler.calculateTimeRange(erddapObj)
        dc.displayAttributes(timeintv, attribute_list)
        
        return attribute_list
    

# AGOL publishing and log updating
# Terminal
def agolPublish(erddapObj, attribute_list:list, isNRT: int) -> None:
    if isNRT == 0:
        seedbool = getattr(erddapObj, 'seed_choice', None)
        if seedbool is None:
            seed_choice = input("Would you like to create a seed file? (y/n): ").lower()
            seedbool = seed_choice == 'y'
    else:
        seedbool = False

    full_url = erddapObj.generate_url(seedbool, attribute_list)
    response = ec.ERDDAPHandler.return_response(full_url)
    filepath = ec.ERDDAPHandler.responseToCsv(erddapObj, response)

    if filepath:
        
        propertyDict = aw.makeItemProperties(erddapObj)

        table_id = aw.publishTable(propertyDict, erddapObj.geoParams, filepath, erddapObj)
        ul.updateLog(erddapObj.datasetid, table_id, "None", full_url, erddapObj.end_time, ul.get_current_time(), isNRT)
        ec.cleanTemp()
    else:
        print(f"Skipping {erddapObj.datasetid} due to bad response.")

# Modified agol publish function for glider datasets
def agolPublish_glider(erddapObj, attribute_list:list, isNRT: int, dataformat="geojson") -> None:

    full_url = erddapObj.generate_url(0, attribute_list)

    response = ec.ERDDAPHandler.return_response(full_url)
    filepath = ec.ERDDAPHandler.responseToCsv(erddapObj, response)

    geojson_path = aw.pointTableToGeojsonLine(filepath, erddapObj)
    
    propertyDict = aw.makeItemProperties(erddapObj)
       
    table_id = aw.publishTable(propertyDict, erddapObj.geoParams, geojson_path, erddapObj, inputDataType= dataformat)

    ul.updateLog(erddapObj.datasetid, table_id, "None", full_url, erddapObj.end_time, ul.get_current_time(), isNRT)
    ec.cleanTemp()


# When users provide multiple datasets for manual upload 
# Terminal
def agolPublishList(dataset_list, erddapObj, isNRT: int):
    if not dataset_list:
        print("No datasets to process.")
        return

    # Store original server info
    original_info = erddapObj.serverInfo
    
    # Get available datasets using original server info
    erddapObj.serverInfo = original_info
    available_datasets = ec.ERDDAPHandler.getDatasetIDList(erddapObj)
    
    # Determine which publish function to use based on the server flag
    is_glider_server = getattr(erddapObj, 'is_glider', False)
    publish_function = agolPublish_glider if is_glider_server else agolPublish

    # Only ask for seed files if not glider data
    if isNRT == 0 and not is_glider_server:
        seed_choice = input("Would you like to create seed files? (y/n): ").lower() == 'y'
        erddapObj.seed_choice = seed_choice

    if isNRT == 0:
        for dataset in dataset_list:
            if dataset not in available_datasets:
                print(f"Dataset ID '{dataset}' not found in the list of available datasets.")
                continue
            attribute_list = parseDas(erddapObj, dataset)
            if attribute_list is None:
                print(f"\nNo data found for dataset '{dataset}', trying next.")
                continue
            else:
                if is_glider_server:
                    publish_function(erddapObj, attribute_list, isNRT)
                else:
                    # Pass the seed_choice to agolPublish
                    erddapObj.seed_choice = seed_choice
                    publish_function(erddapObj, attribute_list, isNRT)
        ec.cleanTemp()
    else:
        for dataset in dataset_list:
            if dataset not in available_datasets:
                print(f"Dataset ID '{dataset}' not found in the list of available datasets.")
                continue
            attribute_list = parseDasNRT(erddapObj, dataset)
            if attribute_list is None:
                print(f"\nNo data found for dataset '{dataset}', trying next.")
                continue
            publish_function(erddapObj, attribute_list, isNRT)
        ec.cleanTemp()

    print("\nAll done!")



###################################
##### Functions for Notebooks #####
###################################

def NRTUpdateAGOL() -> None:
    #This is hardcoded for GCOOS ERDDAP
    erddapObj = ec.erddapGcoos    

    nrt_dict  = lm.NRTFindAGOL()
    for datasetid, itemid in nrt_dict.items():
        if datasetid and itemid:
            try: 
                startWindow, endWindow = lm.movingWindow(isStr=True)
                das_resp = ec.ERDDAPHandler.getDas(erddapObj, datasetid)
                parsed_response = dc.convertToDict(dc.parseDasResponse(das_resp))
                fp = dc.saveToJson(parsed_response, datasetid)
                das_data = dc.openDasJson(datasetid)
                attribute_list = dc.getActualAttributes(das_data, erddapObj)

                setattr(erddapObj, "start_time", startWindow)
                setattr(erddapObj, "end_time", endWindow)
                setattr(erddapObj, "datasetid", datasetid)
                setattr(erddapObj, "attributes", attribute_list)

                url = erddapObj.generate_url(False, attribute_list)

                gis = aw.agoConnect()
                
                content = gis.content.get(itemid)


                OverwriteFS.overwriteFeatureService(content, url, verbose=True, preserveProps=False, ignoreAge = True)
            
            except Exception as e:
                    print(f"Error: {e}")
                    pass

def gliderWorkflow(search_term: str = None, isNRT: int = 0) -> None:
    """
    Automates the workflow for glider data:
    1. Selects glider ERDDAP server
    2. Searches for datasets with given search term
    3. Processes and publishes found datasets
    
    Args:
        search_term (str, optional): Term to search for in dataset names. Defaults to None.
        isNRT (int, optional): Whether to treat as near-real-time data. Defaults to 0.
    """
    # Get glider server
    erddapObj = erddapSelection(GliderServ=True)
    if not erddapObj:
        print("Failed to connect to glider server")
        return

    # Set server explicitly rather than comparing
    erddapObj.server = "https://gliders.ioos.us/erddap/tabledap/"
    erddapObj.is_glider = True  # Add flag to identify as glider server
    
    if search_term:
        # Store original server info
        original_info = erddapObj.serverInfo
        # Construct search URL
        base_url = original_info.split('/erddap/')[0] + '/erddap'
        search_url = f"{base_url}/search/index.json?searchFor={search_term}&page=1&itemsPerPage=100000&protocol=tabledap"
        erddapObj.serverInfo = search_url
        
        # Get matching datasets
        dataset_list = erddapObj.getDatasetIDList()
        # Restore original server info
        erddapObj.serverInfo = original_info

        if dataset_list:
            print(f"\nFound {len(dataset_list)} datasets matching search term '{search_term}'")
            # Process and publish datasets
            agolPublishList(dataset_list, erddapObj, isNRT)
        else:
            print(f"No datasets found matching search term '{search_term}'")
    else:
        print("No search term provided")
