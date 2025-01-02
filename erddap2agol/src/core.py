#Runtime logic consolidated here
import sys, os, requests, json
import time  # Add this import at the top with other imports
from . import erddap_client as ec
from . import das_client as dc
from . import ago_wrapper as aw
from . import data_wrangler as dw
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
 # Survives refactor
def erddapSelection(GliderServ = False, nrtAdd = False) -> ec.ERDDAPHandler:
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
                if nrtAdd is True:
                    erddapObj.is_nrt = True
                    return erddapObj
                else:
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

# Survives refactor
def selectDatasetFromList(erddapObj, dispLength=75) -> list:
    """The big search function that allows users to search datasets and select them for processing.
        
        Returns a list of selected datasets. Should pass to the erddapObj list constructor."""
    def _updateDatasetList(erddapObj, search_term=None):
        original_info = erddapObj.serverInfo
        base_url = original_info.split('/erddap/')[0] + '/erddap'
        
        if erddapObj.is_nrt is True:
            
            search_url = (
                f"{base_url}/search/advanced.json?"
                f"page=1&itemsPerPage=10000000&minTime=now-7days&maxTime=&protocol=tabledap"
            )

            print(f"\nSEARCH URL: {search_url}")
            erddapObj.serverInfo = search_url
            dataset_id_list = erddapObj.getDatasetIDList()
            erddapObj.serverInfo = original_info
            return dataset_id_list

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
    
    def clearScreen():
        import os
        os.system('cls' if os.name == 'nt' else 'clear')
    
    while True:
        clearScreen()
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
            clearScreen()
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
     
# needs change     
def findBigDatasets(dataset_list: list, erddapObj: ec.ERDDAPHandler) -> dict:
    """
    Creates nested dictionary of datasets and their time subsets
    Returns: {datasetid: {subset1: {start:time, end:time}, subset2:{...}}}
    """
    time_dict = {}
    sizeDict = dw.getDatasetSizes(dataset_list, erddapObj)
    sizeDict_filtered = {k: v for k, v in sizeDict.items() if v > 45000}
    bypassDatasets = [k for k, v in sizeDict.items() if k not in sizeDict_filtered]
    
    for dataset_id, row_count in sizeDict_filtered.items():
        setattr(erddapObj, "datasetid", dataset_id)
        # Here we will do parse das so we can properly get the time subset

        time_params = erddapObj.calculateTimeSubset(row_count)
        time_dict[dataset_id] = time_params
        
    return time_dict
    
            
# DAS parsing and attribute definitions for non-NRT datasets
# Wraps getDas, parseDasResponse, convertToDict, saveToJson, openDasJson, getActualAttributes, convertFromUnix, displayAttributes

# obviously needs changes, has moved to data_wrangler 
def parseDas(erddapObj, dataset):
    das_resp = ec.ERDDAPHandler.getDas(erddapObj, dataset)
    
    if das_resp is None:
        print(f"\nNo data found for dataset {dataset}.")
        return None
    
    parsed_response = dc.convertToDict(dc.parseDasResponse(das_resp))
    dc.saveToJson(parsed_response, dataset)
    #print(f"\nDas converted to JSON successfully")

    
    attribute_list = dc.getActualAttributes(dc.openDasJson(dataset), erddapObj)

    #Time stuff starts here
    unixtime = (dc.getTimeFromJson(dataset))
    start, end = dc.convertFromUnix(unixtime)
    
    setattr(erddapObj, "start_time", start)
    setattr(erddapObj, "end_time", end)
    setattr(erddapObj, "datasetid", dataset)
    setattr(erddapObj, "attributes", attribute_list)

    timeintv = ec.ERDDAPHandler.calculateTimeRange(erddapObj)
    dc.displayAttributes(timeintv, attribute_list)
    
    # Ask about seed file after showing record count
    if getattr(erddapObj, 'seed_choice', None) is None:
        seed_choice = input(f"\nDataset contains {timeintv} days of records. Would you like to create a seed file? (y/n): ").lower()
        setattr(erddapObj, 'seed_choice', seed_choice == 'y')
    
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

    window_start, window_end = dw.movingWindow(isStr=True)

    overlapBool = dw.checkDataRange(dataset)
    
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
def check_dataset_exists(dataset_id: str) -> bool:
    """Check if dataset already exists in AGOL by searching for its tag."""
    try:
        # Search for items with the dataset ID tag
        search_results = aw.searchContentByTag(dataset_id)
        return len(search_results) > 0
    except Exception as e:
        print(f"Error checking dataset existence: {e}")
        return False

def agolPublish(erddapObj, attribute_list:list, isNRT: int, skip_check: bool = False) -> None:
    start_time = time.time()
    print(f"\nProcessing dataset: {erddapObj.datasetid}")
    
    # Check if dataset exists unless skip_check is True
    if not skip_check and check_dataset_exists(erddapObj.datasetid):
        print(f"\nWarning: Dataset {erddapObj.datasetid} already exists in AGOL. Skipping processing.")
        return
    
    if isNRT == 0:
        seedbool = getattr(erddapObj, 'seed_choice', False)
    else:
        seedbool = False

    full_url = erddapObj.generate_url(seedbool, attribute_list)
    response = ec.ERDDAPHandler.return_response(full_url)
    filepath = ec.ERDDAPHandler.responseToCsv(erddapObj, response)

    if filepath:
        
        propertyDict = aw.makeItemProperties(erddapObj)

        table_id = aw.postAndPublish(propertyDict, erddapObj.geoParams, filepath, erddapObj)
        ul.updateLog(erddapObj.datasetid, table_id, "None", full_url, erddapObj.end_time, ul.get_current_time(), isNRT)
        ec.cleanTemp()
        
        end_time = time.time()
        processing_time = end_time - start_time
        print(f"Processing completed in {processing_time:.2f} seconds")
    else:
        end_time = time.time()
        processing_time = end_time - start_time
        print(f"Processing failed after {processing_time:.2f} seconds")
        print(f"Skipping {erddapObj.datasetid} due to bad response.")

# Modified agol publish function for glider datasets
def agolPublish_glider(erddapObj, attribute_list:list, isNRT: int, dataformat="geojson", skip_check: bool = False) -> None:

    # Check if dataset exists unless skip_check is True
    if not skip_check and check_dataset_exists(erddapObj.datasetid):
        print(f"\nWarning: Dataset {erddapObj.datasetid} already exists in AGOL. Skipping processing.")
        return

    full_url = erddapObj.generate_url(0, attribute_list)

    response = ec.ERDDAPHandler.return_response(full_url)
    filepath = ec.ERDDAPHandler.responseToCsv(erddapObj, response)

    geojson_path = aw.pointTableToGeojsonLine(filepath, erddapObj)
    
    propertyDict = aw.makeItemProperties(erddapObj)
       
    table_id = aw.postAndPublish(propertyDict, erddapObj.geoParams, geojson_path, erddapObj, inputDataType= dataformat)

    ul.updateLog(erddapObj.datasetid, table_id, "None", full_url, erddapObj.end_time, ul.get_current_time(), isNRT)
    ec.cleanTemp()


# When users provide multiple datasets for manual upload 
# Terminal
def agolPublishList(dataset_list, erddapObj, isNRT: int, skip_check: bool = False):
    if not dataset_list:
        print("No datasets to process.")
        return

    total_start_time = time.time()
    processed_count = 0
    skipped_count = 0
    
    # Store original server info
    original_info = erddapObj.serverInfo
    
    # Get available datasets using original server info
    erddapObj.serverInfo = original_info
    available_datasets = ec.ERDDAPHandler.getDatasetIDList(erddapObj)
    
    # Determine which publish function to use based on the server flag
    is_glider_server = getattr(erddapObj, 'is_glider', False)
    publish_function = agolPublish_glider if is_glider_server else agolPublish

    if isNRT == 0:
        for dataset in dataset_list:
            dataset_start_time = time.time()
            print(f"\nProcessing dataset: {dataset}")
            
            if dataset not in available_datasets:
                print(f"Dataset ID '{dataset}' not found in the list of available datasets.")
                continue

            # Check if dataset exists unless skip_check is True
            if not skip_check and check_dataset_exists(dataset):
                print(f"\nWarning: Dataset {dataset} already exists in AGOL. Skipping processing.")
                skipped_count += 1
                continue

            attribute_list = parseDas(erddapObj, dataset)
            if attribute_list is None:
                print(f"\nNo data found for dataset '{dataset}', trying next.")
                continue
            else:
                if is_glider_server:
                    publish_function(erddapObj, attribute_list, isNRT, skip_check=skip_check)
                else:
                    publish_function(erddapObj, attribute_list, isNRT, skip_check=skip_check)
            
            dataset_end_time = time.time()
            processing_time = dataset_end_time - dataset_start_time
            processed_count += 1
            print(f"Dataset processed in {processing_time:.2f} seconds")
        ec.cleanTemp()
    else:
        for dataset in dataset_list:
            dataset_start_time = time.time()
            print(f"\nProcessing dataset: {dataset}")
            
            if dataset not in available_datasets:
                print(f"Dataset ID '{dataset}' not found in the list of available datasets.")
                continue

            # Check if dataset exists unless skip_check is True
            if not skip_check and check_dataset_exists(dataset):
                print(f"\nWarning: Dataset {dataset} already exists in AGOL. Skipping processing.")
                skipped_count += 1
                continue

            attribute_list = parseDasNRT(erddapObj, dataset)
            if attribute_list is None:
                print(f"\nNo data found for dataset '{dataset}', trying next.")
                continue
            publish_function(erddapObj, attribute_list, isNRT, skip_check=skip_check)
            dataset_end_time = time.time()
            processing_time = dataset_end_time - dataset_start_time
            processed_count += 1
            print(f"Dataset processed in {processing_time:.2f} seconds")
        ec.cleanTemp()

    total_end_time = time.time()
    total_time = total_end_time - total_start_time
    print(f"\nProcessing completed for {processed_count} datasets")
    if skipped_count > 0:
        print(f"Skipped {skipped_count} datasets that already existed in AGOL")
    print(f"Total processing time: {total_time:.2f} seconds")
    print("\nAll done!")



###################################
##### Functions for Notebooks #####
###################################

def NRTUpdateAGOL(skip_check: bool = True) -> None:
    #This is hardcoded for GCOOS ERDDAP
    erddapObj = ec.erddapGcoos    

    nrt_dict  = dw.NRTFindAGOL()
    for datasetid, itemid in nrt_dict.items():
        if datasetid and itemid:
            try: 
                startWindow, endWindow = dw.movingWindow(isStr=True)
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

def gliderWorkflow(search_term: str = None, isNRT: int = 0, skip_check: bool = False) -> None:
    """
    Automates the workflow for glider data:
    1. Selects glider ERDDAP server
    2. Searches for datasets with given search term
    3. Processes and publishes found datasets
    
    Args:
        search_term (str, optional): Term to search for in dataset names. Defaults to None.
        isNRT (int, optional): Whether to treat as near-real-time data. Defaults to 0.
        skip_check (bool, optional): Whether to skip the dataset existence check. Defaults to False.
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
            agolPublishList(dataset_list, erddapObj, isNRT, skip_check=skip_check)
        else:
            print(f"No datasets found matching search term '{search_term}'")
    else:
        print("No search term provided")
