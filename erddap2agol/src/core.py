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
        gcload = ec.ERDDAPHandler.setErddap(ec.custom_server, 15)
        return gcload
    else:
        ec.getErddapList()
        ec.showErddapList()
        uc = input("\nSelect an ERDDAP server to use: ")
        if uc:
            gcload = ec.ERDDAPHandler.setErddap(ec.custom_server, int(uc))
            print(f"\nSelected server: {gcload.server}")
            uc = input("Proceed with server selection? (y/n): ")

            if uc.lower() == "y":
                print("\nContinuing with selected server...")
                return gcload
            else:
                print("\nReturning to main menu...")
                return None
        else:
            print("\nInput cannot be none")
            return None
        
# Select dataset from list and return list of datasets
# This includes logic not found elsewhere, not a wrapper like other core funcs.
# need to handle misinputs
def selectDatasetFromList(gcload, dispLength=50) -> list:
    dataset_id_list = ec.ERDDAPHandler.getDatasetIDList(gcload)
    
    if len(dataset_id_list) >= dispLength:
        print(f"\nThere are more than {dispLength} datasets available on this server.")
        print(f"Datasets are shown {dispLength} at a time.")
        print(f"Enter the number(s) of the datasets you want, separated by commas.")
        print(f"To move forward one page type 'next', to move backwards type 'back'.")
        
        import math
        num_pages = math.ceil(len(dataset_id_list) / dispLength)
        current_page = 1
        input_list = []
        
        def clear_screen():
            import os
            os.system('cls' if os.name == 'nt' else 'clear')
        
        while True:
            clear_screen()
            start_index = (current_page - 1) * dispLength
            end_index = min(start_index + dispLength, len(dataset_id_list))
            current_page_datasets = dataset_id_list[start_index:end_index]
            
            print(f"\nPage {current_page} of {num_pages}")
            print(f"Cart: {len(input_list)} datasets")
            for index, dataset in enumerate(current_page_datasets):
                print(f"{start_index + index + 1}. {dataset}")
    
            print("\nEnter the number(s) of the dataset(s) you want to select, or type 'next', 'back', 'all', 'done', or 'exit'.")
            idx_select = input(": ").strip().lower()
            
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
            elif idx_select == "all":
                for dataset in current_page_datasets:
                    if dataset not in input_list:
                        input_list.append(dataset)
                print(f"Added all datasets on page {current_page} to the list.")
                input("Press Enter to continue...")
            else:
                try:
                    # Handle multiple indices separated by commas
                    indices = [int(i.strip()) for i in idx_select.split(',')]
                    for idx in indices:
                        if start_index < idx <= end_index:
                            selected_dataset = dataset_id_list[idx - 1]
                            if selected_dataset not in input_list:
                                input_list.append(selected_dataset)
                                print(f"Added {selected_dataset} to the list.")
                            else:
                                print(f"{selected_dataset} is already in the list.")
                        else:
                            print(f"Invalid input. Number {idx} out of range for this page.")
                    input("Press Enter to continue...")
                except ValueError:
                    print("Invalid input. Please enter valid numbers separated by commas or a command.")
                    input("Press Enter to continue...")
    
        


# DAS parsing and attribute definitions for non-NRT datasets
# Wraps getDas, parseDasResponse, convertToDict, saveToJson, openDasJson, getActualAttributes, convertFromUnix, displayAttributes
def parseDas(gcload, dataset):
    das_resp = ec.ERDDAPHandler.getDas(gcload, dataset)
    
    if das_resp is None:
        print(f"\nNo data found for dataset {dataset}.")
        return None
    
    parsed_response = dc.convertToDict(dc.parseDasResponse(das_resp))
    dc.saveToJson(parsed_response, dataset)
    print(f"\nDas converted to JSON successfully")

    
    attribute_list = dc.getActualAttributes(dc.openDasJson(dataset), gcload)

    unixtime = (dc.getTimeFromJson(dataset))
    start, end = dc.convertFromUnix(unixtime)
    
    setattr(gcload, "start_time", start)
    setattr(gcload, "end_time", end)
    setattr(gcload, "datasetid", dataset)
    setattr(gcload, "attributes", attribute_list)

    timeintv = ec.ERDDAPHandler.calculateTimeRange(gcload)
    dc.displayAttributes(timeintv, attribute_list)
    
    return attribute_list

# DAS parsing and attribute definitions for NRT datasets
def parseDasNRT(gcload, dataset) -> list:
    das_resp = ec.ERDDAPHandler.getDas(gcload, dataset)
    if das_resp is None:
        print(f"\nNo data found for dataset {dataset}.")
        return None
    
    parsed_response = dc.convertToDict(dc.parseDasResponse(das_resp))
    fp = dc.saveToJson(parsed_response, dataset)
    print(f"\nDas converted to JSON successfully")

    
    attribute_list = dc.getActualAttributes(dc.openDasJson(dataset), gcload)

    window_start, window_end = lm.movingWindow(isStr=True)

    overlapBool = lm.checkDataRange(dataset)
    
    if overlapBool == False:
        print(f"\nNo data found for dataset {dataset} within the last 7 days.")
        return None
    
    else:
        setattr(gcload, "start_time", window_start)
        setattr(gcload, "end_time", window_end)
        setattr(gcload, "datasetid", dataset)
        setattr(gcload, "attributes", attribute_list)

        timeintv = ec.ERDDAPHandler.calculateTimeRange(gcload)
        dc.displayAttributes(timeintv, attribute_list)
        
        return attribute_list
    

# AGOL publishing and log updating
# Terminal
def agolPublish(gcload, attribute_list:list, isNRT: int) -> None:
    if isNRT == 0:
        seed_choice = input("Would you like to create a seed file? (y/n): ").lower()
        seedbool = seed_choice
    else:
        seedbool = False

    full_url = gcload.generate_url(seedbool, attribute_list)
    response = ec.ERDDAPHandler.return_response(full_url)
    filepath = ec.ERDDAPHandler.responseToCsv(gcload, response)

    if filepath:
        
        propertyDict = aw.makeItemProperties(gcload)

        table_id = aw.publishTable(propertyDict, gcload.geoParams, filepath, gcload)
        ul.updateLog(gcload.datasetid, table_id, "None", full_url, gcload.end_time, ul.get_current_time(), isNRT)
        ec.cleanTemp()
    else:
        print(f"Skipping {gcload.datasetid} due to bad response.")

# Modified agol publish function for glider datasets
def agolPublish_glider(gcload, attribute_list:list, isNRT: int, dataformat="geojson") -> None:

    full_url = gcload.generate_url(0, attribute_list)

    response = ec.ERDDAPHandler.return_response(full_url)
    filepath = ec.ERDDAPHandler.responseToCsv(gcload, response)

    geojson_path = aw.pointTableToGeojsonLine(filepath, gcload)
    
    propertyDict = aw.makeItemProperties(gcload)
       
    table_id = aw.publishTable(propertyDict, gcload.geoParams, geojson_path, gcload, inputDataType= dataformat)

    ul.updateLog(gcload.datasetid, table_id, "None", full_url, gcload.end_time, ul.get_current_time(), isNRT)
    ec.cleanTemp()


# When users provide multiple datasets for manual upload 
# Terminal
def agolPublishList(dataset_list, gcload, isNRT: int):
    if not dataset_list:
        print("No datasets to process.")
        return

    available_datasets = ec.ERDDAPHandler.getDatasetIDList(gcload)
    
    # Determine which publish function to use based on the server
    if gcload.server == "https://gliders.ioos.us/erddap/tabledap/":
        publish_function = agolPublish_glider
    else:
        publish_function = agolPublish

    if isNRT == 0:
        for dataset in dataset_list:
            if dataset not in available_datasets:
                print(f"Dataset ID '{dataset}' not found in the list of available datasets.")
                continue
            attribute_list = parseDas(gcload, dataset)
            if attribute_list is None:
                print(f"\nNo data found for dataset '{dataset}', trying next.")
                continue
            else:
                publish_function(gcload, attribute_list, isNRT)
        ec.cleanTemp()
    else:
        for dataset in dataset_list:
            if dataset not in available_datasets:
                print(f"Dataset ID '{dataset}' not found in the list of available datasets.")
                continue
            attribute_list = parseDasNRT(gcload, dataset)
            if attribute_list is None:
                print(f"\nNo data found for dataset '{dataset}', trying next.")
                continue
            publish_function(gcload, attribute_list, isNRT)
        ec.cleanTemp()

    print("\nAll done!")



###################################
##### Functions for Notebooks #####
###################################

def NRTUpdateAGOL() -> None:
    gcload = ec.erddapGcoos    

    nrt_dict  = lm.NRTFindAGOL()
    for datasetid, itemid in nrt_dict.items():
        if datasetid and itemid:
            try: 
                startWindow, endWindow = lm.movingWindow(isStr=True)
                das_resp = ec.ERDDAPHandler.getDas(gcload, datasetid)
                parsed_response = dc.convertToDict(dc.parseDasResponse(das_resp))
                fp = dc.saveToJson(parsed_response, datasetid)
                das_data = dc.openDasJson(datasetid)
                attribute_list = dc.getActualAttributes(das_data, gcload)

                setattr(gcload, "start_time", startWindow)
                setattr(gcload, "end_time", endWindow)
                setattr(gcload, "datasetid", datasetid)
                setattr(gcload, "attributes", attribute_list)

                url = gcload.generate_url(False, attribute_list)

                gis = aw.agoConnect()
                
                content = gis.content.get(itemid)

                OverwriteFS.overwriteFeatureService(content, url, verbose=True, preserveProps=False, ignoreAge = True)
            
            except Exception as e:
                    print(f"Error: {e}")
                    pass
