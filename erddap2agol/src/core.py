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
        return ec.ERDDAPHandler.setErddap(ec.custom_server, 15)
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
def selectDatasetFromList(gcload, dispLength= 50) -> list:
    dataset_id_list = ec.ERDDAPHandler.getDatasetIDList(gcload)
    
    if len(dataset_id_list) >= dispLength:
        print(f"\n There are greater than {dispLength} datasets available on this server.")
        print(f"Datasets are shown {dispLength} datasets at a time.")
        print(f"Enter the number(s) of the datasets you want.")
        print(f"To move forward one page type next, to move backwards type back.")
        
        import math
        num_pages = math.ceil(len(dataset_id_list) / dispLength)
        current_page = 1
        input_list = []
        
        while True:
            start_index = (current_page - 1) * dispLength
            end_index = start_index + dispLength
            current_page_datasets = dataset_id_list[start_index:end_index]
            
            print(f"\nPage {current_page} of {num_pages}")
            print(f"Cart: {len(input_list)} datasets")
            for index, dataset in enumerate(current_page_datasets):
                print(f"{start_index + index + 1}. {dataset}")

            print("\nEnter the number of the dataset(s) you want to select...")
            idx_select = input(": ")
            
            if idx_select == "next":
                if current_page < num_pages:
                    current_page += 1
                else:
                    print("No more pages.")
            
            elif idx_select == "back":
                if current_page > 1:
                    current_page -= 1
                else:
                    print("Already at the first page.")
            
            elif idx_select == "exit":
                run.cui()
            
            elif idx_select == "done":
                print("\nPassing the following datasets to the next step...")
                print(f"{input_list}")
                return input_list
            
            elif idx_select == "all":
                for dataset in current_page_datasets:
                    input_list.append(dataset)
                
                print(f"Added all datasets on page {current_page} to the list.")
                            
            else:
                try:
                    idx_select = int(idx_select)
                    if 1 <= idx_select <= len(dataset_id_list):
                        selected_dataset = dataset_id_list[idx_select - 1]
                        input_list.append(selected_dataset)
                        print(f"Added {selected_dataset} to the list.")
                    else:
                        print("Invalid input")
                except ValueError:
                    print("You need to type a valid number or input.")
    
        


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

# When users provide multiple datasets for manual upload 
# Terminal
def agolPublishList(dataset_list, gcload, isNRT: int):
    if isNRT == 0:
        for dataset in dataset_list:
            attribute_list = parseDas(gcload, dataset)
            if attribute_list is None:
                print(f"\nNo data found for dataset {dataset}, trying next.")
                continue
            else:
                agolPublish(gcload, attribute_list, isNRT)           
        ec.cleanTemp()
    else:
        for dataset in dataset_list:
            attribute_list = parseDasNRT(gcload, dataset)
            if attribute_list is None:
                continue
            
            agolPublish(gcload, attribute_list, isNRT)           
        ec.cleanTemp()

def agolPublish_glider(gcload, attribute_list:list, isNRT: int, dataformat="geojson") -> None:

    full_url = gcload.generate_url(0, attribute_list)

    response = ec.ERDDAPHandler.return_response(full_url)
    filepath = ec.ERDDAPHandler.responseToCsv(gcload, response)

    geojson = aw.pointTableToGeojsonLine(filepath)
    geojson = json.dumps(geojson)

    propertyDict = aw.makeItemProperties(gcload)
    

    table_id = aw.publishTable(propertyDict, gcload.geoParams, geojson, gcload, inputDataType= dataformat)
    ul.updateLog(gcload.datasetid, table_id, "None", full_url, gcload.end_time, ul.get_current_time(), isNRT)
    ec.cleanTemp()

def agolPublishList_glider(dataset_list, gcload, isNRT: int):
    if isNRT == 0:
        for dataset in dataset_list:
            attribute_list = parseDas(gcload, dataset)
            if attribute_list is None:
                print(f"\nNo data found for dataset {dataset}, trying next.")
                continue
            else:
                agolPublish_glider(gcload, attribute_list, isNRT)           
        ec.cleanTemp()
    else:
        for dataset in dataset_list:
            attribute_list = parseDasNRT(gcload, dataset)
            if attribute_list is None:
                continue
            
            agolPublish_glider(gcload, attribute_list, isNRT)           
        ec.cleanTemp()



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
