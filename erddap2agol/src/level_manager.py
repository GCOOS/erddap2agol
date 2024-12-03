from . import erddap_client as ec
from . import das_client as dc
from . import ago_wrapper as aw
from logs import updatelog as ul
from src.utils import OverwriteFS
from arcgis.gis import GIS

#from utils import OverwriteFS

import sys, os, datetime
from datetime import timedelta, datetime



################## NRT Functions ##################

#This function returns the start and end time of the moving window
def movingWindow(isStr: bool):
    if isStr == True:
        start_time = datetime.now() - timedelta(days=7)
        end_time = datetime.now()
        return start_time.isoformat(), end_time.isoformat()
    else:
        start_time = datetime.now() - timedelta(days=7)
        end_time = datetime.now()
        return start_time, end_time

#This function checks if the dataset has data within the last 7 days
def checkDataRange(datasetid) -> bool:
    startDas, endDas = dc.convertFromUnixDT(dc.getTimeFromJson(datasetid))
    window_start, window_end = movingWindow(isStr=False)
    if startDas <= window_end and endDas >= window_start:
        return True
    else:
        return False  

#This function returns all datasetIDs that have data within the last 7 days
#Maybe request a fresh json everytime?
def batchNRTFind(ERDDAPObj: ec.ERDDAPHandler) -> list:
    ValidDatasetIDs = []
    DIDList = ec.ERDDAPHandler.getDatasetIDList(ERDDAPObj)
    for datasetid in DIDList:
        if dc.checkForJson(datasetid) == False:
            das_resp = ec.ERDDAPHandler.getDas(ERDDAPObj, datasetid=datasetid)
            parsed_response = dc.parseDasResponse(das_resp)
            parsed_response = dc.convertToDict(parsed_response)
            dc.saveToJson(parsed_response, datasetid)
        

            if checkDataRange(datasetid) == True:
                ValidDatasetIDs.append(datasetid)
        else:
            if checkDataRange(datasetid) == True:
                ValidDatasetIDs.append(datasetid)
    
    print(f"Found {len(ValidDatasetIDs)} datasets with data within the last 7 days.")
    return ValidDatasetIDs

def NRTFindAGOL() -> list:
    nrt_dict  = ul.updateCallFromNRT(1)
    return nrt_dict

################## Historical Functions ##################

    
