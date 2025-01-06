from .src import erddap_wrangler as ec
from .src import data_wrangler as dw
from .src import agol_wrangler as aw
from .src import update_manager as um
from .src import core
from .src.core import gliderWorkflow, updateNRT
from arcgis.gis import GIS
import sys



#--------------Functions for Notebooks-----------------

def _gliderWorkflow(search_term: str = None):
    gliderWorkflow(search_term)

def _updateNRT():
    updateNRT()

#-----------------ERDDAP2AGOL CUI-----------------

def cui():
    while True:
        print("\nWelcome to ERDDAP2AGOL.")
        print("GCOOS GIS, 2025.")
        print("\n1. Create ERDDAP Datasets")
        print("2. Create Glider DAC Datasets")
        print("3. Create NRT Items")
        print("4. Find & Update NRT Items")
        print("5. Exit")

        user_choice = input(": ")  

        if user_choice == "1":
            default_add_menu()
        elif user_choice == "2":
            glider_add_menu()
        elif user_choice == "3":
            nrt_add_menu()
        elif user_choice == "4":
            core.updateNRT()
        elif user_choice == "5":
            print("Goodbye!")
            ec.cleanTemp()
            sys.exit()
        else:
            print("\nInvalid input. Please try again.")


def default_add_menu():
    print("\nCreate ERDDAP Item")
    erddapObj = core.erddapSelection()
      
    dataset_list = core.selectDatasetFromList(erddapObj)
    
    erddapObj.addDatasets_list(dataset_list)

    datasetObjlist = (erddapObj.datasets)

    for datasetObj in datasetObjlist:
        datasetObj.generateUrl()
        datasetObj.writeErddapData()

    agolObj = aw.AgolWrangler(erddap_obj= erddapObj)
    agolObj.datasets = erddapObj.datasets
    agolObj.makeItemProperties()
    agolObj.postAndPublish()
    print("\nReturning to main menu...")
    erddapObj.reset()
    cui()

def glider_add_menu():
    print("\nWelcome to the *Special* Glider DAC Menu.")

    erddapObj = core.erddapSelection(GliderServ=True)
    erddapObj.server = "https://gliders.ioos.us/erddap/tabledap/"
    
    dataset_list = core.selectDatasetFromList(erddapObj)
    
    erddapObj.addDatasets_list(dataset_list)

    datasetObjlist = (erddapObj.datasets)

    for datasetObj in datasetObjlist:
        datasetObj.generateUrl()
        datasetObj.writeErddapData()

    agolObj = aw.AgolWrangler(erddap_obj= erddapObj)
    agolObj.datasets = erddapObj.datasets
    agolObj.makeItemProperties()
    agolObj.pointTableToGeojsonLine()
    agolObj.postAndPublish()
    print("\nReturning to main menu...")
    erddapObj.reset()
    cui()

def nrt_add_menu():
    print("\nWelcome to the NRT Menu.")

    erddapObj = core.erddapSelection(nrtAdd= True)
    dataset_list = core.selectDatasetFromList(erddapObj)

    manager_obj = um.UpdateManager()
    manager_obj.searchContent()
    dup_removed_list = core.findExistingNRT(manager_obj, dataset_list)

    if len(dup_removed_list) > 0:
        dataList = dup_removed_list
    else:
        dataList = dataset_list 

    erddapObj.addDatasets_list(dataList)

    datasetObjlist = (erddapObj.datasets)

    for datasetObj in datasetObjlist:
        datasetObj.generateUrl()
        datasetObj.writeErddapData()

    agolObj = aw.AgolWrangler(erddap_obj= erddapObj)
    agolObj.datasets = erddapObj.datasets
    agolObj.makeItemProperties()
    agolObj.postAndPublish()
    print("\nReturning to main menu...")
    erddapObj.reset()
    cui()


if __name__ == '__main__':
    cui()