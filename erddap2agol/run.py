from .src import erddap_wrangler as ec
from .src import data_wrangler as dw
from .src import agol_wrangler as aw
from .src import update_manager as um
from .src import core
from .src.core import gliderWorkflow, updateNRT
from arcgis.gis import GIS
import sys


#--------------Functions for Notebooks-----------------

def _updateNRT(verbose_opt: bool = True, preserveProps_opt: bool=True, 
               ignoreAge_opt: bool=True, noProps_opt: bool=False, timeout_Time = 300, max_workers: int = 4) -> None:
    """verbose_opt: bool = True, preserveProps_opt: bool=True, ignoreAge_opt: bool=True, timeoutTime = 300
    Searches your ArcGIS Online account for datasets with the NRT tags, then runs the 
    typical NRT post, but passes a URL providing OFS with the destination data"""
    updateNRT(verbose_opt, preserveProps_opt, ignoreAge_opt, noProps_opt, timeout_Time, max_workers)


def _gliderWorkflow(search_term: str = None) -> None:
    """
    This will be deprecated soon as program-based workflows are refined.
    Automates the workflow for glider data:
    Args:
        search_term (str, optional): Term to search for in dataset names. Defaults to None.
        isNRT (int, optional): Whether to treat as near-real-time data. Defaults to 0.
        skip_check (bool, optional): Whether to skip the dataset existence check. Defaults to False.
        """
    gliderWorkflow(search_term)


#-----------------ERDDAP2AGOL CUI-----------------

def cui():
    ec.cleanTemp()
    while True:
        print("\nWelcome to ERDDAP2AGOL.")
        print("GCOOS GIS, 2025.")
        print("\n1. Create ERDDAP Datasets")
        print("2. Create Glider DAC Datasets")
        print("3. Create NRT Items")
        print("4. Find & Update NRT Items")
        print("5. Options Menu")
        print("6. Exit & Clean Temp")

        user_choice = input(": ")  

        if user_choice == "1":
            add_menu("Create static ERDDAP Item")
        elif user_choice == "2":
            add_menu("Create Glider DAC Tracks", glider=True)
        elif user_choice == "3":
            add_menu("Create NRT Items", nrt=True)
        elif user_choice == "4":
            _updateNRT()
        elif user_choice == "5":
            core.options_menu()
        elif user_choice == "6":
            print("Goodbye!")
            ec.cleanTemp()
            sys.exit()
        else:
            print("\nInvalid input. Please try again.")

def add_menu(menu_title:str, glider: bool = False, nrt: bool = False):
    print(f"\nWelcome to {menu_title}")

    # pass user params to erddapSelection
    erddapObj = core.erddapSelection(
        GliderServ=glider,
        nrtAdd=nrt
    )

    # create list of dataset_id str and add to erddapObj
    dataset_list = core.selectDatasetFromList(erddapObj)

    # Transforms list into dataset objects 
    erddapObj.addDatasets_list(dataset_list)

    datasetObjlist = (erddapObj.datasets)

    for datasetObj in datasetObjlist:
        datasetObj.generateUrl()
        datasetObj.writeErddapData()
    
    agolObj = aw.AgolWrangler(erddap_obj=erddapObj)
    agolObj.datasets = erddapObj.datasets
    agolObj.makeItemProperties()

    if glider:
        agolObj.pointTableToGeojsonLine()

    agolObj.postAndPublish()
    print("\nReturning to main menu...")
    erddapObj.reset()
    ec.cleanTemp()
    cui()


if __name__ == '__main__':
    cui()