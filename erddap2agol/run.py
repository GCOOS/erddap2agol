from .src import erddap_client as ec
from .src import level_manager as lm
from .src import core
from arcgis.gis import GIS

#-----------------ERDDAP2AGOL CUI-----------------
# This will be eventually cleaned up

def cui():
    while True:
        print("\nWelcome to ERDDAP2AGOL.")
        print("GCOOS GIS, 2024.")
        print("\n1. Create ERDDAP Items")
        print("2. Create ERDDAP NRT Items")
        print("3. Glider DAC *Special* Menu")
        print("4. Legacy Add (Manual Input)")

        user_choice = input(": ")  

        if user_choice == "1":
            create_erddap_item_menu()
        elif user_choice == "2":
            nrt_creation()
        elif user_choice == "3":
            glider_menu()
        elif user_choice == "4":
            legacy_add_menu()
        else:
            print("\nInvalid input. Please try again.")

def create_erddap_item_menu():
    print("\nCreate ERDDAP Item")
    erddapObj = core.erddapSelection()
    if not erddapObj:
        cui()
        return
    dataset_list = core.selectDatasetFromList(erddapObj)
    if dataset_list:
        core.agolPublishList(dataset_list, erddapObj, 0)
    print("\nReturning to main menu...")
    cui()

def legacy_add_menu():
    print("\nLegacy Add - Manual Dataset Input")
    print("Select the server of the dataset you want to create an AGOL item for.")

    erddapObj = core.erddapSelection()
    if not erddapObj:
        cui()
        return

    print("\nEnter the datasetid(s) for the dataset you want to create an AGOL item for.")
    print("Separate multiple dataset IDs with commas (e.g., dataset1, dataset2).")
    print("2. back")
    datasetid = input(": ")

    if datasetid == "2":
        cui()
        return

    if core.checkInputForList(datasetid):
        dataset_list = core.inputToList(datasetid)
        if dataset_list:
            core.agolPublishList(dataset_list, erddapObj, 0)
        else:
            print("\nERROR: No Dataset List. Returning to main menu...")
    else:
        attribute_list = core.parseDas(erddapObj, datasetid)
        if attribute_list:
            core.agolPublish(erddapObj, attribute_list, 0)
    
    print("\nReturning to main menu...")
    cui()
    return

def nrt_creation():
    print("\nNRT Creation")
    print("Select which option you would like")
    print("1. Create NRT item with dataset browser")
    print("2. Find ALL valid NRT datasets in a server and add to AGOL")
    print("3. Back")

    user_choice = input(": ")

    if user_choice == "1":
        erddapObj = core.erddapSelection()
        if not erddapObj:
            cui()
            return
        dataset_list = core.selectDatasetFromList(erddapObj)
        if dataset_list:
            core.agolPublishList(dataset_list, erddapObj, 1)
        print("\nReturning to main menu...")
        cui()
    elif user_choice == "2":
        print("Select the server of the dataset you want to create an AGOL item for.")

        erddapObj = core.erddapSelection()
        if not erddapObj:
            cui()
            return 

        print("Finding valid NRT datasets...")
        NRT_IDs = lm.batchNRTFind(erddapObj)

        print(f"\nFound {len(NRT_IDs)} datasets with data within the last 7 days.")
        print("Show dataset IDs? (y/n)")

        uc = input(": ")
        if uc == "y":
            for datasetid in NRT_IDs:
                print(f"{datasetid}")
            print("\n Proceed with processing? (y/n)")
            uc2 = input(": ")
            if uc2 == "n":
                cui()
            else:
                core.processListInput(NRT_IDs, erddapObj, 1)
        else:
            core.processListInput(NRT_IDs, erddapObj, 1)
    else:
        cui()

def glider_menu():
    print("\nWelcome to the *Special* Glider DAC Menu.")
    erddapObj = core.erddapSelection(GliderServ=True)
    erddapObj.server = "https://gliders.ioos.us/erddap/tabledap/"
    
    dataset_list = core.selectDatasetFromList(erddapObj)
    if dataset_list:
        core.agolPublishList(dataset_list, erddapObj, 0)
    print("\nReturning to main menu...")
    cui()

def exit_program():
    print("\nExiting program...")
    exit()

if __name__ == '__main__':
    cui()