import sys, os, concurrent.futures, time, math
from . import erddap_wrangler as ec
from . import agol_wrangler as aw
from . import data_wrangler as dw
from . import update_manager as um
from erddap2agol import run
from src.utils import OverwriteFS
from IPython.display import clear_output
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
        erddapObj = ec.ERDDAPHandler.setErddap(15)
        return erddapObj
    else:
        ec.getErddapList()
        ec.showErddapList()
        uc = input("\nSelect an ERDDAP server to use: ")
        if uc:
            erddapObj = ec.ERDDAPHandler()
            erddapObj =  erddapObj.setErddap(int(uc))
           
            # erddapObj = ec.ERDDAPHandler.setErddap(int(uc))
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
                run.cui()
        else:
            print("\nInput cannot be none")
            return None
        
# if you want to change dispLength, do that here.
def selectDatasetFromList(erddapObj, dispLength=50, interactive=True) -> list:
    """
    The big search function that allows users to search datasets and select them for processing.
    Encapuslates the DatalistManager class.
    
    If 'interactive' is True, this prompts for user input (CLI).
    If 'interactive' is False, you can manage the selection programmatically by
    working directly with the returned data structures or adding your own logic.
    
    Returns a list of selected datasets (the user's "cart").
    """

    # ---------------------- Helper Functions ----------------------
    def _updateDatasetList(erddapObj, search_term=None):
        """
        For a given ERDDAPHandler, fetch and/or filter the dataset list based on search_term.
        """
        original_info = erddapObj.serverInfo
        base_url = original_info.split('/erddap/')[0] + '/erddap'
        
        # If the object is flagged as NRT, we handle the 7-day search
        #-------- Modify for different moving window size
        if erddapObj.is_nrt is True:
            if search_term:
                search_url = (
                    f"{base_url}/search/advanced.json?"
                    f"searchFor={search_term}"
                    f"&page=1&itemsPerPage=10000000&minTime=now-{erddapObj.moving_window_days}days&maxTime=&protocol=tabledap"
                )
            else:
                search_url = (
                    f"{base_url}/search/advanced.json?"
                    f"page=1&itemsPerPage=10000000&minTime=now-{erddapObj.moving_window_days}days&maxTime=&protocol=tabledap"
                )
            erddapObj.serverInfo = search_url
            dataset_id_list = erddapObj.getDatasetIDList()
            erddapObj.serverInfo = original_info
            return dataset_id_list

        # non-NRT
        if search_term:
            base_url = original_info.split('/erddap/')[0] + '/erddap'
            search_url = (
                f"{base_url}/search/index.json?"
                f"searchFor={search_term}"
                f"&page=1&itemsPerPage=100000&protocol=tabledap"
            )
            erddapObj.serverInfo = search_url
            dataset_id_list = erddapObj.getDatasetIDList()
            erddapObj.serverInfo = original_info
            return dataset_id_list

        # Default: no search term, return the entire dataset list
        return erddapObj.getDatasetIDList()

    # ---------------------- DatasetListManager ----------------------
    class DatasetListManager:
        """
        Manages the dataset list, pagination, and selected items (the "cart").
        """
        def __init__(self, erddapObj, _dispLength):
            self.erddapObj = erddapObj
            self._dispLength = _dispLength
            
            self._allDatasetIds = _updateDatasetList(erddapObj)
            total_datasets = len(self._allDatasetIds)

            if total_datasets < self._dispLength:
                self._dispLength = total_datasets

            if total_datasets == 0:
                # if no datasets returned
                self.numPages = 0
                self.currentPage = 0
                print("No datasets available for this server or search term.")
            else:
                # pagination
                self.numPages = math.ceil(total_datasets / self._dispLength)
                self.currentPage = 1

             # list of selectedDatasets (the cart)
            self.selectedDatasets = []

        @property
        def totalDatasets(self):
            return len(self._allDatasetIds)

        def currentPageDatasets(self):
            """
            Return the dataset IDs for the current page.
            """
            start_index = (self.currentPage - 1) * self._dispLength
            end_index = min(start_index + self._dispLength, self.totalDatasets)
            return self._allDatasetIds[start_index:end_index]

        def goNextPage(self):
            if self.currentPage < self.numPages:
                self.currentPage += 1
            else:
                print("No more pages.")

        def goBackPage(self):
            if self.currentPage > 1:
                self.currentPage -= 1
            else:
                print("Already at the first page.")

        def searchDatasets(self, search_term):
            """
            Update the dataset list with a new search term and reset the page to 1.
            """
            new_list = _updateDatasetList(self.erddapObj, search_term)
            if not new_list:
                print(f"No datasets found matching '{search_term}'.")
            self._allDatasetIds = new_list
            self.currentPage = 1
            self.numPages = math.ceil(len(self._allDatasetIds) / self._dispLength)

        def addPage(self):
            """
            Add all datasets on the current page to the cart.
            """
            count_added = 0
            page_ds = self.currentPageDatasets()
            for ds in page_ds:
                if ds not in self.selectedDatasets:
                    self.selectedDatasets.append(ds)
                    count_added += 1
            print(f"Added {count_added} datasets from page {self.currentPage} to the cart.")

        def addAll(self):
            """
            Add all datasets in the entire list to the cart.
            """
            count = 0
            for ds in self._allDatasetIds:
                if ds not in self.selectedDatasets:
                    self.selectedDatasets.append(ds)
                    count += 1
            print(f"Added {count} datasets to the cart. (Cart total: {len(self.selectedDatasets)})")

        def addByIndices(self, indices_str):
            """
            Parses user input to allow:
              - comma-separated single indices (e.g. "10,15")
              - comma-separated ranges (e.g. "10:15, 20:25")
              - or a mix ("10,12:14")
            Index references are for the current page only.
            """
            start_index = (self.currentPage - 1) * self._dispLength
            current_page_ds = self.currentPageDatasets()


            tokens = [token.strip() for token in indices_str.split(',')]
            for token in tokens:
                #potentially change range to colon 
                if ':' in token:
                    # It's a range
                    try:
                        left, right = token.split(':', 1)
                        left_int = int(left)
                        right_int = int(right)

                        # Make sure left_int < right_int in a typical sense,
                        low = min(left_int, right_int)
                        high = max(left_int, right_int)

                        for idx_int in range(low, high + 1):
                            self._addSingleIndex(idx_int, start_index, current_page_ds)
                    except ValueError:
                        print(f"Invalid range input: '{token}'. Please use something like '10:15'.")
                else:
                    # It's a single index
                    if token.isdigit():
                        idx_int = int(token)
                        self._addSingleIndex(idx_int, start_index, current_page_ds)
                    else:
                        print(f"Invalid input '{token}'. Please enter valid numbers, ranges, or commands.")

        def _addSingleIndex(self, idx_int, start_index, current_page_ds):
            """
            Internal helper to add a single index to self.selectedDatasets if valid.
            """
            # The user sees an offset-based index, but we must map that to the dataset list
            # for the current page. The valid range is [start_index+1, start_index+len(current_page_ds)].
            if start_index < idx_int <= start_index + len(current_page_ds):
                selected_dataset = self._allDatasetIds[idx_int - 1]
                if selected_dataset not in self.selectedDatasets:
                    self.selectedDatasets.append(selected_dataset)
                    print(f"Added {selected_dataset} to the cart.")
                else:
                    print(f"{selected_dataset} is already in the cart.")
            else:
                print(f"Invalid index {idx_int} for this page.")

    # ---------------------- CUI Cmd Map ----------------------
    def cmdNext(manager: DatasetListManager, _arg):
        manager.goNextPage()

    def cmdBack(manager: DatasetListManager, _arg):
        manager.goBackPage()

    def cmdAddAll(manager: DatasetListManager, _arg):
        manager.addAll()

    def cmdAddPage(manager: DatasetListManager, _arg):
        manager.addPage()

    def cmdDone(manager: DatasetListManager, _arg):
        # Return True to indicate "finished"
        return True
    
    def cmdMainMenu(manager: DatasetListManager, _arg):
        print("Returning to Main Menu...")
        erddapObj.reset()
        run.cui()

    def cmdExit(manager: DatasetListManager, _arg):
        print("Exiting selection.")
        sys.exit()

    command_map = {
        "next": cmdNext,
        "back": cmdBack,
        "addAll": cmdAddAll,
        "addPage": cmdAddPage,
        "done": cmdDone,
        "mainMenu":cmdMainMenu,
        "exit": cmdExit
    }

    # Create our manager with the initial dataset IDs
    mgr = DatasetListManager(erddapObj, _dispLength=dispLength)

    # If not interactive, just return the manager's selection or
    # potentially return the manager object so we can manipulate it programmatically.
    if not interactive:
        # Return an empty cart or the manager itself—depends on your desired usage.
        # For now, we return the manager so a programmatic caller can do:
        # mgr.searchDatasets("keyword")
        # mgr.addAll()
        # final_selection = mgr.selectedDatasets
        return mgr


    # ---------------------- CUI WhileLoop ----------------------


    while True:
        def clearScreen():
            os.system('cls' if os.name == 'nt' else 'clear')
            clear_output()

        clearScreen()

        print(f"\nPage {mgr.currentPage} of {mgr.numPages} | Cart: {len(mgr.selectedDatasets)} datasets")
        #Get current page
        current_ds = mgr.currentPageDatasets()
        #starting index
        start_idx = (mgr.currentPage - 1) * mgr._dispLength
        #enumerate through current ds, idx used for selection
        for i, ds in enumerate(current_ds):
            #ref datasetTitles dict
            titles = erddapObj.datasetTitles.get(ds,"")
            title_str = f"{start_idx + i + 1}. {titles}"
            # id_str = f"ID: {ds}"
            print(title_str) 
            # print(f"{start_idx + i + 1}. {titles}\tID: {ds}")
            

        print("\nCommands:")
        print("'next', 'back', 'addAll', 'addPage', 'done', 'mainMenu', 'exit'")
        print(" type 'search:keyword1+keyword2' to search datasets.")
        print(" enter comma-separated indices (e.g. '10,12:15') for single or range selection.")
        user_input = input(": ")

        # Check search syntax
        if user_input.startswith('search:'):
            term = user_input.split(':', 1)[1]
            mgr.searchDatasets(term)
            continue

        # Check if it matches one of our known commands
        if user_input in command_map:
            finished = command_map[user_input](mgr, None)
            if finished:
                clearScreen()
                print("\nAdding the following datasets to the next step:")
                print(mgr.selectedDatasets)
                return mgr.selectedDatasets
        else:
            # Possibly indices or ranges
            mgr.addByIndices(user_input)
            # here is where we will put a check for flags
            input("Press Enter to continue...")

# programmatic example of accessing a dataset id list 
# mgr = selectDatasetFromList(erddapObj, dispLength=75, interactive=False) 




#---------------------------------------------------------------------------------------------
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

def findExistingNRT(manager_obj: um.UpdateManager, dataset_list: list) -> list:
    """
    Compare dataset list against existing NRT datasets in AGOL and return non-duplicates.
    """
    existing_datasets = set(manager_obj.datasets.keys())
    
    new_datasets = set(dataset_list)
    
    #duplicates = new_datasets.intersection(existing_datasets)
                
    new_datasets = list(new_datasets - existing_datasets)

    if new_datasets:
        # print(f"\nFound {len(new_datasets)} new NRT datasets to add:")
        # for dataset_id in duplicates:
        #     print(f"- {dataset_id}")
        return new_datasets
    else:
        return None


###################################
##### Functions for Notebooks #####
###################################
def ofsWorkerFunc(agol_id, url, verbose, preserveProps, ignoreAge, noProps):
        """
        Worker function that runs OFS a separate process.
        """
        # get item content
        start = time.time()
        gis = GIS("Home")
        item_content = gis.content.get(agol_id)
        OverwriteFS.overwriteFeatureService(
            item_content,      
            url,
            verbose=verbose,
            preserveProps=preserveProps,
            ignoreAge=ignoreAge,
            noProps=noProps
        )
        end = time.time()
        return end - start

def updateNRT(
    verbose_opt: bool = True,
    preserveProps_opt: bool = True,
    ignoreAge_opt: bool = True,
    noProps_opt: bool = False,
    timeoutTime: int = 300,
    max_workers: int = 4 ) -> None:
    """
    Searches your ArcGIS Online account for datasets with the NRT tags, then
    overwrites them in parallel using a single ProcessPoolExecutor.
    """
    update_manager = um.UpdateManager()
    gis = update_manager.gis
    update_manager.searchContent()

    # Grab all datasets at once
    items = list(update_manager.datasets.items())  #  k d_id: v info

    futures = {}
    start_times = {}
    start_all = time.time()


    
    # 1. Create 1 ProcessPoolExecutor outside the loop
    #------------------------------------------------------
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:

        
        # 2. Submit each dataset as a separate task
        #-------------------------------------------
        for datasetid, info in items:
            serverurl = info.get('base_url')
            datasetObj = dw.DatasetWrangler(dataset_id=datasetid, datasetTitle=None, server=serverurl, is_nrt=True)
            datasetObj.generateUrl(nrt_update=True)  # sets datasetObj.url_s

            agol_id = info.get('agol_id')

            s_time = time.time()
            future = executor.submit(
                ofsWorkerFunc,
                agol_id,
                datasetObj.url_s[0],
                verbose_opt,
                preserveProps_opt,
                ignoreAge_opt,
                noProps_opt
            )
            futures[future] = datasetid
            start_times[future] = s_time
        
        # 3. Collect results as they complete or fail
        #--------------------------------------------
        for future in concurrent.futures.as_completed(futures):
            datasetid = futures[future]
            dataset_start = start_times[future]
            dataset_end = time.time()
            dataset_time = dataset_end - dataset_start
            try:
                duration = future.result(timeout=timeoutTime)
                print(f"Dataset {datasetid} completed in {duration:.2f} seconds (worker time).")


            except concurrent.futures.TimeoutError:
                print(f"Timed out overwriting {datasetid} after {timeoutTime} seconds.")
                # Re-append dataset to the end if needed
                popped_info = update_manager.datasets.pop(datasetid, None)
                if popped_info:
                    update_manager.datasets[datasetid] = popped_info

            except Exception as ex:
                print(f"Error overwriting {datasetid}: {ex}")

    total_time = time.time() - start_all
    print(f"All tasks completed in {total_time:.2f} seconds.")


def gliderWorkflow(search_term: str = None) -> None:
    """
    Automates the workflow for glider data:

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
        else:
            print(f"No datasets found matching search term '{search_term}'")
    else:
        print("No search term provided")


# def updateNRT(timeoutTime = 300) -> None:
#     """Searches your ArcGIS Online account for datasets with the NRT tags, then runs the 
#         typical NRT post, but passes a URL providing OFS with the destination data"""
#     update_manager = um.UpdateManager()
#     gis = update_manager.gis
#     update_manager.searchContent()
#     dataset_list = []

#     for datasetid, info in update_manager.datasets.items():
#         serverurl = info.get('base_url')
#         # print(serverurl)
#         datasetObj = dw.DatasetWrangler(
#             dataset_id= datasetid,
#             server= serverurl,
#             is_nrt= True
#         )
#         datasetObj.generateUrl()
#         agol_id = info.get('agol_id')

#         content_item = gis.content.get(agol_id) 
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
#             future = executor.submit(OverwriteFS.overwriteFeatureService,
#                                      content_item, 
#                                      datasetObj.url_s[0], 
#                                      verbose=True, 
#                                      preserveProps=True, 
#                                      ignoreAge = True
#                                      )
#         try:
#             future.result(timeout=timeoutTime)

#         except concurrent.futures.TimeoutError:
#             print(f"Timed out overwriting {datasetid} after 300 seconds.")
#             # pop from queue and put at end
#             pop = update_manager.datasets.pop(datasetid, None)
#             if pop:
#                 update_manager.datasets[datasetid] = pop
#             continue

#         except Exception as e:
#             raise e

# Survives refactor
# def selectDatasetFromList(erddapObj, dispLength=75) -> list:
#     """The big search function that allows users to search datasets and select them for processing.
        
#         Returns a list of selected datasets. Should pass to the erddapObj list constructor."""
#     def _updateDatasetList(erddapObj, search_term=None):
#         original_info = erddapObj.serverInfo
#         base_url = original_info.split('/erddap/')[0] + '/erddap'
      
#         if erddapObj.is_nrt is True:
#             if search_term:
#                 search_url = (
#                     f"{base_url}/search/advanced.json?"
#                     f"searchFor={search_term}"
#                     f"&page=1&itemsPerPage=10000000&minTime=now-7days&maxTime=&protocol=tabledap" #hardcoded to 7 days
#                 )
#             else:
#                 search_url = (
#                     f"{base_url}/search/advanced.json?"                
#                     f"page=1&itemsPerPage=10000000&minTime=now-7days&maxTime=&protocol=tabledap"
#                 )
            
#             # print(f"\nSEARCH URL: {search_url}")
#             erddapObj.serverInfo = search_url
#             dataset_id_list = erddapObj.getDatasetIDList()
#             erddapObj.serverInfo = original_info
#             return dataset_id_list

#         if search_term:
#             original_info = erddapObj.serverInfo
#             # Extract the base URL (remove everything after /erddap/)
#             base_url = original_info.split('/erddap/')[0] + '/erddap'
#             # Construct the proper search URL
#             search_url = f"{base_url}/search/index.json?searchFor={search_term}&page=1&itemsPerPage=100000&protocol=tabledap"
#             erddapObj.serverInfo = search_url
            
#             #print(f"Searching using URL: {search_url}")  # Debug print
#             dataset_id_list = erddapObj.getDatasetIDList()
#             erddapObj.serverInfo = original_info
#             return dataset_id_list
        
#         return erddapObj.getDatasetIDList()

#     dataset_id_list = _updateDatasetList(erddapObj)
    
#     if len(dataset_id_list) < dispLength:
#         dispLength = len(dataset_id_list)
    
#     print(f"\nDatasets are shown {dispLength} at a time.")
#     print("Enter the number of the datasets you want and press enter.")
#     print("To search datasets, type 'search:keyword'")
#     print("To move forward one page type 'next', to move backwards type 'back'.")
    
#     import math
#     if len(dataset_id_list) == 0:
#         print("No datasets found.")
#         exit()
#     num_pages = math.ceil(len(dataset_id_list) / dispLength)
#     current_page = 1
#     input_list = []
    
#     def clearScreen():
#         import os
#         os.system('cls' if os.name == 'nt' else 'clear')
#         clear_output()
    
#     while True:
#         clearScreen()
#         start_index = (current_page - 1) * dispLength
#         end_index = min(start_index + dispLength, len(dataset_id_list))
#         current_page_datasets = dataset_id_list[start_index:end_index]
        
#         print(f"\nPage {current_page} of {num_pages}")
#         print(f"Cart: {len(input_list)} datasets")

#         for index, dataset in enumerate(current_page_datasets):
#             # Split the dataset info to get just the ID for the cart
#             dataset_id = dataset.split(' - ')[0] if ' - ' in dataset else dataset
#             print(f"{start_index + index + 1}. {dataset}")

#         print("\nEnter the number(s) of the dataset(s) you want to select")
#         print("Commands: 'next', 'back', 'addAll', 'addPage', 'done', 'exit', or 'search:keyword1+keyword2'")
#         idx_select = input(": ")
        
#         if idx_select.startswith('search:'):
#             search_term = idx_select.split(':', 1)[1]

#             print(f"\nSearching for: {search_term}")

#             dataset_id_list = _updateDatasetList(erddapObj, search_term)

#             if not dataset_id_list:
#                 print("No datasets found matching your search.")
#                 input("Press Enter to continue...")
#             num_pages = math.ceil(len(dataset_id_list) / dispLength)
#             current_page = 1
#             continue

#         if idx_select == "next":
#             if current_page < num_pages:
#                 current_page += 1
#             else:
#                 print("No more pages.")
#                 input("Press Enter to continue...")

#         elif idx_select == "back":
#             if current_page > 1:
#                 current_page -= 1
#             else:
#                 print("Already at the first page.")
#                 input("Press Enter to continue...")
#         elif idx_select == "exit":
#             run.cui()

#         elif idx_select == "done":
#             clearScreen()
#             print("\nPassing the following datasets to the next step...")
#             print(f"{input_list}")
#             return input_list
        
#         elif idx_select == "addAll":
#             for dataset in dataset_id_list:  # Changed from current_page_datasets to dataset_id_list
#                 if dataset not in input_list:
#                     input_list.append(dataset)
#             print(f"Added all {len(dataset_id_list)} datasets to the list.")
#             input("Press Enter to continue...")

#         elif idx_select == "addPage":
#             for dataset in current_page_datasets:
#                 if dataset not in input_list:
#                     input_list.append(dataset)
#             print(f"Added all datasets on page {current_page} to the list.")
#             input("Press Enter to continue...")
#         # If user input is int, theyre adding datasets
#         else:
#             try:
#                 indices = [i.strip() for i in idx_select.split(',')]
#                 valid_selection = False
#                 for idx in indices:
#                     if idx.isdigit():
#                         idx_int = int(idx)
#                         if start_index < idx_int <= start_index + len(current_page_datasets):
#                             selected_dataset = dataset_id_list[idx_int - 1]
#                             if selected_dataset not in input_list:
#                                 input_list.append(selected_dataset)
#                                 print(f"Added {selected_dataset} to the list.")
#                             else:
#                                 print(f"{selected_dataset} is already in the list.")
#                             valid_selection = True
#                         else:
#                             print(f"Invalid input. Number {idx_int} out of range for this page.")
#                     else:
#                         print(f"Invalid input '{idx}'. Please enter valid numbers or commands.")
#                 if not valid_selection:
#                     input("Press Enter to continue...")
#             except Exception as e:
#                 print("An unexpected error occurred:", e)
#                 input("Press Enter to continue...")
     
   