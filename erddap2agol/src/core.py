import sys, os, concurrent.futures, time, math, shlex, argparse
from tabulate import tabulate
from . import erddap_wrangler as ec
from . import agol_wrangler as aw
from . import data_wrangler as dw
from . import update_manager as um
from erddap2agol import run
from src.utils import OverwriteFS
from IPython.display import clear_output
from arcgis.gis import GIS
from typing import Optional, Dict, List, Union
from dataclasses import dataclass, field
from datetime import datetime


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
def erddapSelection(GliderServ = False, nrtAdd = False, protocol: str = None) -> ec.ERDDAPHandler:
    if GliderServ == True:
        erddapObj = ec.ERDDAPHandler().setErddap(15)
        return erddapObj
    else:
        ec.getErddapList()
        ec.showErddapList()
        uc = input("\nSelect an ERDDAP server to use: ")
        if uc:
            try:
                erddapObj = ec.ERDDAPHandler()
                erddapObj = erddapObj.setErddap(int(uc))
                
                # If setErddap returns None (invalid index), exit early
                if erddapObj is None:
                    print("Invalid server selection. Please try again.")
                    return None
                
                print(f"\nSelected server: {erddapObj.server}")
                uc = input("Proceed with server selection? (y/n): ")

                if uc.lower() == "y":
                    print("\nContinuing with selected server...")
                    if nrtAdd is True:
                        erddapObj.is_nrt = True
                        erddapObj.protocol = "tabledap"
                        return erddapObj
                    else:
                        erddapObj.protocol = protocol
                        return erddapObj
                else:
                    print("\nReturning to main menu...")
                    erddapObj.reset()
                    run.cui()
            except ValueError:
                print("Please enter a valid number.")
                return None
        else:
            print("\nInput cannot be none")
            return None
            
        
        
# if you want to change dispLength, do that here.
def selectDatasetFromList(erddapObj, dispLength: int = 50, interactive: bool = True) -> list:
    """
    Search, browse, and select ERDDAP datasets.

    If `interactive` is True, the function drives a CLI prompt.  
    If False, you can work with the returned data structures directly
    (e.g., in a notebook or automated workflow).

    Returns
    -------
    list
        The list of dataset IDs the user added to their “cart”.
    """
    # honour a global user-option override
    if getattr(user_options, "disp_length", None):
        dispLength = user_options.disp_length

    # ------------------------------------------------------------------
    # Helper to keep the server query + cache-restore logic in one place
    # ------------------------------------------------------------------
    def _updateDatasetList(erddapObj, search_term: str | None = None) -> list:
        """
        Populate `erddapObj.dataset_titles / dataset_dates` for the
        current search and return *just* the list of dataset IDs.
        """
        # make sure the master cache is built
        erddapObj.buildDateCache()

        original_info = erddapObj.serverInfo
        root         = original_info.split("/erddap/")[0] + "/erddap"

        # ------- build the correct search URL -------
        if erddapObj.is_nrt is True:
            original_info = erddapObj.serverInfo
            base_url = original_info.split('/erddap/')[0] + '/erddap'
            if search_term:
                search_url = (
                    f"{base_url}/search/advanced.json?"
                    f"searchFor={search_term}"
                    f"&page=1&itemsPerPage=10000000&minTime=now-{erddapObj.moving_window_days}days&maxTime=&protocol={erddapObj.protocol}"
                )
            else:
                search_url = (
                    f"{base_url}/search/advanced.json?"
                    f"page=1&itemsPerPage=10000000&minTime=now-{erddapObj.moving_window_days}days&maxTime=&protocol={erddapObj.protocol}"
                )
                
        else:
            # historic catalogue search
            if search_term:
                search_url = (
                    f"{root}/search/index.json?"
                    f"searchFor={search_term}"
                    f"&page=1&itemsPerPage=100000"
                    f"&protocol={erddapObj.protocol}"
                )
            else:
                # full catalogue – same as erddapObj.getDatasetIDList() default
                search_url = None  # sentinel

        # ------- execute the query -------
        if search_url:
            erddapObj.serverInfo = search_url
            id_list = erddapObj.getDatasetIDList()
            erddapObj.serverInfo = original_info
        else:
            id_list = erddapObj.getDatasetIDList()

        # ------- restore authoritative date ranges from the cache -------
        for ds_id in id_list:
            if ds_id in erddapObj.date_range_cache:
                erddapObj.dataset_dates[ds_id] = erddapObj.date_range_cache[ds_id]

        return id_list

    # ---------------------- DatasetListManager ----------------------
    class DatasetListManager:
        """
        Manages dataset pagination and the user’s selected items.
        """
        def __init__(self, erddapObj, _dispLength):
            self.erddapObj   = erddapObj
            self._dispLength = _dispLength

            self._allDatasetIds = _updateDatasetList(erddapObj)
            total_datasets      = len(self._allDatasetIds)

            if getattr(user_options, "disp_length", None):
                self._dispLength = user_options.disp_length
            if total_datasets < self._dispLength:
                self._dispLength = total_datasets

            self.numPages = 0 if total_datasets == 0 else math.ceil(total_datasets / self._dispLength)
            self.currentPage = 1 if total_datasets else 0
            self.selectedDatasets: list[str] = []
            
            self.protocol = erddapObj.protocol
            self.dataset_kwargs = {}
            self.latest_bool = None
            self.user_single_date = None
            self.user_start_date = None
            self.user_end_date =  None
            self.division = None
            

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
        
        def setDateRange(self, sd, ed):
            """Assigns the date range provided by the user to current_start/end attribute"""
            self.user_start_date = sd
            self.user_end_date = ed

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

                    if self.protocol == "griddap":
                        self.dataset_kwargs[selected_dataset] = {
                            'latest_bool': self.latest_bool,
                            'user_single_date': self.user_single_date,
                            'user_start_date': self.user_start_date,
                            'user_end_date': self.user_end_date,
                            "division": self.division,

                        }
                        # print(f"Added {selected_dataset} to the cart.")

                    print(f"Added {selected_dataset} to the cart.")
                else:
                    print(f"{selected_dataset} is already in the cart.")
            else:
                print(f"Invalid index {idx_int} for this page.")

        def _parseInput(self, user_input: str = None):
            """Returns (selected_list, kwargs) or None, this function is important for the selectDatasetFromList function"""
            cmd = user_input.strip()
            if cmd in command_map:
                done = command_map[cmd](self, None)
                if done:
                    clearScreen()
                    print("\nAdding the following datasets to the next step:")
                    table = [(ds, erddapObj.dataset_titles.get(ds, "")) for ds in self.selectedDatasets]
                    print(tabulate(table, headers=['Dataset ID', 'Dataset Title'], tablefmt='grid'))
                    if self.protocol == 'griddap':
                        return self.selectedDatasets, self.dataset_kwargs
                    return self.selectedDatasets, None
            else:
                self.addByIndices(cmd)
                input("Press Enter to continue...")
            return None

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

    # ---------------------- ------------- ----------------------
    # ---------------------- CUI WhileLoop ----------------------
    # ---------------------- ------------- ----------------------
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
            title = erddapObj.dataset_titles.get(ds, "")
            if erddapObj.protocol == "griddap":
                min_t, max_t = erddapObj.dataset_dates.get(ds, ("", ""))
                if min_t and max_t:
                    title_str = (
                        f"{start_idx + i + 1}. {title} "
                        f"[{min_t[:10]} -> {max_t[:10]}]"
                    )
                else:
                    title_str = (
                        f"{start_idx + i + 1}. {title} "
                    )
            else:
                title_str = f"{start_idx + i + 1}. {title}"
            print(title_str)
            
        if mgr.protocol == "griddap":
            print("\nCommands:")
            print("'next', 'back', 'addAll', 'addPage', 'done', 'mainMenu', 'exit'")
            print("Type 'search:keyword1+keyword2' to search datasets.")
            print("Specify date range with -l (latest) OR -sd dd/mm/yyyy AND -ed dd/mm/yyyy")
            print("Use '-div' and 'day', 'week', or 'month' to specify division (not required)")

            raw_input = input(": ")

            tokens = shlex.split(raw_input)

            parser = argparse.ArgumentParser(add_help=False)
            
            parser.add_argument('-l', '-latest',
                                dest='latest',
                                action='store_true',
                                help="use only the latest date")
            
            parser.add_argument('-date', '--date',
                                dest='user_single_date',
                                type=str,
                                help="single date in dd/mm/yy")
            
            parser.add_argument('-sd', '-start-date',
                                dest='user_start_date',
                                type=str,
                                help="start date in dd/mm/YYYY")
            
            parser.add_argument('-ed', '-end-date',
                                dest='user_end_date',
                                type=str,
                                help="end date in dd/mm/YYYY")
            
            parser.add_argument('-div', '-division', dest='division',
                    choices=('day', 'week', 'month'), type=str.lower,
                    help="split selected date range into day/week/month chunks")

            args, rem = parser.parse_known_args(tokens)

            # There are three cases for adding imagery, single date, latest, multiple images
            
            mgr.division = args.division

            if args.division and not (args.user_start_date and args.user_end_date):
                print("\nThere needs to be a start and end date specified with division")
                continue

            # case 1
            if args.user_single_date:
                user_start_date, user_end_date = None, None
                mgr.latest_bool = False
                try:
                    user_single_date = datetime.strptime(str(args.user_single_date), '%d/%m/%Y')
                except Exception as e:
                    print(f"\nThere was an issue parsing your input into a date time format, please try again.")
                    time.sleep(1)
                    continue
                mgr.user_single_date = user_single_date

            # case 2
            elif args.latest:
                user_start_date, user_end_date, user_single_date = None, None, None
                mgr.latest_bool = True
            
            # case 3
            elif args.user_start_date and args.user_end_date:
                try:
                    user_start_date = datetime.strptime(args.user_start_date, '%d/%m/%Y')
                    user_end_date = datetime.strptime(args.user_end_date, '%d/%m/%Y')
                    mgr.setDateRange(user_start_date, user_end_date)
                except Exception as e:
                    print(f"\nThere was an error while converting your input into datetime objects: {e}")
                    time.sleep(1)
                    continue 
            else:
                pass
            
            # Parse the remaining tokens
            selection_str = " ".join(rem)
            if selection_str.startswith("search:"):
                term = selection_str.split(':', 1)[1]
                mgr.searchDatasets(term)
                continue
            try:
                result = mgr._parseInput(selection_str)
            except Exception as e:
                print(f"\nThere was an error while parsing your input: {e}")
                return None
            if result:
                return result


        else:
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
            
            result = mgr._parseInput(user_input)
            if result:
                return result
            # Check if it matches one of our known commands

            # this is now wrapped in the _parseInput() function
            # if user_input in command_map:
            #     finished = command_map[user_input](mgr, None)
            #     if finished:
            #         clearScreen()
            #         print("\nAdding the following datasets to the next step:")
            #         # Create a list of tuples for the table
            #         #print(mgr.selectedDatasets)
            #         table_data = [(ds_id, erddapObj.dataset_titles.get(ds_id, "No title")) for ds_id in mgr.selectedDatasets]
            #         print(tabulate(table_data, headers=['Dataset ID', 'Dataset Title'], tablefmt='grid'))
            #         return mgr.selectedDatasets
            # else:
            #     # Possibly indices or ranges
            #     mgr.addByIndices(user_input)
            #     # here is where we will put a check for flags
            #     input("Press Enter to continue...")

# programmatic example of accessing a dataset id list 
# mgr = selectDatasetFromList(erddapObj, dispLength=75, interactive=False) 

@dataclass
class OptionsMenu:
    custom_title: bool = False  
    sharing_options: List[str] = field(default_factory=lambda: ["PRIVATE", "ORG", "EVERYONE"])
    sharing_level: str = None
    enable_tags_bool: bool = True
    chunk_size: int = None
    disp_length: int = None
    bypass_chunking_bool: bool = False
    all_attributes_bool: bool = False
    additional_tags: List[str] = None
    bounds: tuple = None
    mult_dim_bool: bool = True
    # share_to_group

    def customTitleMenu(self, dataset): 
        print("Custom Title Option")
        print(f"Type 1 to use the existing title.")
        uc = input(f"\nInput the custom title for {dataset.dataset_title}: ")
        if uc == "1":
            print(f"Using default title...")
            pass
        elif isinstance(uc, str):
            print(f"Using title {uc} for dataset {dataset.dataset_title}")
            dataset.dataset_title = uc
        else:
            print(f"Invalid input for the dataset, continuing with default title")
            pass

    def getBoundsFromItem(id: str):
        gis = GIS("Home")
        try:
            service_item = gis.content.get(id)
            extent = service_item.extent
            
            if extent:
                user_options.bounds = extent
            else:
                print(f"\nThere was an error getting the item extent, no extent set")
                time.sleep(1)
                user_options.bounds = None
        except Exception as e:
            print(f"\nThere was an error getting the content item: {e}")
            time.sleep(1)
        
        
        
# Global variable to hold the options.
user_options = OptionsMenu()


def options_menu():
    def clearScreen():
            os.system('cls' if os.name == 'nt' else 'clear')
            clear_output()
    global user_options  # So changes persist across modules.
    while True:
        clearScreen()
        print("\nOptions Menu:")
        
        print("1. Toggle Custom Title (currently: {})".format(user_options.custom_title))
        
        if user_options.sharing_level:
            print("2. Select Sharing Level (currently: {})".format(user_options.sharing_level))
        else:
            print("2. Select Sharing Level (currently: ORG)")

        print("3. Toggle Enable Tags (currently: {})".format(user_options.enable_tags_bool))
        
        if user_options.chunk_size:
            print("4. Change Download Batch Size (currently: {})".format(user_options.custom_title))
        else:
            print("4. Change Download Batch Size (default 100,000)")
        
        print("5. Change the number of datasets displayed")
        print("6. Toggle Bypass Chunking (currently: {})".format(user_options.bypass_chunking_bool))
        print("7. Get all attributes (currently: {})".format(user_options.all_attributes_bool))
        print("8. Add tags to next batch")
        if user_options.bounds:
            print("9. Define bounds with Content Item ID (griddap only): \n{}".format(user_options.bounds))
        else:
            print("9. Define bounds with Content Item ID (griddap only)")

        print("10. Toggle Multidimensional Imagery Option (currently: {})".format(user_options.mult_dim_bool))
        
        print("\nType **done** to save options and return to main menu")
        
        choice = input("Select an option: ").strip()
        
        if choice == "1":
            # Toggle the boolean flag for custom title.
            user_options.custom_title = not user_options.custom_title
            print("Custom Title toggled to: {}".format(user_options.custom_title))

        elif choice == "2":
            # Print current sharing level options and allow selection.
            print("\nAvailable Sharing Levels:")
            for idx, level in enumerate(user_options.sharing_options, start=1):
                print("{}. {}".format(idx, level))
            sel = input("Select a sharing level by number: ").strip()
            try:
                sel_idx = int(sel)
                if 1 <= sel_idx <= len(user_options.sharing_options):
                    user_options.sharing_level = str(user_options.sharing_options[sel_idx - 1])
                    print("Selected sharing level: {}".format(user_options.sharing_level))
                else:
                    print("Invalid selection. Please choose a valid number.")
            except ValueError:
                print("Invalid input. Please enter a number.")

        elif choice == "3":
            # Toggle the enable_tags_bool option.
            user_options.enable_tags_bool = not user_options.enable_tags_bool
            print("Enable Tags toggled to: {}".format(user_options.enable_tags_bool))
            
        elif choice == "4":
            uc = input(f"Input the desired batch size: ")
            try:
                int(uc)
                user_options.chunk_size = uc
            except Exception as e:
                print(f"Invalid input {e}")
        
        elif choice == '5':
            uc = input(f"Select the number of datasets to display on a single page (1-100): ")
            try:
                disp_int = int(uc)
                if disp_int > 100 or disp_int < 1:
                    print(f"Invalid display length selected ({disp_int}). Please choose a number between 1 and 100") 
                    pass
                else:
                    user_options.disp_length = disp_int
            except Exception as e:
                print(f"\nAn error occured while adjusting display len ({e}) ")
        
        elif choice == "6":
            user_options.bypass_chunking_bool = not user_options.bypass_chunking_bool
            print("Bypass chunking toggled to: {}".format(user_options.bypass_chunking_bool))
        
        elif choice == "7":
            user_options.all_attributes_bool = not user_options.all_attributes_bool
            print("Bypass chunking toggled to: {}".format(user_options.all_attributes_bool))

        elif choice == "8":
            print(f"\nCurrent Additional Tags: {user_options.additional_tags}")            
            print("\n1. Add tags to next batch")
            print("2. Remove additional tags")
            print("3. Return")
            choice_tags = input("Selection: ")
            if choice_tags == "1":
                # init empty list for additional tag attributes
                user_options.additional_tags = []
                user_tags = input("Add additional tag(s) seperated by a space: ")
                # iterate through the str and detect the presence of a space
                mult_tag_bool = False
                for chars in user_tags:
                    if chars == " ":
                        mult_tag_bool = True
                    # else:
                    #     mult_tag_bool = False               
                if mult_tag_bool:
                    tag_list = user_tags.split(" ")
                    for tag in tag_list:
                        user_options.additional_tags.append(tag)

                else:
                    user_options.additional_tags.append(user_tags)

            if choice_tags == "2":
                user_options.additional_tags = None

            if choice_tags == "3":
                return None

        elif choice == "9":
            item_id = input("Enter the Item ID to query: ").strip()
            # layer_in = input("Layer index (ENTER for 0): ").strip()
            # try:
            #     layer_idx = int(layer_in) if layer_in else 0
            # except ValueError:
            #     print("Index must be an integer.")
            #     time.sleep(1)
            #     continue

            OptionsMenu.getBoundsFromItem(item_id)
            
        elif choice == "10":
            user_options.mult_dim_bool = not user_options.mult_dim_bool
            print("Bypass chunking toggled to: {}".format(user_options.bypass_chunking_bool))

        elif choice == "done":
            print("\nOptions saved. Returning to Main Menu...")
            time.sleep(0.5)
            clearScreen()
            break
        else:
            print("Invalid option. Please select again.")
    

    




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
            datasetObj = dw.DatasetWrangler(dataset_id=datasetid, dataset_title=None, server=serverurl, is_nrt=True)
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

