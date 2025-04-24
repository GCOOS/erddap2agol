from arcgis.gis import GIS, ItemProperties
from arcgis.features import FeatureLayer, FeatureLayerCollection
from arcgis.gis._impl._content_manager import SharingLevel
from . import data_wrangler as dw
from . import erddap_wrangler as ec
from . import das_client as dc
from . import core 
import os, sys, time, pandas as pd, numpy as np, json
from dataclasses import dataclass, field
from typing import Optional, List, Dict
#from line_profiler import profile
from collections import deque
import concurrent.futures
# sharing levels
# PRIVATE
# ORGANIZATION
# EVERYONE

@dataclass
class AgolWrangler:
    gis: Optional[GIS] = None
    sharing_pref: str = "EVERYONE"
    datasets: List['dw.DatasetWrangler'] = field(default_factory=list)
    item_properties: Dict[str, Dict] = field(default_factory=dict)
    erddap_obj: Optional['ec.ERDDAPHandler'] = None
    enterprise_bool: Optional[bool] = None
    geoParams = None 
    geoParams_online: dict = field(default_factory=lambda: {
        "locationType": "coordinates",
        "latitudeFieldName": "latitude__degrees_north_",
        "longitudeFieldName": "longitude__degrees_east_",
        "timeFieldName": "time__UTC_",
    })
    geoParams_enterprise: dict = field(default_factory=lambda: {
        "locationType": "coordinates",
        "latitudeFieldName": "latitude (degrees_north)",
        "longitudeFieldName": "longitude (degrees_east)",
        "timeFieldName": "time__UTC_",
    })

    def __post_init__(self):
        """Initialize ArcGIS connection and inherit dataset attributes"""
        self.connect()
        if self.erddap_obj and hasattr(self.erddap_obj, 'datasets'):
            self.shareDatasetObjAttrs

    def __iter__(self):
        """Make ERDDAPHandler directly iterable"""
        return iter(self.datasets)
    
    def __len__(self):
        """Get number of datasets"""
        return len(self.datasets)
    
    def __getitem__(self, index):
        """Allow index access to datasets"""
        return self.datasets[index]
    
    def connect(self) -> None:
        """Establish AGOL connection"""
        try:
            self.gis = GIS("home")
            gis = self.gis
            if gis.properties.portalName == "ArcGIS Online":
                self.geoParams = self.geoParams_online
            elif gis.properties.portalName == "ArcGIS Enterprise":
                # self.geoParams = self.geoParams_online
                self.geoParams = self.geoParams_enterprise
                self.enterprise_bool = True
            else:
                self.geoParams = self.geoParams_online
            print("\nSuccesfully connected to " + gis.properties.portalName)
        except Exception as e:
            print(f"{gis.properties.portalName} connection error: {e}")

    def skipFromError(func):
        """General skip error decorator that will be applied to all dataset methods"""
        def wrapper(self, *args, **kwargs):
            if dw.DatasetWrangler.has_error == True:
                print(f"\nSkipping {func.__name__} - due to processing error {self.dataset_id}")
                return None
            return func(self, *args, **kwargs)
        return wrapper

    def shareDatasetObjAttrs(self) -> None:
        """Add datasets from the provided ERDDAPHandler instance."""
        erddapObj = self.erddap_obj
        if hasattr(erddapObj, 'datasets'):
            self.datasets.extend(erddapObj.datasets)
            print(f"Added {len(erddapObj.datasets)} datasets from erddap_obj to AgolWrangler.")
        else:
            print("The provided erddap_obj does not have a 'datasets' attribute.")

    def _flattenTags(self, attrs):
        flat = []
        for attr in attrs:
            if isinstance(attr, list):
                # Recursively flatten if needed.
                flat.extend(self._flattenTags(attr))
            else:
                flat.append(attr)
        return flat
                           


    @skipFromError
    def makeItemProperties(self) -> None:
        """Creates item properties using dataset attributes"""
        
        def _createDescription(dataset, props):
            """
            Incorporate nc_global metadata into AGOL item description if available.
            """
            existing_desc = props.get("description", "")
            desc_parts = []

            for key, label in [
                ("project", "Project"),
                ("comment", "Comment"),
                ("publisher_name", "Publisher"),
                ("publisher_email", "Publisher Email"),
            ]:
                entry = dataset.nc_global.get(key, {}).get("value", "")
                if entry:
                    desc_parts.append(f"{label}: {entry}")

            merged_description = (
                existing_desc.strip() + "\n\n" + "\n".join(desc_parts)
                if existing_desc
                else "\n".join(desc_parts)
            )
            props["description"] = merged_description.strip()

        if self.datasets:
            for dataset in self.datasets:
                try:
                    props = {
                        "type": "CSV",
                        "item_type": "Feature Service",
                        "tags": ["erddap2agol", f"did_{dataset.dataset_id}"]
                    }
                    
                    if dataset.attribute_list:
                        props["tags"].extend(dataset.attribute_list)
                    
                    if dataset.is_nrt is True:
                        props["tags"].append("e2a_nrt")

                    if dataset.server:
                        props["tags"].append(str(dataset.server))

                    if dataset.nc_global:
                        if "publisher_institution" in dataset.nc_global:
                            props["accessInformation"] = dataset.nc_global["publisher_institution"].get("value", "")
                        elif "creator_institution" in dataset.nc_global:
                            props["accessInformation"] = dataset.nc_global["creator_institution"].get("value", "")
                        elif "institution" in dataset.nc_global:
                            props["accessInformation"] = dataset.nc_global["institution"].get("value", "")

                        if "license" in dataset.nc_global:
                            props["licenseInfo"] = dataset.nc_global["license"].get("value", "")
                        
                        # swapped assignment of dataset_title from dataset id attribute to dataset title attribute
                        if core.user_options.custom_title == True:
                            core.user_options.customTitleMenu(dataset)
                        dataset_title = dataset.dataset_title
                        props["title"] = dataset_title

                        server_name = dataset.server.split("/erddap/")[0].split("://")[-1]

                        summary = dataset.nc_global.get("summary", {}).get("value", "")
                        
                        props["snippet"] = f"{summary}. {dataset_title} was generated with erddap2agol from the {server_name} ERDDAP."

                        _createDescription(dataset, props)

                    if dataset.is_glider:
                        props["tags"].append("Glider DAC")
                        props["type"] = "GeoJson"

                    self.item_properties[dataset.dataset_id] = props

                except Exception as e:
                    print(f"Error creating item properties for {dataset.dataset_title}: {e}")

    @skipFromError
    #@profile
    def pointTableToGeojsonLine(self,  X="longitude (degrees_east)", Y="latitude (degrees_north)") -> None:
        """For converting standard ERDDAP csvp into geojson"""
        for dataset in self.datasets:
            if dataset.is_glider == True:
                filepath = dataset.data_filepath
                if dataset.data_filepath:
                    print(f"\nConverting {filepath} to GeoJSON...")
                    df = pd.read_csv(filepath, low_memory=False)

                    # Replace NaN with None in the entire DataFrame
                    df = df.replace({np.nan: None})

                    # Filter out rows with invalid coordinates
                    df = df.dropna(subset=[X, Y])

                    features = []
                    data_columns = [col for col in df.columns if col not in [X, Y]]
                    num_points = len(df)

                    for i in range(num_points - 1):
                        # Coordinates for the line segment
                        line_start = [df.iloc[i][X], df.iloc[i][Y]]
                        line_end = [df.iloc[i + 1][X], df.iloc[i + 1][Y]]
                        coordinates = [line_start, line_end]

                        # Skip if any coordinate is None
                        if None in line_start or None in line_end:
                            continue

                        # Properties from the last point of the segment
                        properties = df.iloc[i + 1][data_columns].to_dict()

                        # Create the GeoJSON feature for the line segment
                        feature = {
                            "type": "Feature",
                            "geometry": {
                                "type": "LineString",
                                "coordinates": coordinates
                            },
                            "properties": properties
                        }

                        features.append(feature)

                    # Assemble the FeatureCollection
                    geojson = {
                        "type": "FeatureCollection",
                        "features": features
                    }
                    savedir = ec.getTempDir()
                    filename = dataset.dataset_id + "_line.geojson"
                    savepath = os.path.join(savedir, filename)
                    with open(savepath, "w") as f:
                        json.dump(geojson, f)
                    print(f"\nGeoJSON conversion complete @ {savepath}.")
                    setattr(dataset, "data_filepath", savepath)
                else:
                    sys.exit()

    def mapItemProperties(self, dataset_id) -> ItemProperties:
        """Map metadata to an item properties attribute of the item class"""
        props = self.item_properties.get(dataset_id, {})
        # check if user disabled tags an adjust accordingly
        if core.user_options.enable_tags_bool == False:
            props["tags"] = []
            return ItemProperties(
                title=props.get("title", ""),
                item_type=props.get("type", ""),
                snippet=props.get("snippet", ""),
                description=props.get("description", ""),
                tags=props.get("tags", []),
                access_information=props.get("accessInformation", ""),
                license_info=props.get("licenseInfo", "")
            )
        else:
            return ItemProperties(
                title=props.get("title", ""),
                item_type=props.get("type", ""),
                snippet=props.get("snippet", ""),
                description=props.get("description", ""),
                tags=props.get("tags", []),
                access_information=props.get("accessInformation", ""),
                license_info=props.get("licenseInfo", "")
            )

    @skipFromError
    def postAndPublish(self, inputDataType="csv", timeoutTime=300) -> None:
        """Publishes all datasets in self.datasets to ArcGIS, handling subsets if needed."""
        geom_params = self.geoParams.copy()
        geom_params.pop('hasStaticData', None)  # Remove if exists, as done in stable code

        # Time tracking variables
        total_start_time = time.time()
        processed_count = 0

        # Helper function to try renaming a file with retries
        def _tryRename(old_path, new_path, max_attempts=5, delay=1):
            for attempt in range(max_attempts):
                try:
                    os.rename(old_path, new_path)
                    print(f"Renamed file from {old_path} to {new_path}")
                    return new_path
                except Exception as ex:
                    print(f"Rename attempt {attempt+1} failed: {ex}")
                    time.sleep(delay)
            raise Exception(f"Failed to rename file {old_path} after {max_attempts} attempts.")

        # Start iterating through datasets 
        for dataset in self.datasets:
            # Dictionary to store renamed file paths for this dataset.
            renamed_files = {}

            if dataset.is_glider is True:
                inputDataType = "GeoJson"

            dataset_start_time = time.time()  # Track start time for this dataset

            item_prop = self.item_properties.get(dataset.dataset_id)
            if not item_prop:
                print(f"No item properties found for {dataset.dataset_title}. Skipping.")
                continue

            paths = dataset.data_filepath
            if not paths:
                print(f"No data file path found for {dataset.dataset_title}. Skipping.")
                continue

            # Set a service name if not already present
            if 'name' not in geom_params or not geom_params['name']:
                geom_params['name'] = item_prop['title']

            gis = self.gis
            # Get the user root folder
            user_root = gis.content.folders.get()

            # ----------------- Helper Functions -----------------
            def addOrRetry(dataset, file, max_attempts=10):
                """
                Attempt to add an item using the provided file and item properties.
                If a conflict error (409) is encountered (i.e., filename exists),
                modify the title by appending _1, _2, etc. and rename the file in place.
                The new name is stored so subsequent calls use it.
                """
                # If this file has been renamed previously, use the new name.
                if file in renamed_files:
                    file = renamed_files[file]
                original_file = file
                props = self.item_properties.get(dataset.dataset_id).copy()
                base_title = props.get("title", "")
                attempt = 0
                while attempt < max_attempts:
                    try:
                        print(f"Attempt {attempt+1}: Trying to add item with title: {props.get('title')} and file: {os.path.basename(file)}")
                        item_future = user_root.add(item_properties=self.mapItemProperties(dataset_id=dataset.dataset_id), file=file)
                        item = item_future.result()
                        return item
                    except Exception as e:
                        error_str = str(e)
                        if "409" in error_str and "already exists" in error_str:
                            attempt += 1
                            new_title = base_title + f"_{attempt}"
                            print(f"Filename conflict encountered. Changing title to {new_title} and renaming file, then retrying...")
                            props["title"] = new_title
                            dirname, basename = os.path.split(original_file)
                            name, ext = os.path.splitext(basename)
                            new_basename = name + f"_{attempt}" + ext
                            new_file = os.path.join(dirname, new_basename)
                            # Try renaming the file, waiting if it's locked.
                            try:
                                _tryRename(file, new_file)
                            except Exception as rename_error:
                                raise Exception(f"Unable to rename file: {rename_error}")
                            file = new_file
                            renamed_files[original_file] = new_file
                        else:
                            raise e
                raise Exception("Max attempts reached for adding item with retry.")
            
            def publishOrRetry(item, publish_parameters, file_type, timeout=300, max_attempts=10):
                """
                Attempt to publish an item with the provided publish parameters and file type.
                If a conflict error (409) is encountered (i.e., an item with this title already exists),
                update the item's title by appending _1, _2, etc. and retry publishing.
                """
                attempt = 0
                base_title = item.title
                while attempt < max_attempts:
                    try:
                        #print(f"Attempt {attempt+1}: Publishing item with title: {item.title}")
                        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                            future = executor.submit(item.publish, publish_parameters=publish_parameters, file_type=file_type)
                            published_item = future.result(timeout=timeout)
                        return published_item
                    except Exception as e:
                        error_str = str(e)
                        if "409" in error_str and "already exists" in error_str:
                            attempt += 1
                            new_title = base_title + f"_{attempt}"
                            print(f"Publish conflict encountered. Changing title to {new_title} and retrying...")
                            item.update(item_properties={"title": new_title})
                            time.sleep(1)
                        else:
                            raise e
                raise Exception("Max attempts reached for publishing item with retry.")
            # ----------------- End Helper Functions -----------------

            def adjustSharingAndCapabilities(published_item):
                try:
                    refreshed_item = self.gis.content.get(published_item.id)
                except Exception as e:
                    print(f"Error retrieving refreshed item: {e}")
                    return

                try:
                    item_flc = FeatureLayerCollection.fromitem(refreshed_item)
                    update_definition_dict = {"capabilities": "Query,Extract"}
                    item_flc.manager.update_definition(update_definition_dict)
                except Exception as e:
                    print(f"Error adjusting capabilities: {e}")

                try:
                    item_sharing_mgr = refreshed_item.sharing
                    if core.user_options.sharing_level == "EVERYONE":
                        item_sharing_mgr.sharing_level = SharingLevel.EVERYONE
                    elif core.user_options.sharing_level == "ORG":
                        item_sharing_mgr.sharing_level = SharingLevel.ORG
                    elif core.user_options.sharing_level == "PRIVATE":
                        item_sharing_mgr.sharing_level = SharingLevel.PRIVATE
                    else:
                        item_sharing_mgr.sharing_level = SharingLevel.ORG
                except Exception as e:
                    print(f"Error adjusting sharing level: {e}")

            try:
                if dataset.needs_Subset:
                    # -------------Subset file scenario-------------
                    # -------------First file-------------
                    first_path = paths[0]
                    print(f"\nAdding first subset item for {dataset.dataset_id} to ArcGIS...")
                    try:
                        item = addOrRetry(dataset, first_path)
                    except Exception as e:
                        print(f"Unfortunately adding the first subset failed: {e}")
                        dataset.has_error = True
                        continue

                    # Publish
                    print(f"\nPublishing item for {dataset.dataset_title}...")
                    try:
                        published_item = publishOrRetry(item, publish_parameters=geom_params, file_type=inputDataType, timeout=timeoutTime)
                        adjustSharingAndCapabilities(published_item)
                    except Exception as e:
                        print(f"Unfortunately publishing the file has failed: {e}")
                        dataset.has_error = True
                        continue

                    # -------------Append Subsets-------------
                    if published_item.layers:
                        feature_layer = published_item.layers[0]
                        subset_idx = 1
                        for subset_path in paths[1:]:
                            
                            try:
                                subset_item = addOrRetry(dataset, subset_path)
                            except Exception as e:
                                print(f"\nAdding the subset item failed (addOrRetry method):\nError Message- {e}")
                            try:
                                analyze_params = gis.content.analyze(item=subset_item.id, file_type='csv')
                            except Exception as e:
                                print(f"\nAnalyzing the subset item failed (gis.content.analyze method):\nError Message- {e}")
                            try:
                                append_success = feature_layer.append(
                                    item_id=subset_item.id,
                                    upload_format='csv',
                                    source_info=analyze_params['publishParameters'],
                                    upsert=False,
                                    return_messages=True
                                )
                            except Exception as e:
                                print(f"\nAppending the item failed (feature_layer.append):\nError Message- {e}")
                                                   
                                append_success = False
                            if append_success:
                                subset_idx += 1
                                print(f"\nAppended Subset {subset_idx} of {len(paths)} to {published_item.title}")
                            else:
                                print(f"\nFailed to append subset # {subset_idx} to {published_item.title}")
                            if self.enterprise_bool:
                                subset_item.delete()
                            else:
                                subset_item.delete(permanent= True)
                else:
                    #--------Single file scenario--------------
                    path = dataset.data_filepath
                    print(f"\nAdding item for {dataset.dataset_title} to {gis.properties.portalName}...")
                    try:
                        item = addOrRetry(dataset, path)
                    except Exception as e:
                        print(f"Unfortunately adding the file has failed: {e}")
                        dataset.has_error = True
                        continue
                    # Publish
                    print(f"\nPublishing item for {dataset.dataset_title}...")
                    try:
                        published_item = publishOrRetry(item, publish_parameters=geom_params, file_type=inputDataType, timeout=timeoutTime)
                        adjustSharingAndCapabilities(published_item)
                    except Exception as e:
                        print(f"Unfortunately publishing the file has failed: {e}")
                        dataset.has_error = True
                        continue
                    #--------Single file scenario--------------

                dataset_end_time = time.time()
                dataset_processing_time = dataset_end_time - dataset_start_time
                processed_count += 1
                print(f"Finished processing dataset {dataset.dataset_title} in {dataset_processing_time:.2f} seconds")

            except concurrent.futures.TimeoutError:
                print(f"Publishing took longer than 3 minutes for {dataset.dataset_title}. Cancelling operation.")
                continue
            except Exception as e:
                print(f"An error occurred adding the item for {dataset.dataset_title}: {e}")
                continue

        total_end_time = time.time()
        total_time = total_end_time - total_start_time
        if processed_count == 0:
            print("\n 0 datasets processed")
        else:
            print("\nAll done!")
            print(f"Processing completed for {processed_count} datasets")
            print(f"Total processing time: {total_time:.2f} seconds")



    def searchContentByTag(self, tag: str) -> list:
        gis = self.gis
        try:
            search_query = f'tags:"{tag}" AND owner:{gis.users.me.username} AND type:Feature Service'
            search_results = gis.content.search(query=search_query, max_items=1000)

            # Check if any items were found
            if not search_results:
                print(f"No items found with the tag '{tag}' for the logged-in user.")
                return []

            # Extract and return the item IDs
            item_ids = [item.id for item in search_results]
            
            print(f"Found {len(item_ids)} items with the tag '{tag}':")
            for item in search_results:
                print(f"Title: {item.title}, ID: {item.id}")

            return item_ids
        
        except Exception as e:
            print(f"An error occurred while searching for items: {e}")

    def disable_editing(self, item_id):
        gis = self.gis
        item = gis.content.get(item_id)
        if item is None:
            print(f"Item {item_id} not found")
            return

        # Get the FeatureLayerCollection from the item
        flc = FeatureLayerCollection.fromitem(item)

        # Update the capabilities to disable editing
        flc.manager.update_definition({"capabilities": "Query"})
        print(f"Editing successfully disabled for item {item_id}")
