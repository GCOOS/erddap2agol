from arcgis.gis import GIS
from arcgis.features import FeatureLayer, FeatureLayerCollection
from arcgis.gis._impl._content_manager import SharingLevel
from . import data_wrangler as dw
from . import erddap_wrangler as ec
from . import das_client as dc
import copy, os, sys, time, pandas as pd, numpy as np, json
from dataclasses import dataclass, field
from typing import Optional, List, Dict
import concurrent.futures

@dataclass
class AgolWrangler:
    gis: Optional[GIS] = None
    sharing_pref: str = "EVERYONE"
    datasets: List['dw.DatasetWrangler'] = field(default_factory=list)
    item_properties: Dict[str, Dict] = field(default_factory=dict)
    erddap_obj: Optional['ec.ERDDAPHandler'] = None 
    geoParams: dict = field(default_factory=lambda: {
    "locationType": "coordinates",
    "latitudeFieldName": "latitude__degrees_north_",
    "longitudeFieldName": "longitude__degrees_east_",
    "timeFieldName": "time__UTC_",
    })


    def __post_init__(self):
        """Initialize AGOL connection and inherit dataset attributes"""
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
            print("\nSuccesfully connected to " + gis.properties.portalName + " on " + gis.properties.customBaseUrl)
        except Exception as e:
            print(f"AGOL connection error: {e}")

    def skipFromError(func):
        """General skip error decorator that will be applied to all dataset methods"""
        def wrapper(self, *args, **kwargs):
            if dw.DatasetWrangler.has_error == True:
                print(f"Skipping {func.__name__} - due to processing error {self.dataset_id}")
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

    @skipFromError
    def makeItemProperties(self) -> None:
        """Creates item properties using dataset attributes"""
        
        def createDescription(dataset, props):
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
                        "tags": ["erddap2agol", f"{dataset.dataset_id}"]
                    }
                    
                    if dataset.attribute_list:
                        props["tags"].extend(dataset.attribute_list)
                    
                    if dataset.is_nrt is True:
                        props["tags"].append("e2a_nrt")

                    if dataset.server:
                        props["tags"].append(str(dataset.server))

                    if dataset.nc_global:
                        if "institution" in dataset.nc_global:
                            props["accessInformation"] = dataset.nc_global["institution"].get("value", "")
                        elif "creator_institution" in dataset.nc_global:
                            props["accessInformation"] = dataset.nc_global["creator_institution"].get("value", "")

                        if "license" in dataset.nc_global:
                            props["licenseInfo"] = dataset.nc_global["license"].get("value", "")

                        dataset_title = dataset.dataset_id
                        props["title"] = dataset_title

                        server_name = dataset.server.split("/erddap/")[0].split("://")[-1]
                        summary = dataset.nc_global.get("summary", {}).get("value", "")
                        props["snippet"] = f"{summary}. {dataset_title} was generated with erddap2agol from the {server_name} ERDDAP."

                        createDescription(dataset, props)

                    if dataset.is_glider:
                        props["tags"].append("Glider DAC")
                        props["type"] = "GeoJson"

                    self.item_properties[dataset.dataset_id] = props

                except Exception as e:
                    print(f"Error creating item properties for {dataset.dataset_id}: {e}")

    @skipFromError
    def pointTableToGeojsonLine(self,  X="longitude (degrees_east)", Y="latitude (degrees_north)") -> None:
        """For converting standard ERDDAP csvp into geojson"""
        for dataset in self.datasets:
            if dataset.is_glider == True:
                filepath = dataset.data_filepath
                if dataset.data_filepath:
                    print(f"\nConverting {filepath} to GeoJSON...")
                    df = pd.read_csv(filepath)

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

    #-------- Come back to the below function to figure out the multiprocessing stuff

    # def pointTableToGeojsonLine(self, X="longitude (degrees_east)", Y="latitude (degrees_north)") -> dict:
    #     """For converting standard ERDDAP csvp into geojson"""
    #     def chunkCsv(df_chunk, X, Y, data_columns):
    #         """Chunk csv into kv dict for multproc"""
    #         df_chunk = df_chunk.dropna(subset=[X, Y]).replace({np.nan: None})
    #         features = []
    #         for i in range(len(df_chunk) - 1):
    #             point_start = [df_chunk.iloc[i][X], df_chunk.iloc[i][Y]]
    #             point_end = [df_chunk.iloc[i+1][X], df_chunk.iloc[i+1][Y]]
    #             if None in point_start or None in point_end:
    #                 continue
    #             props = df_chunk.iloc[i+1][data_columns].to_dict()
    #             feature = {
    #                 "type": "Feature",
    #                 "geometry": {"type": "LineString", "coordinates": [point_start, point_end]},
    #                 "properties": props
    #             }
    #             features.append(feature)
    #         return features
        
    #     for dataset in self.datasets:
    #         if dataset.is_glider:
    #             filepath = dataset.data_filepath
    #             if filepath:
    #                 chunksize = 10000  # adjust as needed
    #                 futures = []
    #                 data_columns = None
    #                 with concurrent.futures.ProcessPoolExecutor() as executor:
    #                     for df_chunk in pd.read_csv(filepath, chunksize=chunksize):
    #                         if data_columns is None:
    #                             data_columns = [col for col in df_chunk.columns if col not in [X, Y]]
    #                         futures.append(executor.submit(chunkCsv, df_chunk, X, Y, data_columns))
                    
    #                 features = []
    #                 for f in concurrent.futures.as_completed(futures):
    #                     features.extend(f.result())

    #                 geojson = {"type": "FeatureCollection", "features": features}
                    
    #                 savedir = ec.getTempDir()
    #                 filename = dataset.dataset_id + "_line.geojson"
    #                 savepath = os.path.join(savedir, filename)
    #                 with open(savepath, "w") as f:
    #                     json.dump(geojson, f)
    #                 print(f"\nGeoJSON conversion complete @ {savepath}.")
    #                 setattr(dataset, "data_filepath", savepath)

    #             else:
    #                 sys.exit()

    @skipFromError
    def postAndPublish(self, inputDataType="csv", timeoutTime=300) -> None:
        """Publishes all datasets in self.datasets to AGOL, handling subsets if needed."""
        geom_params = self.geoParams.copy()
        geom_params.pop('hasStaticData', None)  # Remove if exists, as done in stable code

        # Time tracking variables
        total_start_time = time.time()
        processed_count = 0

        # Start iterating through datasets 
        for dataset in self.datasets:
            
            if dataset.is_glider is True:
                 inputDataType="GeoJson"

            dataset_start_time = time.time()  # Track start time for this dataset

            item_prop = self.item_properties.get(dataset.dataset_id)
            if not item_prop:
                print(f"No item properties found for {dataset.dataset_id}. Skipping.")
                continue

            paths = dataset.data_filepath
            if not paths:
                print(f"No data file path found for {dataset.dataset_id}. Skipping.")
                continue

            # Set a service name if not already present
            if 'name' not in geom_params or not geom_params['name']:
                geom_params['name'] = item_prop['title']

            def adjustSharingAndCapabilities(published_item):
                # Get fresh item
                #time.sleep(3)  # brief pause
                refreshed_item = self.gis.content.get(published_item.id)

                # Update capabilities using FeatureLayerCollection (like stable code)
                try:
                    item_flc = FeatureLayerCollection.fromitem(refreshed_item)
                    update_definition_dict = {"capabilities": "Query,Extract"}
                    item_flc.manager.update_definition(update_definition_dict)
                    #print(f"Successfully updated capabilities for {refreshed_item.title}")
                except Exception as e:
                    print(f"Error adjusting capabilities: {e}")

                try:
                    item_sharing_mgr = refreshed_item.sharing
                    item_sharing_mgr.sharing_level = SharingLevel.EVERYONE
                    #print(f"Successfully updated sharing for {refreshed_item.title} to EVERYONE")
                except Exception as e:
                    print(f"Error adjusting sharing level: {e}")

            try:
                gis = self.gis

                if dataset.needs_Subset:
                # -------------Subset file scenario-------------
                    first_path = paths[0]
                    print(f"\nAdding first subset item for {dataset.dataset_id} to AGOL...")
                    item = gis.content.add(item_properties=item_prop, data=first_path, HasGeometry=True)

                    # Publish
                    print(f"\nPublishing item for {dataset.dataset_id}...")

                    # set worker to keep track of time for publish.
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(item.publish, publish_parameters=geom_params, file_type=inputDataType)
                        published_item = future.result(timeout=timeoutTime)
                    
                    adjustSharingAndCapabilities(published_item)

                    # Append subsequent subsets
                    if published_item.layers:
                        feature_layer = published_item.layers[0]
                        for subset_path in paths[1:]:
                            subset_item = gis.content.add({}, data=subset_path, HasGeometry=True)
                            analyze_params = gis.content.analyze(item=subset_item.id)
                            append_success = feature_layer.append(
                                item_id=subset_item.id,
                                #Probably wont have to worry about geojson for large files, but just in case this is where youd change it
                                upload_format='csv',
                                source_info=analyze_params['publishParameters'],
                                upsert=False
                            )
                            if append_success:
                                print(f"Appended {subset_item.title} to {published_item.title}")
                            else:
                                print(f"\nFailed to append {subset_item.title}")
                            subset_item.delete(permanent=True)
                # -------------Subset file scenario-------------

                else:
                    #--------Single file scenario--------------
                    #path = paths if isinstance(paths, str) else paths[0]
                    path = dataset.data_filepath
                    print(f"\nAdding item for {dataset.dataset_id} to AGOL...")
                    item = gis.content.add(item_properties=item_prop, data=path, HasGeometry=True)
                    # Publish
                    print(f"\nPublishing item for {dataset.dataset_id}...")

                    # set worker to keep track of time for publish.
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(item.publish, publish_parameters=geom_params, file_type=inputDataType)
                        published_item = future.result(timeout=timeoutTime)
                    adjustSharingAndCapabilities(published_item)
                    #--------Single file scenario--------------

                # End of dataset processing - print time
                dataset_end_time = time.time()
                dataset_processing_time = dataset_end_time - dataset_start_time
                processed_count += 1
                print(f"Finished processing dataset {dataset.dataset_id} in {dataset_processing_time:.2f} seconds")

            except concurrent.futures.TimeoutError:
                print(f"Publishing took longer than 3 minutes for {dataset.dataset_id}. Cancelling operation.")
                future.cancel()
            except Exception as e:
                print(f"An error occurred adding the item for {dataset.dataset_id}: {e}")
                continue

        # After all datasets processed, print total time
        total_end_time = time.time()
        total_time = total_end_time - total_start_time
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




    #The below functions have no utility right now
    # #-----------------------------------------------------------
    # def appendTableToFeatureService(self, featureServiceID: str, tableID: str) -> str:
    #     gis = self.gis
    #     try:
    #         featureServiceItem = gis.content.get(featureServiceID)
    #         tableItem = gis.content.get(tableID)    
    #         response = featureServiceItem.append(item_id= tableID, upload_format ='csv', source_table_name = tableItem.title)      
            
    #         if response['status'] == 'Completed':
    #             print(f"Successfully appended data to Feature Service ID: {featureServiceItem.id}")
    #         else:
    #             print(f"Append operation completed with issues: {response}")
            
    #         return response
    #     except Exception as e:
    #         print(f"An error occurred appending the CSV data: {e}")

    # def createFeatureService(self, item_prop: dict) -> str:
    #     gis = self.gis
    #     item_prop_mod = copy.deepcopy(item_prop)
    #     item_prop_mod["title"] = item_prop_mod["title"] + "_AGOL"
    #     isAvail = gis.content.is_service_name_available(item_prop_mod['title'], "Feature Service")
    #     if isAvail == True:
    #         try:
    #             featureService = gis.content.create_service(item_prop_mod['title'], "Feature Service", has_static_data = False) 
    #             featureService.update(item_properties = item_prop_mod)
    #             print(f"Successfully created Feature Service {item_prop_mod['title']}")
    #             return featureService.id
            
    #         except Exception as e:
    #             print(f"An error occurred creating the Feature Service: {e}")
    #     else:
    #         print(f"Feature Service {item_prop_mod['title']} already exists, use OverwriteFS to Update")




