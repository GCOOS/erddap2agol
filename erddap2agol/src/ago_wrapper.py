from arcgis.gis import GIS
from arcgis.features import FeatureLayer, FeatureLayerCollection
from arcgis.gis._impl._content_manager import SharingLevel
from . import data_wrangler as dw
from . import erddap_client as ec
from . import das_client as dc
import copy, os, sys, pandas as pd, numpy as np, json
from dataclasses import dataclass, field
from typing import Optional, List, Dict

@dataclass
class AgolWrangler:
    gis: Optional[GIS] = None
    sharing_pref: str = "EVERYONE"
    datasets: List['dw.DatasetWrangler'] = field(default_factory=list)
    item_properties: Dict[str, Dict] = field(default_factory=dict)
    erddap_obj: Optional['ec.ERDDAPHandler'] = None 
    geoParams: dict = field(default_factory=lambda: {
        "locationType": "coordinates",
        "latitudeFieldName": "latitude (degrees_north)",
        "longitudeFieldName": "longitude (degrees_east)"
    })


    def __post_init__(self):
        """Initialize AGOL connection"""
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

    def shareDatasetObjAttrs(self) -> None:
        erddapObj = self.erddap_obj
        """Add datasets from the provided ERDDAPHandler instance."""
        if hasattr(erddapObj, 'datasets'):
            self.datasets.extend(erddapObj.datasets)
            print(f"Added {len(erddapObj.datasets)} datasets from erddap_obj to AgolWrangler.")
        else:
            print("The provided erddap_obj does not have a 'datasets' attribute.")

    def makeItemProperties(self) -> None:
        """Creates item properties using dataset attributes"""
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

                    if dataset.nc_global:
                        # Set institution
                        if "institution" in dataset.nc_global:
                            props["accessInformation"] = dataset.nc_global["institution"].get("value", "")
                        elif "creator_institution" in dataset.nc_global:
                            props["accessInformation"] = dataset.nc_global["creator_institution"].get("value", "")
                        
                        # Set license
                        if "license" in dataset.nc_global:
                            props["licenseInfo"] = dataset.nc_global["license"].get("value", "")
                        
                        # Set title and summary
                        dataset_title = dataset.dataset_id

                        props["title"] = dataset_title

                        server_name = dataset.server.split('/erddap/')[0].split('://')[-1]
                        summary = dataset.nc_global.get("summary", {}).get("value", "")
                        props["snippet"] = f"{summary}. {dataset_title} was generated with erddap2agol from the {server_name} ERDDAP."

                    if dataset.is_glider:
                        props["tags"].append("Glider DAC")
                        props["type"] = "GeoJson"
                        
                    self.item_properties[dataset.dataset_id] = props
                    
                except Exception as e:
                    print(f"Error creating item properties for {dataset.dataset_id}: {e}")


    def pointTableToGeojsonLine(self,  X="longitude (degrees_east)", Y="latitude (degrees_north)") -> dict:
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
                        point_start = [df.iloc[i][X], df.iloc[i][Y]]
                        point_end = [df.iloc[i + 1][X], df.iloc[i + 1][Y]]
                        coordinates = [point_start, point_end]

                        # Skip if any coordinate is None
                        if None in point_start or None in point_end:
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

    def postAndPublish(self, inputDataType="csv") -> None:
        geom_params = self.geoParams
        """Publishes all datasets in self.datasets to AGOL, handling subsets if needed."""
        def adjustSharing(published_item):
            try:
                item_gis = gis.content.get(published_item.id)
                item_sharing_mgr = item_gis.sharing
                #hard coded
                item_sharing_mgr.sharing_level = SharingLevel.EVERYONE
                if item_gis.layers:
                    feature_layer = item_gis.layers[0]
                    update_definition_dict = {"capabilities": "Query,Extract"}
                    feature_layer.manager.update_definition(update_definition_dict)
            except Exception as e:
                print(f"An error occurred during disable editing by updating layer capabilities for {dataset.dataset_id}: {e}")

        for dataset in self.datasets:
            item_prop = self.item_properties.get(dataset.dataset_id)
            print(item_prop)
            
            if not item_prop:
                print(f"No item properties found for {dataset.dataset_id}. Skipping.")
                continue

            paths = dataset.data_filepath
            if not paths:
                print(f"No data file path found for {dataset.dataset_id}. Skipping.")
                continue

            try:
                gis = self.gis
                                        
                if dataset.needs_Subset:
                    # ----Post and publish the first subset----
                    first_path = paths[0]
                    print(f"\nAdding first subset item for {dataset.dataset_id} to AGOL...")
                    item = gis.content.add(item_properties=item_prop, data=first_path, HasGeometry=True)

                    # Ensure unique service name
                    if 'name' not in geom_params or not geom_params['name']:
                        geom_params['name'] = item_prop['title']

                    print(f"\nPublishing item for {dataset.dataset_id}...")
                    published_item = item.publish(publish_parameters=geom_params, file_type=inputDataType)
                    adjustSharing(published_item)


                    print(f"\nSuccessfully uploaded and published {item_prop['title']} to ArcGIS Online")
                    print(f"Item ID: {published_item.id}")

                    
                    if not published_item.layers:
                        print(f"No layers found in published item {published_item.id}. Skipping dataset.")
                        continue

                    # Get the feature layer from the published item
                    feature_layer = published_item.layers[0]
                    
                    for subset_path in paths[1:]:
                        print(f"\nAppending subset to feature layer {dataset.dataset_id}...")
                        # Upload the subset CSV as an item
                        subset_item = gis.content.add({}, data=subset_path, HasGeometry=True)
                        analyze_params = gis.content.analyze(item=subset_item.id)
                        append_success = feature_layer.append(
                            item_id=subset_item.id,
                            upload_format='csv',
                            source_info=analyze_params['publishParameters'],
                            upsert=False
                        )
                        if append_success:
                            print(f"Successfully appended {subset_item.title} to {published_item.title}")
                        else:
                            print(f"Failed to append {subset_item.title} to {published_item.title}")
                        # Delete the temporary uploaded item
                        subset_item.delete()
                else:

                    # ---Nonsubset dataset processing
                    path = paths if isinstance(paths, str) else paths[0]
                    print(f"\nAdding item for {dataset.dataset_id} to AGOL...")
                    item = gis.content.add(item_properties=item_prop, data=path, HasGeometry=True)

                    if 'name' not in geom_params or not geom_params['name']:
                        geom_params['name'] = item_prop['title']

                    print(f"\nPublishing item for {dataset.dataset_id}...")
                    if dataset.is_glider:
                        #hardcoding geojson into filetype arg for geojson
                        published_item = item.publish(publish_parameters=geom_params, file_type="GeoJson")
                        adjustSharing(published_item)

                    else:
                        published_item = item.publish(publish_parameters=geom_params, file_type=inputDataType)
                        adjustSharing(published_item)
                        

                    print(f"\nSuccessfully published HFL- {dataset.dataset_id} to ArcGIS Online")

                    # Disable editing by updating layer capabilities
                    


            except Exception as e:
                print(f"An error occurred adding the item for {dataset.dataset_id}: {e}")
                continue



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

    def createFeatureService(self, item_prop: dict) -> str:
        gis = self.gis
        item_prop_mod = copy.deepcopy(item_prop)
        item_prop_mod["title"] = item_prop_mod["title"] + "_AGOL"
        isAvail = gis.content.is_service_name_available(item_prop_mod['title'], "Feature Service")
        if isAvail == True:
            try:
                featureService = gis.content.create_service(item_prop_mod['title'], "Feature Service", has_static_data = False) 
                featureService.update(item_properties = item_prop_mod)
                print(f"Successfully created Feature Service {item_prop_mod['title']}")
                return featureService.id
            
            except Exception as e:
                print(f"An error occurred creating the Feature Service: {e}")
        else:
            print(f"Feature Service {item_prop_mod['title']} already exists, use OverwriteFS to Update")




