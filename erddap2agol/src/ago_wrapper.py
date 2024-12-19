from arcgis.gis import GIS
from arcgis.features import FeatureLayer, FeatureLayerCollection
from arcgis.gis._impl._content_manager import SharingLevel
from . import data_wrangler as dw
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

    def __post_init__(self):
        """Initialize AGOL connection"""
        self.connect()

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


    def makeItemProperties(self, dataset: 'dw.DatasetWrangler') -> None:
        """Creates item properties using dataset attributes"""
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
                dataset_title = dataset.nc_global.get("title", {}).get("value", dataset.dataset_id)
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


    def pointTableToGeojsonLine(filepath, erddapObj: ec.ERDDAPHandler, X="longitude (degrees_east)", Y="latitude (degrees_north)") -> dict:
        if filepath:
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
            filename = erddapObj.datasetid + "_line.geojson"
            savepath = os.path.join(savedir, filename)
            with open(savepath, "w") as f:
                json.dump(geojson, f)
            print(f"\nGeoJSON conversion complete @ {savepath}.")
            return savepath
        else:
            sys.exit()

    def postAndPublish(item_prop: dict, geom_params: dict, path, erddapObj: ec.ERDDAPHandler, inputDataType="csv", sharing_lvl = "EVERYONE") -> str:
        try:
            geom_params.pop('hasStaticData', None) 

            print(f"\ngis.content.add...")
            item = gis.content.add(item_prop, path, HasGeometry=True)

            # Check if the server matches the specific condition
            if erddapObj.server == "https://gliders.ioos.us/erddap/tabledap/":
                # Ensure a unique service name for this specific server
                unique_service_name = f"{item_prop['title']}_service"
                erddapObj.geoParams['name'] = unique_service_name  # Explicitly set service name

            
            # Ensure publish parameters include a unique service name
            if 'name' not in erddapObj.geoParams or not erddapObj.geoParams['name']:
                erddapObj.geoParams['name'] = item_prop['title']

            print(f"\nitem.publish...")
            published_item = item.publish(publish_parameters=erddapObj.geoParams, file_type=inputDataType)

            # we should be doing this above, i believe geoParams is the service definition
            # Disable editing by updating layer capabilities
            item_gis = gis.content.get(published_item.id)
            item_flc = FeatureLayerCollection.fromitem(item_gis)
            update_definition_dict = {"capabilities": "Query,Extract"}
            item_flc.manager.update_definition(update_definition_dict)

            if sharing_lvl is not None:
                item_sharing_mgr = item_gis.sharing
                item_sharing_mgr.sharing_level = SharingLevel.EVERYONE

            print(f"\nSuccessfully uploaded {item_prop['title']} to ArcGIS Online")
            print(f"Item ID: {published_item.id}")
            return published_item.id
        except Exception as e:
            print(f"An error occurred adding the item: {e}")



    def searchContentByTag(tag: str) -> list:
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

    def disable_editing(item_id):
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
    #-----------------------------------------------------------
    def appendTableToFeatureService(featureServiceID: str, tableID: str) -> str:
        try:
            featureServiceItem = gis.content.get(featureServiceID)
            tableItem = gis.content.get(tableID)    
            response = featureServiceItem.append(item_id= tableID, upload_format ='csv', source_table_name = tableItem.title)      
            
            if response['status'] == 'Completed':
                print(f"Successfully appended data to Feature Service ID: {featureServiceItem.id}")
            else:
                print(f"Append operation completed with issues: {response}")
            
            return response
        except Exception as e:
            print(f"An error occurred appending the CSV data: {e}")

    def createFeatureService(item_prop: dict) -> str:
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




