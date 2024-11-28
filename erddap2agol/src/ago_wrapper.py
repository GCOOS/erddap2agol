from arcgis.gis import GIS
from arcgis.features import FeatureLayer, FeatureLayerCollection
from . import erddap_client as ec
from . import das_client as dc
import copy, json

gis = GIS("home")

#Connect to AGO. This may work different with docker. 
def agoConnect() -> None:
    try:
        gis = GIS("home")
        print("\nSuccesfully connected to " + gis.properties.portalName + " on " + gis.properties.customBaseUrl)
        return gis
    except Exception as e:
        print(f"An error occurred connecting to ArcGIS Online: {e}")

# Need to work out the rest of the metadata in item props
def makeItemProperties(erddapObj: "ec.ERDDAPHandler", accessLevel = None) -> dict:
    dataid = erddapObj.datasetid
    attribute_tags = erddapObj.attributes

    tags = ["erddap2agol", f"{dataid}"]

    if attribute_tags is not None:
        tags.extend(attribute_tags)

    ItemProperties = {
        "title": dataid,
        "type": "CSV",
        "item_type": "Feature Service",
        "tags": tags
    }

    dasJson = dc.openDasJson(dataid)
    metadata = dasJson.get("NC_Global", {})
    if "license" in metadata and metadata["license"] is not None:
        ItemProperties["licenseInfo"] = metadata["license"].get("value", "")

    #print(ItemProperties)
    return ItemProperties

def defineGeoParams(erddapObj: ec.ERDDAPHandler) -> dict:
    # Hard code coordinate parameters presuming these have already
    # been checked to exist 

    geom_params = erddapObj.geoParams

    attribute_list = erddapObj.attributes
    #This doesnt work, we might have to publish first, then update the properties
    # for attribute in attribute_list:
    #     if "depth" in attribute or "z" in attribute:
    #         geom_params["hasZ"] = True
        

    return geom_params
        

#Also important and should be improved 
def publishTable(item_prop: dict, geom_params: dict, path, erddapObj: ec.ERDDAPHandler):
    try:
        # Remove 'hasStaticData' from geom_params if present
        geom_params.pop('hasStaticData', None)
        
        item = gis.content.add(item_prop, path, HasGeometry=True)
        published_item = item.publish(publish_parameters=erddapObj.geoParams)

        # Disable editing by updating layer capabilities
        fl = published_item.layers[0]
        fl.manager.update_definition({"capabilities": "Query"})

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




