##############################################################################
# Update / Overwrite Feature Service                                         #
# By: Deniz Karagulle & Paul Dodd, Software Product Release, Esri            #
#                                                                            #
# v1.0.0, Jan 2019 - Released                                                #
# v1.1.0, Jan 2019 - Added support for 'Touching' views related to FS.       #
# v1.2.0, Feb 2019 - Updated to run Standalone or be leveraged via Import.   #
# v1.3.0, Feb 2019 - Updated to support URL download.                        #
# v1.4.0, Feb 2019 - Updated to better handle SSL Context. Save download     #
#                    to temp folder when url is used. Open url prior to      #
#                    downloading, to execute faster Last Modified check!     #
#                    Compare url data Last Modified to Service Layer Last    #
#                    Modified, skip if not newer. Improved reporting.        #
# v1.4.1, Mar 2019 - Patched Last Service Edit date logic to handle 'None'   #
#                    Added Elapsed Time to Overwrite results.                #
# v1.4.2, Apr 2020 - Added connection password validation, avoid password    #
#                    prompt if password is missing or null.                  #
# v1.4.3, May 2020 - Added login option to leverage ArcGIS Pro account.      #
# v1.4.4, Jul 2020 - Corrected Url Header access issue during downloads.     #
# v2.0.0, Sep 2021 - Added touching 'timeInfo' on Layers that have Time      #
#                    Series enabled. Added 'getFeatureServiceTarget',        #
#                   'swapFeatureViewLayers', and 'updateRelationships'       #
#                    functionality to support A/B FV Layer Swap logic. Adds  #
#                    indexes, 'timeInfo', and layer optimization properties  #
#                    following overwrite. Restores item Data if needed.      #
#                    Added Profile Password verify, and 'AllowPWprompt'      #
#                    action switch. Added 'no' options. Added 'outPath' and  #
#                    'dryRun' options. Added 'convert' switches.             #
# v2.0.1, Sep 2021 - Patch 'status' for download and Covert actions, should  #
#                    report 'None' when no update required!                  #
# v2.0.2, Oct 2021 - Patch to work around Service Overwrite not triggering   #
#                    backend to update Layer Extent on associated Views.     #
# v2.1.0, Nov 2021 - Removed Dropping Optimization on Overwrite, not needed !#
#                    Added File Item Overwrite support. Trigger CRC file     #
#                    check when Conversion enabled. Added 'ignoreAge' and    #
#                    'noSwap' parameters to 'swapFeatureViewLayers' function.#
#                    Added 'ignoreAge' parameter to 'overwriteFeatureService'#
#                    function. Added automatic Layer Optimize cancelation on #
#                    Overwrite.                                              #
# v2.1.1, Dec 2021 - Patch Conversion routines to handle null Z-values in    #
#                    Geometries, improve trapping of failures during row     #
#                    processing, and updated retired 'Rss2Json' routine to   #
#                    include deprecation message.                            #
# v2.1.2, Feb 2022 - Patch to detect existing field Indexes case-insensitive.#
#                    Added 'ignoreDataItemCheck' parameter to 'get target'   #
#                    function, helping support Swap Layers to services that  #
#                    do not have a data file item when not overwriting.      #
#                    Added service manager refresh to admin update steps, to #
#                    clear REST cache, ensuring we receive the current props.#
#                    Patch added to overwrite and swap logic to support      #
#                    change in Python API v2.0 service 'table' property.     #
#                    Updated 'updateRelationships' function to return proper #
#                    outcome details (overlooked).                           #
# v2.1.3, Jun 2024 - Patch service manager refresh failure issue. Added trace#
#                    report to Convert execution on exception. Set 'ignore-  #
#                    DataItemCheck' property to True when 'GetTarget' action #
#                    initiated. Hardened Async job status check. Update      #
#                    'overwriteFeatureService' to support GeoPackage type    #
#                    and file item type when item.name includes a period,    #
#                    updated retry loop to try one final overwrite after del,#
#                    fixed error stop issue on failed overwrite attempts.    #
#                    Removed restriction on uploading files larger than 2GB. #
#                    Restores missing 'itemInfo' file on service File items. #
#                    Corrected false swap success when view has no layers.   #
#                    Lifted restriction of Overwrite/Swap Layers for OGC.    #
#                    Added 'serviceDescription' to service detail backup.    #
#                    Added 'thumbnail' to item backup/restore logic. Added   #
#                    'byLayerOrder' parameter to 'swapFeatureViewLayers'.    #
#                    Added 'SwapByOrder' action switch.                      #
#                    Patch added to overwriteFeatureService 'status' check.  #
# v2.1.4, Aug 2024 - Patch for June 2024 update made to 'managers.overwrite' #
#                    API script that blocks uploads > 25MB, API v2.3.0.3     #
#                    Patch 'overwriteFeatureService' to correctly identify   #
#                    overwrite file if service has multiple Service2Data     #
#                    relationships.                                          #
##############################################################################

import os, sys, datetime, tempfile, json, time, traceback, platform
import urllib.request, urllib.parse, shutil, filecmp, zlib
import base64, collections

if not __name__ == "__main__":
    # Make sure arcgis module is loaded if importing
    import arcgis

version = "v2.1.4"
converterFolder = "Converters"

# Item types that can be a source to a Service
dataItemTypes = ["Service Definition", "CSV", "Shapefile", "Tile Package", "Feature Collection", "File Geodatabase", "GeoJson",
    "GeoPackage", "Scene Package", "Vector Tile Package", "SQLite Geodatabase", "Microsoft Excel", "Compact Tile Package", "Image Collection"
] # From 'Service2Data' Relationship, less 'Feature Service': https://developers.arcgis.com/rest/users-groups-and-items/relationship-types.htm

# Item types that are simply a file-based item
fileItemTypes = ["Microsoft Word", "Microsoft PowerPoint", "PDF", "Image", "Visio Document", "Map Package", "Code Sample"] + dataItemTypes

def _getManager( item, verbose=None, outcome=None):
    """Internal Function: _getManager( <Feature Service or View Item object>[, <verbose>[, <outcome>]])

Returns: Feature Layer Collection manager for given Feature Service or View Item, or <outcome> results on failure.

If aquired, manager object is set as <item> attribute called 'manager' and manager object is returned.
"""

    if not outcome:
        outcome = { "success": None, "items": []}

    #
    # Verify Item is of Type Feature Service
    #
    if not item.type == "Feature Service":
        if not verbose == False:
            print( "\n * Item is not a Feature Service or View: {}, '{}'".format( item.id, item.title))

        outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "get manager", "result": "Item Type is NOT a 'Feature Service' or 'Feature View'"})
        outcome[ "success"] = False     # Set as error

        return outcome

    else:
        # Get Feature Layer Collection Manager object
        manager = arcgis.features.FeatureLayerCollection.fromitem( item).manager if not hasattr( item, "manager") else item.manager
        if not hasattr( item, "manager"):
            setattr( item, "manager", manager)

        _refreshManager( manager)

        return manager

def _refreshManager( manager):
    """Internal Function: _refreshManager( <FeatureLayerCollection object to refresh>)"""
    try:
        manager.refresh()
    except Exception as e:
        print( " * Refresh Manager Error: '{}'".format( e))

def _getRecursiveKey( obj, compoundKey, checkIfIn=False):
    """Internal Function: _getRecursiveKey( <dictionary object>, <compound key string>)

Return: Value of recursive dictionary Key search using string of Keys separated by '.' (checkIfIn = False)

Or

Return: True or False if <compoundKey> is in <obj> (checkIfIn = True)
"""
    keys = compoundKey.split( ".", 1)
    if keys:
        if keys[0] in obj:
            if len( keys) > 1:
                return _getRecursiveKey( obj[ keys[0]], keys[-1], checkIfIn)
            return True if checkIfIn else obj[ keys[0]]
        return False

def _getCRC( filename):
    # Calculate CRC for datafile <filename>
    crc = 0
    if os.path.exists( filename):
        with open( filename, "rb") as iFP:
            block = True
            while block:
                block = iFP.read()
                if block:
                    crc += zlib.crc32( block) & 0xffffffff
    return crc

def _asyncJob( service, endpoint, data, verbose=None, indent="", noWait=False, timeout=None):
    """Internal Function: _asyncJob( <service>, <endpoint>, <data>[, <verbose>[, <indent>[, <noWait>]])

     <service> = Service or Layer Object
    <endpoint> = Service or Layer URL endpoint, will be appended to Service or Layer's URL
        <data> = Dictionary of endpoint Key:Value content to deliver to endpoint
     <verbose> = True, False, or None, display responses and progress
      <indent> = Leading spaces to apply to messages, for alignment only
      <noWait> = Perform one status check then exit without waiting for completion.
                 Outcome will be {"success": True, "status": "<status URL>"}
     <timeout> = Timeout period in seconds, before giving up!

Submit URL with Async call and wait for Job results to return.
"""
    outcome = {"success": None}
    con = service._gis._con
    url = service.url + "/" + endpoint
    msg = ""
    lastStatus = ""
    consecutiveExceptions = 5   # Allowed number of Consecutive Exceptions before quitting when no timeout given!
    exceptionsLimit = consecutiveExceptions
    sleepCycles = 0
    sleepTime = 0.25
    sleepIntervals = {"8": 1, "11": 2, "16": 5, "23": 10, "29": 15}   # Slowly increasing Job status query interval by changing the Sleep Time value at Sleep Cycle Intervals
    # Key: Value = {<change at interval>: <new sleep time>}. Matching: 2secs (8 @1/4sec/ea), 5secs (+3 @1sec/ea), 15secs (+5 @2secs/ea), 60secs (+7 @5secs/ea), and 120secs (+6 @10secs/ea)

    try:
        outcome = con.post( url, data)
        if isinstance( outcome, dict) and outcome.get( "statusURL", None):
            statusUrl = outcome[ "statusURL"]
            msgSep = ""
            if not verbose == False:
                msg = "{} - JobId: '{}'".format( indent, os.path.split( statusUrl)[-1])
                if verbose:
                    print( msg, end="")
                msgSep = "\n"

            # Handle Job status query
            while True:
                try:
                    outcome = con.post( statusUrl, {"f": "json"})
                    consecutiveExceptions = exceptionsLimit
                except Exception as e:
                    # Capture exception and retry until timeout
                    if not timeout and consecutiveExceptions <= 1:
                        raise
                    consecutiveExceptions -= 1
                    outcome = {"status": str(e)}

                if verbose:
                    print( "{}Status: '{}'".format( msgSep, outcome))
                    msgSep = ""

                status = outcome.get( "status", "Error").capitalize()

                if not status == lastStatus:
                    lastStatus = status
                    if verbose:
                        print( "{}{} - Job Status: '{}'".format( msgSep, indent, status))
                    msgSep = ""

                errorCode = outcome["error"].get( "code", 0) if "error" in outcome else 399
                errorDesc = outcome["error"].get( "description", "N/A") if "error" in outcome else outcome

                if status == "Completed":
                    outcome = {"success": True}
                    break
                elif status in ["Failed", "Error"]:
                    outcome = {"success": False, "error": {"code": errorCode, "message": "{}, '{}' request failed!".format( status, endpoint), "details": str( errorDesc)}}
                    break

                if noWait:
                    if not verbose == False:
                        print( " * No Wait specified * Manual Status URL: '{}'".format( statusUrl))
                    outcome = {"success": True, "status": statusUrl}
                    break

                sleepCycles += 1
                sleepTime = sleepIntervals.get( str( sleepCycles), sleepTime)   # Adjust Sleep time by the interval cycles performed
                if verbose:
                    print( "{} * Waiting {} seconds...".format( indent, sleepTime))

                time.sleep( sleepTime)

                if timeout:
                    timeout -= sleepTime
                    if timeout <= 0:
                        print( " * Last Status: '{}'".format( lastStatus))
                        return "Timeout"

    except Exception as e:
        outcome = {"success": False, "error": {"code": 400, "message": "Error, '{}' request failed!".format( endpoint), "details": [str( e)]}}
        if not verbose == False:
            traceback.print_exc()
            print( "\n * Error: '{}'".format( e))

    if verbose is None:
        # Provide length of Progress Message, for removal by requester if required.
        outcome["progressLength"] = len(msg) + 1

    return outcome

def _backupProperties( item, verbose=None, outcome=None, outPath=""):
    """Internal Function: _backupProperties( <Feature Service or View Item object>[, <verbose>[, <outcome>]])

Temporarily store select Item and Service properties as 'backup' Attribute Dictionaries in Item.

Returns: Outcome Dictionary
"""
    def makeDict( obj, keys):
        output = {}
        for key, applyKey in keys:
            if _getRecursiveKey( obj, key, True):
                output[ key] = (applyKey, _getRecursiveKey( obj, key))

        return output

    # Item and Service Properties to record
    itemProperties = [
        ("extent", "extent")#,
        #("title", "title"),
        #("snippet", "snippet"),
        #("description", "description"),
        #("tags", "tags"),
        #("licenseInfo", "licenseInfo"),
        #("accessInformation", "accessInformation")
    ]
    serviceProperties = [
        ("capabilities", "capabilities"),
        ("hasStaticData", "hasStaticData"),
        ("hasVersionedData", "hasVersionedData"),
        ("adminServiceInfo.cacheMaxAge", "cacheMaxAge"),
        ("maxRecordCount", "maxRecordCount"),
        ("serviceDescription", "serviceDescription")
    ]

    if not outcome:
        outcome = { "success": None, "items": []}

    try:
        backupDetails = {}

        # Get backup file if exists
        backupFile = os.path.join( tempfile.gettempdir() if not outPath else outPath, "{}_Backup.json".format( item.id))
        if os.path.exists( backupFile):
            if verbose:
                print( " * Loading details from Backup File: '{}'".format( backupFile))
            # Load from Backup file, prior restore failed!
            try:
                backupDetails = json.load( open( backupFile, "r"))
            except Exception as e:
                if not verbose == False:
                    print( " * Failed to load Backup File '{}', error: '{}'".format( backupFile, e))

        # Backup Item or View properties
        itemProps = backupDetails.get( "itemDetails", makeDict( item, itemProperties))
        for key, value in itemProps.copy().items():
            if (isinstance( value, list) or isinstance( value, tuple)) and value and key == value[0]:
                # Is old structure, update
                itemProps[key] = value[1]

        #setattr( item, "backupItemProperties", makeDict( backupDetails.get( "itemDetails", item), itemProperties))
        setattr( item, "backupItemProperties", itemProps)

        # Backup Item or View Thumbnail
        if backupDetails.get("itemThumbnail"):
            setattr( item, "backupItemThumbnail", backupDetails.get( "itemThumbnail", {}))
        else:
            thumbnail = item.get_thumbnail()
            if thumbnail and item.thumbnail:
                try:
                    thumbnail = "".join( ["data:image/{};base64,".format( os.path.splitext( item.thumbnail)[-1][1:])] + base64.encodebytes( thumbnail).decode().split())
                    setattr( item, "backupItemThumbnail", {"file_name": item.thumbnail, "encoded_image": thumbnail})
                except Exception as e:
                    if not verbose == False:
                        print( " * Failed to backup item Thumbnail, error: '{}'".format( e))

        # Backup Item Data, if it has any!
        for loop in range( 2, -1, -1):
            setattr( item, "backupItemData", backupDetails.get( "itemData", item.get_data()))
            if item.backupItemData:
                break
            if loop:
                time.sleep( 1)
        else:
            if verbose:
                print( "\n * Service/View Item 'data' is empty, nothing to backup!")

        # Backup Service properties
        manager = _getManager( item, verbose=verbose, outcome=outcome)
        managerProperties = dict( manager.properties) if hasattr( manager, "properties") else {}
        if managerProperties:
            # Service Layer properties differ from Layer properties, which contain more detail
            managerProperties[ "layers"] = [dict( layer.properties) for layer in manager.layers] if hasattr( manager, "layers") else []
            managerProperties[ "tables"] = [dict( table.properties) for table in manager.tables] if hasattr( manager, "tables") else []

        #serviceDetails = backupDetails.get( "serviceDetails", dict( manager.properties) if hasattr( manager, "properties") else {})
        serviceDetails = backupDetails.get( "serviceDetails", managerProperties)

        if serviceDetails:
            setattr( item, "backupServiceProperties", makeDict( serviceDetails, serviceProperties))

            # Backup View's Related Items, favor existing related items over backup, as long as the relation count is correct!
            relatedItems = [relItem.id for relItem in item.related_items( "Service2Service", "reverse")] if manager.properties.get( "isView", False) else []
            setattr( item, "backupRelationships", relatedItems if len( relatedItems) == 2 else backupDetails.get( "relatedItems", relatedItems))
            #setattr( item, "backupRelationships", backupDetails.get( "relatedItems", [relItem.id for relItem in item.related_items( "Service2Service", "reverse")] if manager.properties.get( "isView", False) else []))

            # Backup Layer and Table properties
            setattr( item, "backupLayerProperties", eval( serviceDetails.get( "layers", []).__repr__()))
            setattr( item, "backupTableProperties", eval( serviceDetails.get( "tables", []).__repr__()))

            json.dump( {
                #"itemDetails": backupDetails.get( "itemDetails", item.backupItemProperties),
                #"itemThumbnail": backupDetails.get( "itemThumbnail", item.backupItemThumbnail),
                "itemDetails": item.backupItemProperties,
                #"itemThumbnail": item.backupItemThumbnail,
                "serviceDetails": serviceDetails,
##                "serviceDetails": item.backupServiceProperties,
                "itemData": item.backupItemData,
                "relatedItems": item.backupRelationships
            }, open( backupFile, "w"), indent=3, separators=(',', ':'))
            setattr( item, "backupFile", backupFile)
        else:
            raise Exception( "Service/View 'manager' has no Properties")

    except Exception as e:
        if not verbose == False:
            traceback.print_exc()
            print( " * Backup Properties Failed, Item: {}, '{}', Error: {}".format( item.id, item.title, e))
        outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "backup service/item properties", "result": str( e)})
        outcome[ "success"] = False     # Set as error

    return outcome

def _prints( message, spaces):
    """Internal Function: _prints( <string message>, <lenth of spaces>)
Print with additional spaces padded on right, to clear remaining line."""

    spaces -= len( message)
    print( message + " " * spaces)

def _restoreProperties( item, verbose=None, outcome=None, touchTimeSeries=True, noIndexes=False, preserveProps=True, noWait=False, noProps=False, dryRun=False):
    """Internal Function: _restoreProperties( <Feature Service or View Item object>[, <verbose>[, <outcome>[, <toughTimeSeries>[, <noIndexes>[, <preserveProps>[, <noWait>[, <noProps>[, <dryRun>]]]]]]]])

Restore select Item and Service properties from 'backup' Attribute Dictionaries in Item.

Returns: updated item object
"""
    maxVerbose = "{}".format( verbose).lower() == "max"

    if not verbose == False:
        if noProps:
            print( "\n * Properties NOT Restored, 'No Props' set to True!")
        else:
            print( "\nStarting Service Property Restoration...")

    startRestore = datetime.datetime.now()

    if not outcome:
        outcome = { "success": None, "items": []}

    # Grab backed up properties
    backupFile = item.backupFile if hasattr( item, "backupFile") else None
    serviceProperties = item.backupServiceProperties if hasattr( item, "backupServiceProperties") else {}
    itemProperties = item.backupItemProperties if hasattr( item, "backupItemProperties") else {}
    itemThumbnail = item.backupItemThumbnail if hasattr( item, "backupItemThumbnail") else {}
    layerProperties = item.backupLayerProperties if hasattr( item, "backupLayerProperties") else []
    tableProperties = item.backupTableProperties if hasattr( item, "backupTableProperties") else []
    itemData = item.backupItemData if hasattr( item, "backupItemData") else {}
    relatedItems = item.backupRelationships if hasattr( item, "backupRelationships") else []

    # Reload Item before checking for changes, to gather up to date details, backup attributes are automatically dropped!
    item = item._gis.content.get( item.id)
    manager = _getManager( item, verbose=verbose, outcome=outcome)

    if verbose and manager and not noProps:
        print( "\nCurrent Details Gathered, Elapsed Time: {}".format( datetime.datetime.now() - startRestore))

    # Restore Related items
    setRelated = False
    if relatedItems:
        outcomeCount = len( outcome[ "items"])
        updateRelationships( item, relatedItems, verbose=verbose, outcome=outcome, dryRun=dryRun)
        if not outcomeCount == len( outcome[ "items"]):
            # Error detected, set retry condition for a second attempt after item/service restoration
            setRelated = True

    if not noProps:
        useDP = True if globals().get( "optimizedp") else False
        # Restore only if 'noProps' is False!
        if hasattr( manager, "properties"):
            serviceIndexes = set()  # Record any indexes created for service, as not to repeat on multiple Layers!
            isView = ("isView" in manager.properties and manager.properties["isView"])
            # Restore Layer and Table properties
            for title, array, backup in [["Layer", manager.layers if hasattr( manager, "layers") else [], layerProperties], ["Table", manager.tables if hasattr( manager, "tables") else [], tableProperties]]:
                for index, layer in enumerate( array):
                    serviceDefinition = backup[ index] if len( backup) > index else {}
                    layerName = layer.properties.get( "name", "")
                    userTable = layer.properties.get( "adminLayerInfo", {}).get( "tableName", "").split(".", 1)[-1]
                    timeDefinition = {}
                    timeAction = "updateDefinition"
                    multiScaleGeometry = {} # Supporting Layer Optimization

                    if "multiScaleGeometryInfo" in serviceDefinition and not isView:
                        multiScaleGeometry = {"multiScaleGeometryInfo": eval( serviceDefinition[ "multiScaleGeometryInfo"].__repr__())}
                        if useDP:
                            multiScaleGeometry[ "multiScaleGeometryInfo"][ "generalizationType"] = "DP"

                        del serviceDefinition["multiScaleGeometryInfo"]

                    if verbose:
                        isDP = multiScaleGeometry.get( "multiScaleGeometryInfo", {}).get( "generalizationType", "") == "DP"
                        print( "\nRestoring {} Properties for: '{}'{}".format( title, layerName, " * Optimized Layer{} *".format( " (w/DP)" if isDP else "") if multiScaleGeometry else ""))

                    if "timeInfo" in serviceDefinition:
                        if touchTimeSeries:
                            timeDefinition[ "timeInfo"] = dict( serviceDefinition.get( "timeInfo", {}))
                            if timeDefinition[ "timeInfo"].get( "hasLiveData", False):
                                if verbose:
                                    print( " - Turning OFF live data in Time Info!")
                                timeDefinition[ "timeInfo"][ "hasLiveData"] = False

                        elif "timeInfo" not in layer.properties:
                            timeDefinition[ "timeInfo"] = dict( serviceDefinition.get( "timeInfo", {}))
                            if verbose:
                                print( " - Adding Time Info to Layer!")
                            timeAction = "addToDefinition"

                        del serviceDefinition[ "timeInfo"]

                    if "adminLayerInfo" in serviceDefinition:
                        del serviceDefinition[ "adminLayerInfo"]

                    indexes = {}
                    if "indexes" in serviceDefinition:
                        if not isView:
                            # Get Fields as Dict
                            fields = {field["name"].lower(): field for field in layer.properties["fields"]}

                            for index in serviceDefinition[ "indexes"]:
                                indexFields = str(index[ "fields"]).lower()
                                for newLayerIndex in layer.properties[ "indexes"]:
                                    if indexFields == str(newLayerIndex[ "fields"]).lower(): # v2.1.2
                                        break
                                else:
                                    skipMessage = ""
                                    indexFields = []

                                    # No Indexes flag specified?
                                    if noIndexes:
                                        skipMessage = "'noIndexes' flag set!"

                                    elif not index[ "fields"]:
                                        # Do not add index that does not include fields
                                        skipMessage = "index has no fields!"

                                    else:
                                        # Validate fields
                                        for field in index[ "fields"].split( ","):
                                            field = field.strip()
                                            if not field.lower() in fields:
                                                skipMessage = "index field '{}' does not exist!".format( field)
                                                break
                                            elif fields[ field.lower()].get( "length", 0) > 4000:
                                                skipMessage = "index field '{}' cannot be used as a key column, too large (>4000 bytes)!".format( field)
                                                break
                                            indexFields.append( field)

                                    # Report findings
                                    if skipMessage:
                                        if verbose:
                                            print( " * Skipping missing index: '{}', {}".format( index[ "name"], skipMessage))
                                        continue

                                    # Add Index to list
                                    indexName = "_".join( [userTable] + indexFields + ["idx"])
                                    if indexName in serviceIndexes:
                                        if verbose:
                                            print( " * Skipping creation, index already exists: '{}'".format( indexName))
                                        continue

                                    serviceIndexes.add( indexName)

                                    indexes[ "indexes"] = indexes.get( "indexes", [])
                                    indexes[ "indexes"].append( eval( index.__repr__()))
                                    #indexes[ "indexes"][-1][ "name"] = userTable + "_" + "_".join( indexFields) + "_idx"
                                    indexes[ "indexes"][-1][ "name"] = indexName
                                    indexes[ "indexes"][-1][ "fields"] = ",".join( indexFields) # If Multi-field index, join using comma

                                    if verbose:
                                        print( " - Add missing Index for Field(s): '{}', index: '{}'".format( indexes[ "indexes"][-1]["fields"], indexes["indexes"][-1]["name"]))

                        del serviceDefinition[ "indexes"]

                    if "editingInfo" in serviceDefinition:
                        #serviceDefinition[ "editingInfo"][ "lastEditDate"] = 0
                        del serviceDefinition[ "editingInfo"]

                    # Manage Layer Properties that have to be applied at the Service Level
                    for property in ["preferredTimeReference"]:
                        if property in serviceDefinition:
                            if serviceDefinition[ property]:
                                # Add to Service Details as Property Reference and Tuple containing Key and Value
                                serviceProperties[ property] = (property, serviceDefinition[ property])
                            del serviceDefinition[ property]

                    # Manage removal of unacceptable properties for updaing a View
                    if isView:
                        for property in ["definitionQuery", "multiScaleGeometryInfo", "multiScaleGeometryStatus", "updateLayerDefinitionStatus"]:
                            if property in serviceDefinition:
                                del serviceDefinition[ property]

                    # Ignore key properties from backed up that are no longer present in overwritten service/view
                    acceptableAdditions = set( ["multiScaleGeometryInfo", "exceedsLimitFactor", "viewDefinitionQuery", "layerOverrides"])
                    for property in list( serviceDefinition.keys()):
                        if property in acceptableAdditions:
                            continue

                        if property not in layer.properties:
                            if verbose:
                                print( " - Ignoring Layer Property: '{}', no longer on file!".format( property))
                            del serviceDefinition[ property]

                        #elif json.dumps( serviceDefinition[ property]) == json.dumps( layer.properties.get( property, "")):
                        elif serviceDefinition[ property] == layer.properties[ property]:
                            #if verbose:
                            #    print( " - Ignoring Layer Property: '{}', no change detected!".format( property))
                            del serviceDefinition[ property]

                    if serviceDefinition and verbose:
                        print( " - {} Properties to Restore: '{}'".format( title, "', '".join( serviceDefinition.keys())))

                    #makeAsyncCall = True if globals().get( "async") else False
                    makeAsyncCall = False #True
                    noChangesApplied = True
                    for defTitle, defAction, definition, applyDef, skipEmpty, asyncCall, asyncNoWait in [
                        [ title, "updateDefinition", serviceDefinition, 1, True, makeAsyncCall, False],
                        [ "Index", "addToDefinition", indexes, 1, True, makeAsyncCall, False],
                        [ "Time", timeAction, timeDefinition, 1, True, makeAsyncCall, False],
                        [ "Optimization", "updateDefinition", multiScaleGeometry, 1, True, True, noWait]]:

                        if skipEmpty and not definition:
                            # Skip if definition is empty and no action required
                            continue

                        start = datetime.datetime.now()
                        noChangesApplied = False

                        actionData = json.dumps( obj=definition, separators=(',', ':'))
                        data = { "f": "json", "async": asyncCall, defAction: actionData}
                        dataSize = len( urllib.parse.urlencode( data)) + 200

                        if asyncCall and dataSize > 65535:
                            if verbose:
                                print( " * Cannot Restore {} properties Asynchronously, size ({:.1f}KB) Exceeds 64KB max, switching to Synchronous!".format( defTitle, float( dataSize)/1024))
                            asyncCall = False
                            data[ "async"] = False

                        try:
                            if dryRun:
                                if verbose:
                                    print( " * Dry Run * No Change!")
                                status = {"success": True}
                            else:
                                # Async cannot handle Data larger than 64KB, use Sync instead!
                                if asyncCall:
                                    status = _asyncJob( layer, defAction, data, verbose=True if maxVerbose else (None if verbose else False), noWait=asyncNoWait)
                                else:
                                    # Apply Definition X times, to make sure it sticks!
                                    for loop in range( applyDef-1, -1, -1):
                                        status = layer._gis._con.post(  layer.url + "/" + defAction, data)
                                        if loop:
                                            time.sleep(1)   # Pause between applications

                        except Exception as e:
                            status = e
                            if not verbose == False:
                                traceback.print_exc()
                                print( "\n * Error: '{}'".format( e))

                        if not (isinstance( status, dict) and status.get( "success", False)):
                            outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "restore layer '{}' properties".format( layerName), "result": str( status)})
                            outcome[ "success"] = False     # Set as error
                            if not verbose == False:
                                _prints( "\r * Failed to Restore {} properties: '{}', Elapsed Time: {}".format( defTitle, status, datetime.datetime.now() - start), status.get( "progressLength", 0) if hasattr( status, "get") else 0)
                        elif verbose:
                            _prints( "\r - {} Details Restored! Elapsed Time: {}".format( defTitle, datetime.datetime.now() - start), status.get( "progressLength", 0) if hasattr( status, "get") else 0)

                    # Report if no changes made
                    if noChangesApplied and verbose:
                        print( " * No property updates required!")

            # Strip unchanged Service properties
            start = datetime.datetime.now()
            changes = []
            for key, (backupKey, backupValue) in serviceProperties.copy().items():
                managerValue = _getRecursiveKey( manager.properties, key)
                if str( managerValue) == str( backupValue):
                    del serviceProperties[ key]
                else:
                    changes.append( "    Key: '{}'\n   From: '{}'\n     To: '{}'".format( key, managerValue, backupValue))

            # Create Dictionary of remaining key:value pairs in Service
            serviceProperties = dict( serviceProperties.values())

            # Restoring Service properties that may have been lost during Overwrite!
            if verbose:
                print( "\nRestoring Service Properties" if serviceProperties else "\nTouching Service Properties")

            if dryRun:
                if verbose:
                    print( " * Dry Run * No Change!")
                status = {"success": True}
            else:
                #status = manager.update_definition( serviceProperties)
                # Async required to retain 'Cache Control' settings!
                status = _asyncJob( manager, "updateDefinition", { "f": "json", "async": True, "updateDefinition": json.dumps(obj=serviceProperties, separators=(',', ':'))}, verbose=True if maxVerbose else (None if verbose else False))
            if not (status and isinstance( status, dict) and "success" in status and status[ "success"]):
                if not verbose == False:
                    _prints( "\r * Failed to Restore Service Properties, Status: {}".format( status), status.get( "progressLength", 0) if hasattr( status, "get") else 0)
                outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "restore service properties", "result": str( status)})
                outcome[ "success"] = False     # Set as error
            elif verbose:
                if changes:
                    _prints( "\r - Properties Restored, Elapsed Time: {}".format( datetime.datetime.now() - start), status.get( "progressLength", 0) if hasattr( status, "get") else 0)
                    print( "\n".join( changes))
                else:
                    _prints( "\r - Success! Elapsed Time: {}".format( datetime.datetime.now() - start), status.get( "progressLength", 0) if hasattr( status, "get") else 0)
                #
                # Service Restoration Complete!
                #
                print( "\nService Properties Restored, Elapsed Time: {}".format( datetime.datetime.now() - startRestore))

        # Strip unchanged Item properties
        start = datetime.datetime.now()
        item = item._gis.content.get( item.id)
        changes = []

        #print( "Before: ", itemProperties)

        #for key, (backupKey, backupValue) in itemProperties.copy().items():
        for key, backupValue in itemProperties.copy().items():
            #print( "Key: '{}', BackupKey: '{}', BackupValue: '{}'".format( key, backupKey, backupValue))
            itemValue = _getRecursiveKey( item, key)
            if str( itemValue) == str( backupValue):
                del itemProperties[ key]
            else:
                changes.append( "    Key: '{}'\n   From: '{}'\n     To: '{}'".format( key, itemValue, backupValue))

        # Create Dictionary from remaining key:value pairs in Item
        #itemProperties = dict( itemProperties.values())

        #print( "After: ", itemProperties)

        # Check for changes to Item Data
        itemDataStr = json.dumps( itemData)
        if json.dumps( item.get_data()) == itemDataStr:  # Compare current data value to backup
            itemDataStr = ""    # Clear backup if equal, no updates required!

        # Restoring Item properties that may have been lost during Overwrite!
        if itemProperties or itemDataStr:
            post = ""
            conj = ""
            params = {}
            if itemProperties:
                params["item_properties"] = itemProperties
                conj = " and "
                post = "Properties"
            if itemDataStr:
                params["data"] = itemDataStr
                post += conj + "Data"

            if not verbose == False:
                print( "\nRestoring Item " + post)
                if verbose:
                    if itemDataStr:
                        print( " - Item data will be: '{}'".format( itemDataStr))
                    else:
                        print( " - Item data will not change!")

            # Apply Item property changes three times to make sure they stick!
            for loop in range( 2, -1, -1):
                if dryRun:
                    if verbose:
                        print( " * Dry Run * No Change!")
                    status = True
                else:
                    status = item.update( **params)
                if loop:
                    time.sleep(1)   # Pause between applications

            if not (status == True):
                if not verbose == False:
                    print( " * Failed to Restore Item Properties, Status: {}".format( status))
                outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "restore item properties", "result": str( status)})
                outcome[ "success"] = False     # Set as error
            elif verbose:
                print( " - {} Restored, Elapsed Time: {}".format( post, datetime.datetime.now() - start))
                print( "\n".join( changes))

        # Restoring Item Thumbnail if lost during Overwrite
        item_thumbnail = item.thumbnail
        if itemThumbnail.get( "file_name") and not itemThumbnail.get( "file_name", "") == item_thumbnail:
            status = item.update_thumbnail( **itemThumbnail)
            if not (status == True):
                if not verbose == False:
                    print( " * Failed to Restore Item Thumbnail, Status: {}".format( status))
                outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "restore item thumbnail", "result": str( status)})
                outcome[ "success"] = False     # Set as error
            elif verbose:
                print( "    Key: '{}'\n   From: '{}'\n     To: '{}'".format( "Thumbnail", item_thumbnail, itemThumbnail.get( "file_name")))


    # Second Restore Related items attempt for Views
    if setRelated or (relatedItems and not len( relatedItems) == len( [relItem.id for relItem in item.related_items( "Service2Service", "reverse")])):
        if verbose:
            print( "\n * Relationship count mismatch, updating!")
        outcomeCount = len( outcome[ "items"])
        updateRelationships( item, relatedItems, verbose=verbose, outcome=outcome, dryRun=dryRun)
        if not outcomeCount == len( outcome[ "items"]):
            # Error detected, set condition, saving backup file!
            if not noProps:
                preserveProps = True    # Save Backup File
            #outcome[ "success"] = False

    # Manage retention of Item/Service Properties Backup File
    if backupFile:
        if not outcome[ "success"] == False and not preserveProps:
            if verbose:
                print( " - Dropping Backup{1} File: '{0}'".format( backupFile, ", Property Preservation DISABLED," if not preserveProps else ""))
            os.remove( backupFile)
        elif verbose:
            print( " * Retaining Backup{1} File: '{0}'".format( backupFile, ", Property Preservation in effect," if preserveProps else ""))

    if not verbose == False:
        if not noProps:
            print( "\n{} Property Restoration! Total Elapsed Time: {}".format( "Successfully Completed" if not outcome[ "success"] == False else " * Failed to Complete", datetime.datetime.now() - startRestore))

    return item

def _checkView( view, verbose=None, outcome=None, hadError=False, dryRun=False, outPath=""):
    """Internal Function: _checkView( <view item>, verbose=True, dryRun=False)

Check View for issues, fix/repair as needed.
"""
    if not outcome:
        outcome = { "success": None, "items": []}

    addLayersFile = os.path.join( tempfile.gettempdir() if not outPath else outPath, "{}_addLayers.json".format( view.id))
    relatedItems = view.backupRelationships if hasattr( view, "backupRelationships") else []

    # Double check Layer/Table count!
    viewDetails = view._gis._con.post(  view.url, {"f": "json"})

    # Check status of View Layers
    if os.path.exists( addLayersFile) and not (view.layers or viewDetails["layers"] or view.tables or viewDetails["tables"]):
        # View is missing Layers, fix before allowing Overwrite action!
        addLayers = json.load( open( addLayersFile, "r"))
        viewManager = _getManager( view, verbose=verbose, outcome=outcome)

        if isinstance( addLayers, dict) and "layers" in addLayers and addLayers[ "layers"]:
            if not verbose == False:
                print( "\n * View is missing its Layers, restoring!")
            start = datetime.datetime.now()

            try:
                if dryRun:
                    if verbose:
                        print( " * Dry Run * No Change!")
                    status = {"success": True}
                else:
                    data = { "f": "json", "async": False, "addToDefinition": json.dumps(obj=addLayers, separators=(',', ':'))}
                    status = viewManager._gis._con.post(  viewManager.url + "/" +"addToDefinition", data)

            except Exception as e:
                status = e
                if not verbose == False:
                    traceback.print_exc()
                    print( "\n * Error: '{}'".format( e))

            if status and isinstance( status, dict) and "success" in status and status[ "success"]:
                if not verbose == False:
                    print( " - Success! Elapsed Time: {}".format( datetime.datetime.now() - start))
            else:
                outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "restore service layers", "result": status})
                hadError = True
                if not verbose == False:
                    print( "\n * Failed to restore Layers, error: '{}'\n".format( status))

        _refreshManager( viewManager)   # Refresh with updated details

    # Check for missing Relationships, Views only
    if relatedItems:
        relatedServices = [relItem.id for relItem in view.related_items( "Service2Service", "reverse")]
        if len( relatedItems) > len( relatedServices):
            if verbose:
                print( "\n * View is missing one or more Related Services, restoring!")
            updateRelationships( view, relatedItems, verbose=verbose, dryRun=dryRun)
        elif set( relatedItems) != set( relatedServices):
            if verbose:
                print( "\n * Updating related service ids in backup, a change was detected on View!")
            view.backupRelationships = relatedServices

    return hadError

def _importConverter( moduleName):
    import importlib

    # Get path to This script, add location to import path if not already available
    filePath, scriptFile = os.path.split( os.path.realpath(__file__))

    #
    # Add Converter Module folders to import path
    for folder in [filePath] + [os.path.join( filePath, converterFolder)] + [os.path.join( filePath, modPath) for modPath in moduleName.split(".")[:-1]]:
        if folder not in sys.path:
            sys.path.append( folder)

    mod = None
    try:
        mod = importlib.import_module( moduleName)
        if not hasattr( mod, "convert"):
            raise Exception( "Module is missing 'convert' function")

        if not mod.convert.__code__.co_argcount:
            raise Exception( "Function 'convert' has NO parameters, minimum of 1 is required")

    except Exception as e:
        raise Exception( "Failed to import module '{}', Error: '{}'".format( moduleName, e))

    return mod

def updateRelationships( view, relateIds=[], unRelate=False, verbose=None, outcome=None, dryRun=False):
    """Function: updateRelationships( <view>[, <relateIds>[, <unRelate>[, <verbose>[, <outcome>[, <dryRun>]]]]])

    Set or Remove Relationships between one or more Feature Services and a Feature View. Leveraged by A/B
    enabled Feature Views for SwapLayer action (re-pointing a View's Layers from one Feature Service to another).

Return: <outcome> Dictionary object updated with error results if issue.
    Outcome structure: {
                           "success": True = Made an update / False = Failure encountered / None = No changes made,
                            "items": [
                                {
                                    "id": <item id>,
                                    "title": <item title>,
                                    "itemType": <item type>,
                                    "action": <operation>,
                                    "result": <details>
                                }
                            ]
                        }

           <view>: (required) The Hosted Feature View 'arcgis.gis.item' object you wish to update. This is the
                              Feature Service View that will have its Layers Swapped (or re-pointed) to a
                              different Feature Service based on its related Services.

      <relateIds>: (optional) String or List of Strings containing Item Ids of Feature Items you wish to
                              relate <view> to.
                              Default: Display Current Related Items

       <unRelate>: (optional) True or False, used to indicate whether to Add or Remove (unrelate) <relateIds>.
                              Can also be a String Relationship Type to bulk remove (<relateIds> is then ignored).
                              See documentation link 'https://bit.ly/2LAHNoK' for available types.
                              Default: False, Add Relationships

        <verbose>: (optional) True to Display step by step progress actions and results
                              False to Display nothing
                              Default: None, just Display major progress and error results.

        <outcome>: (optional) Dictionary object to update with results.
                              Should be formatted as { "success": None, "items": []}
                                                       'success' will contain 'False' if critical failure,
                                                       'items' will contain
                              dictionary of details.
                              Default: None

         <dryRun>: (optional) True or False, indicate that no update to a Service or Item should be done,
                              just go through the motions without making a change!
                              Default: False, Touch or Update the Service and Item.
"""

    if not outcome:
        outcome = { "success": None, "items": []}

    if not isinstance( relateIds, list):
        # Make it a list if not already
        relateIds = [relateIds]

    if unRelate and isinstance( unRelate, str):
        # Un-relate all items by Type
        direction = "forward" if unRelate == "Service2Data" else "reverse"
        related = view.related_items( unRelate, direction)
        removed = []
        while related:
            for item in related:
                if item in removed:
                    # If Initial removal fails, trap and report issue!
                    if not verbose == False:
                        print( " * Failed to Remove Relationship to Item: {}, '{}'".format( item.id, item.title))
                    outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "remove relationship", "result": "Failed to Remove Relationship to Item: {}, '{}'".format( item.id, item.title)})
                    related = False
                    break

                if not verbose == False:
                    print( " - Removing Relationship to item: {}, '{}'".format( item.id, item.title))

                status = "Unknown"
                if direction == "reverse":
                    try:
                        if dryRun:
                            if verbose:
                                print( " * Dry Run * No Change!")
                            status = True
                        else:
                            status = item.delete_relationship( view, unRelate)  # Delete Relationship of View in Item
                    except Exception as e:
                        status = str( e)
                else:
                    try:
                        if dryRun:
                            if verbose:
                                print( " * Dry Run * No Change!")
                            status = True
                        else:
                            status = view.delete_relationship( item, unRelate)  # Delete Relationship of Item in View
                    except Exception as e:
                        status = str( e)

                if not status == True:
                    if not verbose == False:
                        print( " * Failed to Remove Relationship to Item: {}, '{}', Outcome: {}".format( item.id, item.title, status))
                    outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "remove relationship", "result": "Failed to Remove Relationship to Item: {}, '{}', Outcome: {}".format( item.id, item.title, status)})
                    status = False

                if outcome[ "success"] == None or not status:
                    outcome[ "success"] = status

                removed.append( item)

            if related:
                related = view.related_items( unRelate, direction)

    elif relateIds:
        # Add or Remove related Items from View
        children = [item.id for item in view.related_items( "Service2Service", "forward")]  # Create Child Lookup
        parents = [item.id for item in view.related_items( "Service2Service", "reverse")]   # Create Parent Lookup
        data = [item.id for item in view.related_items( "Service2Data", "forward")]         # Create Data Lookup
        related = children + parents
        # Service2Data types:
        #dataTypes = ['GeoJson', 'Feature Collection', 'Microsoft Excel', 'File Geodatabase', 'Shapefile', 'CSV', 'Service Definition']

        status = None
        for itemId in relateIds:
            item = view._gis.content.get( itemId)   # Leverage View's arcgis API GIS connection to load Item for Relate operation
            if not item:
                if not verbose == False:
                    print( " * Failed to find specified Item: {}".format( itemId))
                outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "remove relationship", "result": "Failed to find specified Item: {}".format( itemId)})
                outcome[ "success"] = False
                continue

            if unRelate:
                if not itemId in related:
                    if verbose:
                        print( " * Ignored * Remove Relationship, View not related to Item: {}, '{}'".format( item.id, item.title))
                    continue

                # Remove relationship
                if itemId in children:
                    if not verbose == False:
                        print( " - Removing Child Relationship to Item: {}, '{}'".format( item.id, item.title))

                    try:
                        if dryRun:
                            if verbose:
                                print( " * Dry Run * No Change!")
                            status = True
                        else:
                            status = view.delete_relationship( item, "Service2Service")
                    except Exception as e:
                        status = str( e)

                    if not status == True:
                        if not verbose == False:
                            print( " * Failed to Remove Relationship to Item: {}, '{}', Outcome: {}".format( item.id, item.title, status))
                        outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "remove relationship", "result": "Failed to Remove Relationship to Item: {}, '{}', Outcome: {}".format( item.id, item.title, status)})
                        status = False

                if itemId in parents:
                    if not verbose == False:
                        print( " - Removing Parent Relationship to Item: {}, '{}'".format( item.id, item.title))

                    try:
                        if dryRun:
                            if verbose:
                                print( " * Dry Run * No Change!")
                            status = True
                        else:
                            status = item.delete_relationship( view, "Service2Service")
                    except Exception as e:
                        status = str( e)

                    if not status == True:
                        if not verbose == False:
                            print( " * Failed to Remove Relationship to Item: {}, '{}', Outcome: {}".format( item.id, item.title, status))
                        outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "remove relationship", "result": "Failed to Remove Relationship to Item: {}, '{}', Outcome: {}".format( item.id, item.title, status)})
                        status = False

            else:
                addRelationship, addLookup, addDirection = ("Service2Data", data, "reverse") if item.type in dataItemTypes else ("Service2Service", children, "forward")
                # Add relationship
                if addRelationship == "Service2Data" and (data or item.related_items( "Service2Data", "reverse")):
                    if verbose:
                        if data:
                            print( " * Ignored * Add Relationship, Service already Related to a Data Item!")
                        else:
                            print( " * Ignored * Add Relationship, Data Item already Related to a Service!")
                elif itemId not in addLookup:
                    if verbose:
                        print( " - Adding Relationship to Item: {}, '{}'".format( item.id, item.title))

                    try:
                        if dryRun:
                            if verbose:
                                print( " * Dry Run * No Change!")
                            status = True
                        else:
                            status = item.add_relationship( view, addRelationship) if addDirection == "forward" else view.add_relationship( item, addRelationship)
                    except Exception as e:
                        status = str( e)

                    if not status == True:
                        if not verbose == False:
                            print( " * Failed to Add Relationship to Item: {}, '{}', Outcome: {}".format( item.id, item.title, status))
                        outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "add relationship", "result": "Failed to Add Relationship to Item: {}, '{}', Outcome: {}".format( item.id, item.title, status)})
                        status = False

                elif verbose:
                    print( " * Ignored * Add Relationship, View already Related to Item: {}, '{}'".format( item.id, item.title))

            if outcome[ "success"] == None or not status:
                outcome[ "success"] = status

    else:
        # List relationships
        if verbose:
            relationships = 0
            dataItems = []
            for index, (itemText, viewText, relationshipType, direction) in enumerate([("Relies on Data Item", "Relies on Data Item", "Service2Data", "forward"), ("Is Relied on by Item", "Is Relied on by Item", "Service2Data", "reverse"), ("Is Child to Item", "Is Child to View", "Service2Service", "reverse"), (" Is Parent to Item", " Is Parent to View", "Service2Service", "forward")]):
                items = view.related_items( relationshipType, direction)
                for item in items:
                    if item in dataItems:
                        continue

                    isView = "View Service" in item.typeKeywords
                    print( "{: >22}: {}, '{}' ({}{})".format( viewText if isView else itemText, item.id, item.title, item.type, " View" if isView else ""))
                    relationships += 1

                if not index:
                    dataItems = items

            if not relationships:
                print( "\n * No Relationships found!")

    return outcome

def getFeatureServiceTarget( view, verbose=None, outcome=None, ignoreDataItemCheck=False):
    """Function: getFeatureServiceTarget( <view>[, <verbose>[, <outcome>[, <ignoreDataItemCheck>]]])

    Identifies Idle Service Item target for Multi-Service enabled Views, supporting Swap Layers workflow.

Returns: Feature Service Item and Data File details related to inactive A/B Feature Service related to <view>.
    Output structure: {"view": <related item>, "service": <item>, "filename": <string>, "fileType": <string>}

Or

Returns: <outcome> Dictionary object updated with error results if issue.
    Outcome structure: {
                           "success": True = Made an update / False = Failure encountered / None = No changes made,
                           "items": [
                               {
                                   "id": <item id>,
                                   "title": <item title>,
                                   "itemType": <item type>,
                                   "action": <operation>,
                                   "result": <details>
                               }
                           ]
                       }

                   <view>: (required) The Hosted Feature View 'arcgis.gis.item' object you wish to update.
                                      This is the Feature Service View that will have its Layers Swapped or re-
                                      pointed to the Target Feature Service based on available related Services.

                <verbose>: (optional) True to Display step by step progress actions and results
                                      False to Display nothing
                                      'max' to Display Maximum Diagnostic detail
                                      Default: None, just display major progress and error results.

                <outcome>: (optional) Dictionary object to update with results. Should be formatted as
                                      { "success": None, "items": []}
                                      'success' will contain 'False' if critical failure,
                                      'items' will contain dictionary of details.
                                      Default: None

    <ignoreDataItemCheck>: (optional) True to ignore verification check that target services have an associated
                                      data file item.
                                      False to verify data file item exists for target services.
                                      Default: False
"""
    maxVerbose = "{}".format( verbose).lower() == "max"

    if not outcome:
        outcome = { "success": None, "items": []}

    if not verbose == False:    # Display unless told not to
        print( "\nAcquiring Target Feature Service...")

    # Initially set Last Modified Date
    setattr( view, "serviceLastModified", 0)

    #
    # Verify view Item is of Type Feature Service and get Layer Manager
    #
    viewManager = _getManager( view, verbose=verbose, outcome=outcome)
    if not outcome["success"] == False:
        #
        # Verify Feature Service item is actually a view
        #
        if not (hasattr( viewManager, "properties") and hasattr( viewManager.properties, "isView") and viewManager.properties.isView):
            outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "verify", "result": "Feature Service Item is NOT a 'View'"})
            outcome[ "success"] = False     # Set as error
        else:
            #
            # Verify Service has required Related Parent Views
            #
            views = []
            services = []
            for target in view.related_items( "Service2Service", "reverse"):
                if "View Service" in target.typeKeywords:
                    views.append( target)
                elif "Feature Service" in target.typeKeywords:
                    services.append( target)

            if len( views) < 2 and services:
                # Use related Feature Views first and fallback to Feature Services
                views = services

            if not len( views) == 2:
                outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "verify", "result": "Feature Service View requires 2 Related Feature Services or 2 Related Feature Views, found: {}".format( len( views))})
                outcome[ "success"] = False     # Set as error
            else:
                #
                # Verify Related Views/Services have a data source
                #
                targets = []
                for relatedView in views:
                    serviceItem = [relatedView]
                    if "View Service" in relatedView.typeKeywords:
                        serviceItem = relatedView.related_items( "Service2Data")
                        if not serviceItem:
                            outcome[ "items"].append( {"id": relatedView.id, "title": relatedView.title, "itemType": relatedView.type, "action": "verify", "result": "Related View does NOT contain a Data Source"})
                            outcome[ "success"] = False     # Set as error
                            continue

                    serviceItem = serviceItem[0]
                    #
                    # Verify related Data Source (Feature Service) has a file Data Source
                    #
                    fileItem = serviceItem.related_items( "Service2Data")
                    if not (fileItem or ignoreDataItemCheck):
                        outcome[ "items"].append( {"id": serviceItem.id, "title": serviceItem.title, "itemType": serviceItem.type, "action": "verify", "result": "Target Feature Service '{}' is missing a file Data Source".format( serviceItem.title)})
                        outcome[ "success"] = False     # Set as error
                    else:
                        targets.append( {"view": relatedView, "service": serviceItem, "filename": fileItem[0].name if fileItem else "N/A", "fileType": fileItem[0].type if fileItem else "N/A"})

                #
                # Match Target
                #
                if not targets:
                    outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "verify", "result": "No Data Service Target available"})
                    outcome[ "success"] = False     # Set as error
                else:
                    currentService = "* Unknown or NOT Set - Targeting First Available *"

                    try:
                        # Do we have a current service?
                        layer = viewManager.properties.layers[0]
                        currentService = "/" + layer.adminLayerInfo.viewLayerDefinition.sourceServiceName + "/FeatureServer"

                        # Grab Last Edit Date, save to view
                        serviceLastModified = layer.get( "editingInfo", {}).get( "lastEditDate", 0)
                        serviceLastModified = 0 if not serviceLastModified else datetime.datetime.utcfromtimestamp( int( serviceLastModified / 1000))
                        setattr( view, "serviceLastModified", serviceLastModified)
                    except:
                        try:
                            # Try using Related Data Service
                            relatedItem = view.related_items( "Service2Data")[0]
                            currentService = "/".join( [''] + relatedItem.url.split("/")[-2:])
                        except:
                            pass

                    if verbose:
                        print( " -         Current Feature Service: {}".format( currentService))

                    for target in targets:
                        # Grab first target that does not match the current!
                        if not target[ "service"].url.endswith( currentService):
                            if verbose:
                                print( " -  Target Feature Service Item Id: {}".format( target[ "service"].id))
                                print( " -    Target Feature Service Title: '{}'".format( target[ "service"].title))
                                print( " - Target Feature Service Filename: '{}' ({})".format( target[ "filename"], target[ "fileType"]))
                            return target
    else:
        if not verbose == False:
            print( "\n * No Target Feature Service Available!")

    return outcome

def swapFeatureViewLayers( view, updateFile=None, touchItems=True, verbose=None, touchTimeSeries=True, outcome=None, noIndexes=False, preserveProps=True, noWait=False, noProps=False, converter=None, outPath="", dryRun=False, noSwap=False, ignoreAge=False, byLayerOrder=False):
    """Function: swapFeatureViewLayers( <view>[, <updateFile>[, <touchItems>[, <verbose>[, <touchTimeSeries>[, <outcome>[, <noIndexes>[, <preserveProps>[, <noWait>[, <noProps>[, <converter>[, <outPath>[, <dryRun>[, <noSwap>[, <ignoreAge>]]]]]]]]]]]]]])

    Overwrite the inactive Feature Service (when <updateFile> specified) and/or Swap Layers in specified View to
    point to newly updated Feature Service. Used by A/B View enabled Services whereby the View's Layers are pointed
    to the newly updated Feature Service View's Layers.

    If updateFile is not included, this function will switch Layers of a View to access inactive Feature Service.

Returns: <output> Dictionary object containing update success status plus a list of Items and their altered status.
    Outcome structure: {
                           "success": True = Made an update / False = Failure encountered / None = No changes made,
                           "items": [
                               {
                                   "id": <item id>,
                                   "title": <item title>,
                                   "itemType": <item type>,
                                   "action": <operation>,
                                   "result": <details>
                               }
                           ]
                       }

Or

    Exception is raised when a critial obsticle is reached.

               <view>: (required) The Hosted Feature View 'arcgis.gis.item' object you wish to update. This is the
                                  Feature Service View that will have its Layers Swapped (or re-pointed) to a
                                  different Feature Service.

         <updateFile>: (optional) The File Path and/or Name of the file, or URL, to overwrite Service data with.
                                  Default: None, only 'Touch' the Feature Service Item if allowed.

         <touchItems>: (optional) 'Touch' Feature Service Item (if no <updateFile>) and related Views, to refresh
                                  last modified date?
                                  Default: True

            <verbose>: (optional) True to Display step by step progress actions and results
                                  False to Display nothing
                                  'max' to Display Maximum Diagnostic detail
                                  Default: None, just Display major progress and error results.

    <touchTimeSeries>: (optional) 'Touch' Time Series enabled Layers in Feature Service and related Views,
                                  to refresh time extent.
                                  Default: True

            <outcome>: (optional) Dictionary object to update with results.
                                  Should be formatted as { "success": None, "items": []}
                                                           "success" will contain False if critical failure,
                                                           "items" will contain dictionary of details.
                                  Default: None

          <noIndexes>: (optional) True or False, ignore missing field indexes on service during property restoration.
                                  Default: False, recreate missing indexes if possible

      <preserveProps>: (optional) True or False, indicate if Service and View properties should be Preserved as
                                  a backup 'Snapshot' file following a successful overwrite and property restoration.
                                  If False, backup 'Snapshot' file will be removed on successful property restoration.
                                  * Caution * If set to True, 'preserveProps' setting CANNOT be set to True!
                                  Default: True, properties will be used for all updates that follow,
                                           regardless of post property alterations.

             <noWait>: (optional) True or False, instruct property restore function not to wait for re-application of
                                  properties like Layer Optimization to complete before continuing the Overwrite
                                  action. When enabled, function will report condition and supply a URL in the Outcome
                                  that can be used for manual status review.
                                  Default: False, property restoration function will wait for other properties like
                                           Layer Optimization to be re-applied before proceeding to the next
                                           processing 'step' in the workflow.

            <noProps>: (optional) True or False, indicate that NO Service or View Properties should be applied
                                  following a successful update.
                                  * Caution * If set to True, 'preserveProps' setting CANNOT be set to True!
                                  Default: False, properties will be restored following a successful update.

          <converter>: (optional) String name of Conversion Module to import, or List containing Module name and input
                                  parameters as needed. The 'convert' function in this module will be used to convert
                                  the <updateFile> prior to running the Overwrite process.
                                  Ex. 'Xml2GeoJSON' as is, or ['Xml2GeoJSON', 'False'] to ignore Publishing date. This
                                  reads RSS data and converts it to Json Featureset prior to Overwriting a Hosted
                                  Feature Service built using the Json data. Module is relative to 'Converters' folder
                                  in OverwriteFS script home location. Module name can be Dot notation <path>.<module>
                                  Default: No conversion will take place.

            <outPath>: (optional) String containing file system folder Path to use as the output location for
                                  <updateFile> URL file download and Service property backup files.
                                  Default: Store output in User's Temporary folder.

             <dryRun>: (optional) True or False, indicate that no update to a Service or Item should be done,
                                  just go through the motions without making a change!
                                  Default: False, Touch or Update the Service and Item.

             <noSwap>: (optional) True or False, indicate update of Target Service only, if <updateFile> included.
                                  No Layer Swap is attempted. A QA/QC workflow step. Re-Initiate Swap when validated!
                                  Default: False, Touch or Update the Service and Item.

          <ignoreAge>: (optional) True or False, instructing function to ignore <url> download age checks, updating
                                  Service without checking age of downloaded data.
                                  Default: False, cancel Service update when <url> data is older than last Service
                                           update.

       <byLayerOrder>: (optional) True or False, when True, instructs function to map Target Layers by the order they
                                  appear Layer list.
                                  Default: False, map Target Layers by View layer's sourceLayerId.
"""
    maxVerbose = "{}".format( verbose).lower() == "max"

    if not outcome:
        outcome = { "success": None, "items": []}

    if not verbose == False:
        print( "\nInvoking Feature View Layer Swap Workflow{}...".format( " (Target update ONLY!)" if noSwap else ("" if not byLayerOrder else " (by Layer Order)")))

    # Verify Properties
    if (preserveProps and noProps):
        outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "verify", "result": "Mutually exclusive parameters are set, cannot Preserve and Ignore Service/View Properties!"})
        outcome[ "success"] = False     # Set as error

        return outcome

    if outPath:
        if not os.path.exists( outPath):
            outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Unable to locate specified output path: '{}'!".format( outPath)})
            outcome[ "success"] = False     # Set as error

            return outcome

        elif not os.path.isdir( outPath):
            outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Specified output path: '{}', is NOT a Folder!".format( outPath)})
            outcome[ "success"] = False     # Set as error

            return outcome

    # Load Converter if required
    if converter:
        if not isinstance( converter, list):
            # Make it a List
            converter = [converter]

        if isinstance( converter[0], str):
            try:
                converter[0] = _importConverter( converter[0])

                # Validate Arguments, all come in as strings
                argCount = converter[0].convert.__code__.co_argcount
                argNames = converter[0].convert.__code__.co_varnames[1:argCount]    # excluding download filename

                if len( converter) > argCount:
                    if argCount > 1:
                        raise Exception( "Too many Parameters specified, {}, only need values for ('{}')".format( len( converter) - 1, "', '".join( argNames)))
                    else:
                        raise Exception( "No Parameters required, {} specified".format( len( converter) - 1))

                for index, value in enumerate( converter[1:]):
                    try:
                        if str( value).lower() in ["true", "false"]:
                            value = str( value).capitalize()
                        converter[ index + 1] = eval( str( value))

                    except Exception as e:
                        raise Exception( "Failed to evaluate Parameter '{}', Error: '{}'".format( argNames[ index], e))

            except Exception as e:
                outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "verify", "result": "Loading Converter '{}' Failed, Error: '{}'!".format( converter[0], e)})
                outcome[ "success"] = False     # Set as error

                return outcome

    # Validate View and make sure no restrictive dependants exist
    if not (view.type == "Feature Service" and "View Service" in view.typeKeywords):
        # Error out if we are trying to Swap Layers on something other than a View
        outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "verify", "result": "Item is NOT a Feature Service 'View', cannot Swap Layers!"})
        outcome[ "success"] = False     # Set as error

        return outcome

    for item in view.related_items( "Service2Service"):
        if item.type in ["WFS"]:    # "OGCFeatureServer",
            outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Swapping Layers is NOT allowed, a dependent WFS Service exists!"})
            outcome[ "success"] = False     # Set as error

            return outcome

    layerBackupFile = os.path.join( tempfile.gettempdir() if not outPath else outPath, "{}_Layers.json".format( view.id))
    addLayersFile = os.path.join( tempfile.gettempdir() if not outPath else outPath, "{}_addLayers.json".format( view.id))

    # Backup Item and Service properties
    _backupProperties( view, verbose=verbose, outcome=outcome, outPath=outPath)

    # Check status of View's Layers
    hadError = _checkView( view, verbose=verbose, outcome=outcome, dryRun=dryRun, outPath=outPath)

    #
    # Get Target Feature Service Details
    #
    target = getFeatureServiceTarget( view, verbose=verbose, outcome=outcome, ignoreDataItemCheck=False if updateFile else True)

    if "service" in target:
        viewIsService = target[ "view"] == target[ "service"]   # Is the Target View a Service?

        if updateFile:
            if os.path.isdir( updateFile):
                updateFile = os.path.join( updateFile, target[ "filename"])

            serviceLastModified = 0 if not hasattr( view, "serviceLastModified") else view.serviceLastModified
            setattr( target[ "service"], "returnUpdatedItem", True) # Tell overwriteFeatureService function to return the updated item object instead of the status
            target[ "service"] = overwriteFeatureService( target[ "service"], updateFile=updateFile, touchItems=touchItems, verbose=verbose, touchTimeSeries=touchTimeSeries, outcome=outcome, ignoreItems=view.id, serviceLastModified=serviceLastModified, noIndexes=noIndexes, preserveProps=preserveProps, noWait=noWait, noProps=noProps, converter=converter, outPath=outPath, dryRun=dryRun, ignoreAge=ignoreAge)
            if viewIsService:
                target[ "view"] = target[ "service"]

        if noSwap and updateFile:
            if not verbose == False:
                print( "\n * Target Service Update with No Layer Swap requested, ignoring Swap!")

        # If Successful Update or no update and no errors
        elif (updateFile and outcome[ "success"] == True) or not (updateFile or outcome[ "success"] == False):
            if verbose:
                print( "\nCollecting Feature View Details for Swap...")

            #
            # Swap Layers of Main View to match Layers of Target View
            #
            targetLayers = collections.OrderedDict() # Admin detail for each Layer in Target View
            targetManager = _getManager( target[ "view"], verbose=verbose, outcome=outcome)
            if outcome[ "success"] == False:
                return outcome

            viewLayers = {"layers": []} # Updated Layer details for Main View, used to Swap View Layers!
            viewManager = _getManager( view, verbose=verbose, outcome=outcome)
            if outcome[ "success"] == False:
                return outcome

            addLayers = {"layers": []}
            altLayers = {"layers": []}
            dropLayers = {"layers": []}
            serviceHasLayer = True
            fallbackLayers = {"layers": eval( viewManager.properties.get( "layers", []).__repr__())}

            # Collect Administrative details for All availble Layers
            for layer in targetManager.properties.get( "layers", []):
                if not "viewLayerDefinition" in layer[ "adminLayerInfo"]:
                    # From Feature Service
                    targetId = str( layer[ "id"])
                    targetLayers[ targetId] = {
                        "viewLayerDefinition": {
                            "sourceServiceName": targetManager.properties[ "adminServiceInfo"][ "name"],
                            "sourceLayerId": layer[ "id"],
                            "sourceLayerFields": "*"
                        }
                    }
                else:
                    # From Feature View
                    adminId = layer[ "adminLayerInfo"][ "viewLayerDefinition"][ "sourceLayerId"]
                    #targetLayers[ str( adminId)] = layer[ "adminLayerInfo"].copy()

                    targetId = str( adminId)
                    targetLayers[ targetId] = {
                        "viewLayerDefinition": {
                            "sourceServiceName": layer[ "adminLayerInfo"][ "viewLayerDefinition"][ "sourceServiceName"],
                            "sourceLayerId": adminId,
                            "sourceLayerFields": layer[ "adminLayerInfo"][ "viewLayerDefinition"][ "sourceLayerFields"]
                        }
                    }

                # Add common properties to Target Layer
                for property in [ "geometryField", "xssTrustedFields"]:
                    if property in layer[ "adminLayerInfo"]:
                        targetLayers[ targetId][ property] = layer[ "adminLayerInfo"][ property]

                # Add to Alternate add layers list
                altLayers[ "layers"].append({
                    "adminLayerInfo": targetLayers[ targetId],
                    "id": layer[ "id"],
                    "name": layer[ "name"]
                })

            # Set Layers based on View Service or Saved Layers file if no Layers available in service!
            layers = viewManager.properties.get( "layers", [])
            if not layers:
                serviceHasLayer = False
                if os.path.exists( layerBackupFile):
                    # Try loading from Layer backup file
                    try:
                        layers = json.load( open( layerBackupFile, "r"))
                    except Exception as e:
                        if not verbose == False:
                            print( " * Note * Failed to load 'Layers' backup file: '{}', Error: '{}'".format( layerBackupFile, e))

            if byLayerOrder and (len(layers) > len(targetLayers)):
                outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "swap layers", "result": "Layer count on Target is less than View"})
                outcome[ "success"] = False     # Set as error
            else:
                # Cycle through Main View layer list, extract and update Admin details for Layer Swap
                for layerIndex, layer in enumerate( layers):
                    adminId = str( layer[ "adminLayerInfo"][ "viewLayerDefinition"][ "sourceLayerId"] if not byLayerOrder else list(targetLayers.keys())[layerIndex])
                    dropLayers[ "layers"].append( {"id": layer[ "id"]}) # Add Layer to drop list
                    viewLayers[ "layers"].append( eval( layer.__repr__()))    # Save Layer details

                    if adminId in targetLayers:
                        # Update Layer's Admin details to point to Target Layer
                        #viewLayers[ "layers"][-1][ "adminLayerInfo"] = targetLayers[ adminId]
                        addLayers[ "layers"].append({
                            "adminLayerInfo": targetLayers[ adminId],
                            "id": layer[ "id"],
                            "name": layer[ "name"]
                        })
                        del viewLayers[ "layers"][-1][ "adminLayerInfo"]
                        if "editingInfo" in viewLayers[ "layers"][-1]:
                            viewLayers[ "layers"][-1][ "editingInfo"][ "lastEditDate"] = 0
                    else:
                        outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "swap layers", "result": "View Layer id '{}' has no matching 'sourceLayerId' of {} in Target.\n * TIP * Ensure Layer ids in Target match View, or enable 'by order' option.".format( layer[ "id"], adminId)})
                        outcome[ "success"] = False     # Set as error

            # Swap Layers in View if all is OK!
            if not outcome[ "success"] == False:
                if not verbose == False:
                    print( "\nStarting Swap Layer Process...")

                # Save addLayers detail
                if addLayers and addLayers["layers"]:
                    json.dump( addLayers, open( addLayersFile, "w"), indent=3, separators=(',', ': '))

                #if verbose:
                #    print( " - Refreshing Connection...")
                #viewManager._gis._con.relogin()

                # Backup Item and Service properties
                #_backupProperties( view, verbose=verbose, outcome=outcome)

                swapStart = datetime.datetime.now()
                start = datetime.datetime.now()

                # Drop Layers from Main View
                if not verbose == False:
                    print( " - Dropping Existing Layers...")

                status = {"success": None}
                #asyncCall = True if globals().get( "async") else False
                asyncCall = True

                if serviceHasLayer:
                    if dryRun:
                        if verbose:
                            print( " * Dry Run * No Change!")
                        status = {"success": True}
                    else:
                        data = { "f": "json", "async": asyncCall, "deleteFromDefinition": json.dumps(obj=dropLayers, separators=(',', ':'))}

                        if asyncCall:
                            status = _asyncJob( viewManager, "deleteFromDefinition", data, verbose=True if maxVerbose else (None if verbose else False))
                        else:
                            try:
                                status = viewManager._gis._con.post(  viewManager.url + "/" +"deleteFromDefinition", data)
                            except Exception as e:
                                status = e
                                if not verbose == False:
                                    traceback.print_exc()
                                    print( "\n * Error: '{}'".format( e))

                        _refreshManager( viewManager)
                else:
                    if verbose:
                        print( " * Nothing to drop, Layers list is empty!")
                    status = {"success": True}

                if not (status and isinstance( status, dict) and "success" in status and status[ "success"]):
                    outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "delete from service definition", "result": status})
                    outcome[ "success"] = False     # Set as error
                else:
                    if verbose and serviceHasLayer:
                        _prints( "\r   - Success! Elapsed Time: {}".format( datetime.datetime.now() - start), status.get( "progressLength", 0) if hasattr( status, "get") else 0)

                    # Remove View Item's Data relationship to Feature Service (View will auto-update with correct Data relationship)
                    #updateRelationships( view, unRelate="Service2Data", verbose=verbose, outcome=outcome)

                    # Add Updated Layer details back in!
                    for applyGroup, applyLayers in [[ "Adding Target", addLayers if viewLayers[ "layers"] else altLayers], [ "Restoring Original", fallbackLayers]]:
                        if not applyLayers:
                            continue

                        if not verbose == False:
                            print( " - {} Feature Service Layers...".format( applyGroup))

                        start = datetime.datetime.now()

                        try:
                            if dryRun:
                                if verbose:
                                    print( " * Dry Run * No Change!")
                                status = {"success": True}
                            else:
                                data = { "f": "json", "async": asyncCall, "addToDefinition": json.dumps(obj=applyLayers, separators=(',', ':'))}

                                if asyncCall:
                                    status = _asyncJob( viewManager, "addToDefinition", data, verbose=True if maxVerbose else (None if verbose else False))
                                else:
                                    status = viewManager._gis._con.post(  viewManager.url + "/" +"addToDefinition", data)

                                _refreshManager( viewManager)

                        except Exception as e:
                            status = e
                            if not verbose == False:
                                traceback.print_exc()
                                print( "\n * Error: '{}'".format( e))

                        if status and isinstance( status, dict) and "success" in status and status[ "success"]:
                            if verbose:
                                _prints( "\r   - Success! Elapsed Time: {}".format( datetime.datetime.now() - start), status.get( "progressLength", 0) if hasattr( status, "get") else 0)

                            if not verbose == False:
                                print( " - View now points to Feature Service: {}, '{}'".format( target[ "service"].id, target[ "service"].title))
                                print( "\nFeature View Layers Swapped, Total Elapsed Time: {}".format( datetime.datetime.now() - swapStart))

                            # Exit Add Layers!
                            break
                        else:
                            outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "add to service definition", "result": status})
                            hadError = True
                            if not verbose == False:
                                print( "\n * Failed {} Layers, error: '{}'\n".format( applyGroup, status))

                    if not (status and isinstance( status, dict) and "success" in status and status[ "success"]):
                        outcome[ "items"].append( {"id": view.id, "title": view.title, "itemType": view.type, "action": "add to service definition", "result": status})
                        outcome[ "success"] = False     # Set as error
                    else:
                        # Add successful, remove 'add' Layers file
                        if os.path.exists( addLayersFile):
                            os.remove( addLayersFile)

                        # Restore Backed up Service and Item properties
                        view = _restoreProperties( view, verbose=verbose, outcome=outcome, touchTimeSeries=touchTimeSeries, noIndexes=noIndexes, preserveProps=preserveProps, noWait=noWait, noProps=noProps, dryRun=dryRun)

                        _refreshManager( viewManager)

                        try:
                            start = datetime.datetime.now()
                            serviceDetails = viewManager._gis._con.post( viewManager.url, { "f": "json"})

                            if "layers" in serviceDetails:
                                # Save Layers to backup file
                                json.dump( serviceDetails["layers"], open( layerBackupFile, "w"), indent=3, separators=(',', ': '))

                                if not verbose == False:
                                    print( "\nLayer Details Backed up, not related to Preservation, Total Elapsed Time: {}".format( datetime.datetime.now() - start))

                        except Exception as e:
                            if not verbose == False:
                                traceback.print_exc()
                                print( " * Note * Failed to create 'Layers' backup file: '{}', Error: '{}'".format( layerBackupFile, e))

                if not verbose == False:
                    print( "\nFeature View Layer Swap {} Overall Elapsed Time: {}".format( "* Failed!" if outcome[ "success"] == False else "was Successful," if not hadError else "had Issues", datetime.datetime.now() - swapStart))

                if not outcome[ "success"] == False:
                    outcome[ "success"] = (hadError == False)

    return outcome

def overwriteFeatureService( item, updateFile=None, touchItems=True, verbose=None, touchTimeSeries=True, outcome=None, ignoreItems=[], serviceLastModified=0, noIndexes=False, preserveProps=True, noWait=False, noProps=False, converter=None, outPath="", dryRun=False, ignoreAge=False):
    """Function: overwriteFeatureService( <item>[, <updateFile>[, <touchItems>[, <verbose>[, <touchTimeSeries>[, <outcome>[, <ignoreItems>[, <serviceLastModified>[, <preserveProps>[, <noWait>[, <noProps>[, <converter>[, <outPath>[, <dryRun>[, <ignoreAge>]]]]]]]]]]]]]])

    Overwrites an Existing Feature Service with new Data matching Schema of data used during initial Publication.

    If updateFile is not included, this function will only touch the Service item to update its last modified date.

    * Note * If Views have been created that reference the Service, this function will also touch the View items to
             update their last mondified date.

Returns:
    <outcome> Dictionary object is updated with success status and an Item list of Items altered and their status.
    Outcome structure: {
                           "success": True = Made an update / False = Failure encountered / None = No changes made,
                           "items": [
                               {
                                   "id": <item id>,
                                   "title": <item title>,
                                   "itemType": <item type>,
                                   "action": <operation>,
                                   "result": <details>
                               }
                           ]
                       }

Or

    Exception is raised when a critial obsticle is reached.

               <item>: (required) The Hosted Feature Service 'arcgis.gis.item' object you wish to update.

         <updateFile>: (optional) The File Path and/or Name of the file, or URL, to overwrite Service data with.
                                  Default: None, only 'Touch' the Feature Service Item if allowed.

         <touchItems>: (optional) True or False, 'Touch' Feature Service Item (if no <updateFile>) and related Views,
                                  to refresh last modified date?
                                  Default: True

            <verbose>: (optional) True to Display step by step progress actions and results
                                  False to Display nothing
                                  'max' to Display Maximum Diagnostic detail
                                  Default: None, just Display major progress and error results.

    <touchTimeSeries>: (optional) True or False, 'Touch' Time Series enabled Layers in Feature Service and related
                                  Views, to refresh time extent.
                                  Default: True

            <outcome>: (optional) Dictionary object to update with results.
                                  Should be formatted as { "success": None, "items": []}
                                                           "success" will contain "False" if critical failure,
                                                           "items" will contain dictionary of details.
                                  Default: None

        <ignoreItems>: (optional) String or List of Strings containing Item Ids of Views to explicitly ignore
                                  'touch' actions on.
                                  Default: Empty list, no items are ignored

<serviceLastModified>: (optional) Long Integer as Unix epoch time of last modified time. To compare to download file.
                                  Default: 0

          <noIndexes>: (optional) True or False, ignore missing field indexes on service during property restoration.
                                  Default: False, recreate missing indexes if possible

      <preserveProps>: (optional) True or False, indicate if Service and View properties should be Preserved as a
                                  backup 'Snapshot' file following a successful overwrite and property restoration.
                                  If False, backup 'Snapshot' file will be removed on successful property restoration.
                                  * Caution * If set to True, 'noProps' setting CANNOT be set to True!
                                  Default: True, properties will be used for all updates that follow,
                                           regardless of post property alterations.

             <noWait>: (optional) True or False, instruct property restore function not to wait for re-application of
                                  properties like Layer Optimization to complete before continuing the Overwrite
                                  action. When enabled, function will report condition and supply a URL in the Outcome
                                  that can be used for manual status review.
                                  Default: False, property restoration will wait for other properties like Layer
                                           Optimization to be re-applied before proceeding to the next processing
                                           'step' in the workflow.

            <noProps>: (optional) True or False, indicate that NO Service or View Properties should be applied
                                  following a successful update.
                                  * Caution * If set to True, 'preserveProps' setting CANNOT be set to True!
                                  Default: False, properties will be restored following a successful update.

          <converter>: (optional) String name of Conversion Module to import, or List containing Module name and input
                                  parameters as needed. The 'convert' function in this module will be used to convert
                                  the <updateFile> prior to running the Overwrite process.
                                  Ex. 'Xml2GeoJSON' as is, or ['Xml2GeoJSON', 'False'] to ignore Publishing date. This
                                  reads RSS data and converts it to Json Featureset prior to Overwriting a Hosted
                                  Feature Service built using the Json data. Module is relative to 'Converters' folder
                                  in OverwriteFS script home location. Module name can be Dot notation <path>.<module>
                                  Default: No conversion will take place.

            <outPath>: (optional) String containing file system folder Path to use as the output location for
                                  <updateFile> URL file download and Service property backup files.
                                  Default: Store output in User's Temporary folder.

             <dryRun>: (optional) True or False, indicate that no update to a Service or Item should be done,
                                  just go through the motions without making a change!
                                  Default: False, Touch or Update the Service and Item.

          <ignoreAge>: (optional) True or False, instructing function to ignore <url> download age checks, updating
                                  Service without checking age of downloaded data.
                                  Default: False, cancel Service update when <url> data is older than last Service
                                           update.
"""

    def touchItem( item, message, outcome):
        if (not verbose == False) and message:
            print( message)

        status = None
        try:
            if dryRun:
                if verbose:
                    print( " * Dry Run * No Change!")
                status = True
            else:
                status = item.update()

            if not status == True:
                raise Exception( status)

            if verbose:
                print( " - Success!")

        except Exception as e:
            if not verbose == False:
                print( " * Failed to Touch details for Item Id: '{}', Outcome: '{}'".format( item.id, e))
            status = "Failed, Outcome: '{}'".format( e)
            status = status if not "error code" in status.lower() else status.replace( "\n", " ")

        outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "touch item", "result": status})
        return (status == True)

    def touchTimeInfo( item, message, outcome):
        if item.type in ["Vector Tile Service", "OGCFeatureServer", "WFS"]:
            # No need to touch Vector Tile, OGC, or WFS Layers
            return

        if (not verbose == False) and message:
            print( message)

        status = None
        layerName = ""
        asyncCall = True if globals().get( "async") else False

        try:
            #
            # Get Feature Layer Manager for item
            #
            manager = _getManager( item, verbose=verbose, outcome=outcome)
            if outcome[ "success"] == False:
                raise Exception( "Failed to Get Feature Layer Collection Manager")

            for layer in manager.layers:
                definition = {}
                layerName = layer.properties.get( "name", "")

                if "timeInfo" in layer.properties:
                    if verbose:
                        print( " * Touching Time Info for Layer: '{}'".format( layerName))

                    definition["timeInfo"] = layer.properties[ "timeInfo"]

                if definition:
                    if dryRun:
                        if verbose:
                            print( " * Dry Run * No Change!")
                        status = { "success": True}
                    else:
                        data = { "f": "json", "async": asyncCall, "updateDefinition": json.dumps(obj=definition, separators=(',', ':'))}
                        if asyncCall:
                            status = _asyncJob( layer, "updateDefinition", data, verbose=None if verbose else False)
                        else:
                            status = layer._gis._con.post(  layer.url + "/" + "updateDefinition", data)

                    if not (isinstance( status, dict) and status.get( "success", False)):
                        raise Exception( status)

                    if verbose:
                        _prints( "\r - Success!", status.get( "progressLength", 0) if hasattr( status, "get") else 0)
                    status = "Success"

            # Sync View Properties if needed!
            if status is None:
                if verbose:
                    print( " * No Time Series enabled Layers found!")
                    print( " - Syncing View Properties...")

                if dryRun:
                    if verbose:
                        print( " * Dry Run * No Change!")
                    status = { "success": True}
                else:
                    data = { "f": "json", "async": asyncCall, "updateDefinition": "{}"}
                    if asyncCall:
                        status = _asyncJob( manager, "updateDefinition", data, verbose=None if verbose else False)
                    else:
                        status = layer._gis._con.post(  manager.url + "/" + "updateDefinition", data)

                if not (isinstance( status, dict) and status.get( "success", False)):
                    raise Exception( status)

                if verbose:
                    _prints( "\r - Success!", status.get( "progressLength", 0) if hasattr( status, "get") else 0)
                status = "Success"

        except Exception as e:
            if not verbose == False:
                traceback.print_exc()
                print( "\n * Failed to Update Service details for Item Id: '{}'{}, Outcome: '{}'".format( item.id, ", Layer: '{}'".format( layerName) if layerName else "", e))
            status = "Failed, Outcome: '{}'".format( e)
            status = status if not "error code" in status.lower() else status.replace( "\n", " ")

        outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "touch time info", "result": status})
        return (status == "Success")

    ###########################
    # Start of Function Logic #
    ###########################

    maxVerbose = "{}".format( verbose).lower() == "max"

    isFileItem = (item.type in fileItemTypes) or (item.name and "." in item.name and item.type != "Feature Service") # is allowed type or is a filename in item?

    if not outcome:
        outcome = { "success": None, "items": []}

    if (not verbose == False) and updateFile:
        print( "\nInvoking Overwrite {}...".format( "File Item" if isFileItem else "Feature Service"))

    # Verify Properties
    if (preserveProps and noProps) and not isFileItem:
        outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Mutually exclusive parameters are set, cannot Preserve and Ignore Service/View Properties!"})
        outcome[ "success"] = False     # Set as error

        return outcome

    if outPath:
        if not os.path.exists( outPath):
            outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Unable to locate specified output path: '{}'!".format( outPath)})
            outcome[ "success"] = False     # Set as error

            return outcome

        elif not os.path.isdir( outPath):
            outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Specified output path: '{}', is NOT a Folder!".format( outPath)})
            outcome[ "success"] = False     # Set as error

            return outcome

    # Load Converter if required
    if converter:
        if not isinstance( converter, list):
            # Make it a List
            converter = [converter]

        if isinstance( converter[0], str):
            try:
                converter[0] = _importConverter( converter[0])

                # Validate Arguments, all come in as strings
                argCount = converter[0].convert.__code__.co_argcount
                argNames = converter[0].convert.__code__.co_varnames[1:argCount]    # excluding download filename

                if len( converter) > argCount:
                    if argCount > 1:
                        raise Exception( "Too many Parameters specified, {}, only need values for ('{}')".format( len( converter) - 1, "', '".join( argNames)))
                    else:
                        raise Exception( "No Parameters required, {} specified".format( len( converter) - 1))

                for index, value in enumerate( converter[1:]):
                    try:
                        if str( value).lower() in ["true", "false"]:
                            value = str( value).capitalize()
                        converter[ index + 1] = eval( str( value))

                    except Exception as e:
                        raise Exception( "Failed to evaluate Parameter '{}', Error: '{}'".format( argNames[ index], e))

            except Exception as e:
                outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Loading Converter '{}' Failed, Error: '{}'!".format( converter[0], e)})
                outcome[ "success"] = False     # Set as error

                return outcome

    if ignoreItems and not isinstance( ignoreItems, list):
        # Make it a list
        ignoreItems = [str( ignoreItems)]

    returnItem = (hasattr( item, "returnUpdatedItem") and item.returnUpdatedItem) # Used by Sizzle logic!

    #
    # Verify Item
    #
    if not (item.type == "Feature Service" or isFileItem):
        outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Item Type is NOT a 'Feature Service' or supported File Item Type"})
        outcome[ "success"] = False     # Set as error
    else:
        #
        # Get Feature Layer Manager for item
        #
        manager = _getManager( item, verbose=verbose, outcome=outcome) if not isFileItem else None
        if outcome[ "success"] == False:
            return outcome

        #
        # Verify Feature Service item is not a view
        #
        #if hasattr( manager, "properties") and hasattr( manager.properties, "isView") and manager.properties.isView:
        if "View Service" in item.typeKeywords:
            # Error out if we are trying to overwrite a View
            if updateFile:
                outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Feature Service Item is a 'View', cannot Overwrite"})
                outcome[ "success"] = False     # Set as error
                return outcome

            # Check status of View's Layers
            _checkView( item, verbose=verbose, outcome=outcome, dryRun=dryRun, outPath=outPath)

        #
        # Verify Feature Service is NOT actively in a Publishing state
        #
        #status = item.status()
        #if status and isinstance( status, dict) and status.get( "jobInfo", {}).get( "jobType", "") == "publish" and status.get( "status", "") == "processing":
        #    outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Feature Service is already undergoing a 'publishing' action, cannot Overwrite at this time, active JobId: {}".format( status.get( "jobInfo", {}).get( "jobId", "N/A"))})
        #    outcome[ "success"] = False     # Set as error
        #    return outcome

        if updateFile:
            #
            # Verify Service Data Item and get original Filename used for publication
            #
            outputFile = "" if not isFileItem else item.name
            for dataItem in item.related_items( "Service2Data"):
                outputFile = dataItem.name
                break

            if not outputFile:
                outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Missing Associated Service Data Item or datafile Item 'name'"})
                outcome[ "success"] = False     # Set as error

                return outcome

            #
            # Check that Service Overwrite is allowed
            #
            hasChangeTrackingOnView = manager.properties.get( "hasChangeTrackingEnabledViews", False) if not isFileItem else False

            if hasChangeTrackingOnView:
                outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Overwrite on Service is NOT allowed, a dependent View or Service has Change Tracking Enabled!"})
                outcome[ "success"] = False     # Set as error

                return outcome

            for view in item.related_items( "Service2Service"):
                if view.type in ["WFS"]:    # "OGCFeatureServer",
                    outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Overwrite on Service is NOT allowed, a dependent WFS Service exists!"})
                    outcome[ "success"] = False     # Set as error

                    return outcome

            #
            # Do Download and/or Update
            #
            headers = None
            layers = manager.properties.get( "layers", []) if not isFileItem else []

            #
            # Check for Optimization
            #
            wasOptimized = False
            if manager:
                for layer in manager.layers:
                    if "multiScaleGeometryInfo" in layer.properties:
                        wasOptimized = True
                        if verbose:
                            print( " - Detected Optimization on Layer Id: {}".format( layer.properties.get( "id", "")))

            # Get Last Modified details, use provided 'view' value if available!
            if not serviceLastModified:
                serviceLastModified = 0 if not layers else layers[0].get( "editingInfo", {}).get( "lastEditDate", 0)
                serviceLastModified = 0 if not serviceLastModified else datetime.datetime.utcfromtimestamp( int( serviceLastModified / 1000))

            if verbose and serviceLastModified:
                print( " - Service Last Modified: {}".format( serviceLastModified.strftime( "%a, %d %b %Y %H:%M:%S GMT")))

            #
            # Download Web data for update!
            #
            lastFile = {}
            if updateFile.split(":")[0].lower() in ["ftp", "http", "https"]:
                outputFile = os.path.join( tempfile.gettempdir() if not outPath else outPath, outputFile)

                # Get Last Modified details
                fileLastModified = 0 if not os.path.exists( outputFile) else datetime.datetime.utcfromtimestamp( int( os.stat( outputFile).st_mtime))

                # Set lastModified to file lastModified if Service details are not available!
                lastModified = serviceLastModified if serviceLastModified else fileLastModified

                try:
                    authHandlers = []

                    # Add Hander(s)
                    authHandlers.append( urllib.request.HTTPSHandler( context=urllib.request.ssl.SSLContext( urllib.request.ssl.PROTOCOL_SSLv23)))

                    # Install Handler(s)
                    if not verbose == False:
                        print( "\nAccessing URL...")
                    urllib.request.install_opener( urllib.request.build_opener( * authHandlers))

                    headers = {
                        "Accept": "*/*",
                        "Accept-Encoding": "*",
                        "User-Agent": "Python/v{} OverwriteFS.py/{}".format( platform.python_version(), version)
                    }

                    request = urllib.request.urlopen( urllib.request.Request( updateFile, headers=headers))
                    headers = dict( request.info()._headers) if hasattr( request, "info") else {}   # Get Header 'Tuple' list and convert to dictionary

                    # Adjust Headers
                    for key, value in headers.copy().items():
                        if key.lower() == "last-modified":
                            del headers[ key]
                            headers[ "Last-Modified"] = value

                    if verbose:
                        print( " -   Download to: '{}'".format( outputFile))
                        print( " - Last Modified: {} (Local Download)".format( fileLastModified.strftime( "%a, %d %b %Y %H:%M:%S GMT") if fileLastModified else "N/A"))
                        print( " - Last Modified: {} (URL Content)".format( datetime.datetime.strptime( headers["Last-Modified"], "%a, %d %b %Y %H:%M:%S GMT") if "Last-Modified" in headers else "N/A"))
                        if maxVerbose:
                            print( "\nWeb Headers:")
                            for key, value in headers.items():
                                print( " - {}: {}".format( key, value))

                    try:
                        if lastModified and "Last-Modified" in headers and lastModified >= datetime.datetime.strptime( headers["Last-Modified"], "%a, %d %b %Y %H:%M:%S GMT"):
                            status = "No Change in URL Data"
                            if verbose:
                                print( "\n * {}{}!".format( status, " * instructed to Ignore" if ignoreAge else ""))

                            if not ignoreAge:
                                if touchItems:
                                    #for view in item.related_items( "Service2Service"):
                                    for view in item.related_items( "Service2Data", "reverse"): # Select only Views that rely on this Service
                                        touchItem( view, "\nTouching details on related View: '{}'".format( view.title), outcome)

                                outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "download", "result": status})
                                outcome[ "success"] = None  # No Action Performed

                                return outcome

                        elif fileLastModified:
                            # Trigger CRC File comparison if we have an existing download!
                            # Save CRC, Size, and Name of existing file
                            crcStart = datetime.datetime.now()
                            crcValue = _getCRC( outputFile)
                            lastFile = { "filename": outputFile, "CRC": crcValue, "filesize": os.stat( outputFile).st_size}
                            if maxVerbose:
                                print( "\nElapsed Time to Calc CRC value on existing file: {}, Value: {}".format( datetime.datetime.now() - crcStart, crcValue))

                    except Exception as e:
                        if verbose:
                            print( " * Issue Ignored * But, unable to compare 'Last-Modified' Dates, Error: '{}'".format( e))

                    # Download file!
                    if not verbose == False:
                        print( "\nDownloading Data...")

                    with open( outputFile, "wb") as oFP:
                        buffer = request.read()
                        while buffer:
                            oFP.write( buffer)
                            buffer = request.read()

                        updateFile = outputFile

                except Exception as e:
                    status = "Failed to Download data from url, Outcome: '{}'".format( e)
                    if not verbose == False:
                        print( " * {}".format( status))

                    outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "download", "result": status})
                    outcome[ "success"] = False     # Set as error

                    return outcome

            elif os.path.isdir( updateFile):
                updateFile = os.path.join( updateFile, outputFile)

                if not os.path.exists( updateFile):
                    outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Update Filename '{}', Originally Published with {}, CANNOT be found in Folder: '{}'".format( outputFile, "Item" if isFileItem else "Service", os.path.split( updateFile)[0])})
                    outcome[ "success"] = False     # Set as error

                    return outcome

            #elif not os.path.split( updateFile)[-1] == os.path.split( outputFile)[-1]:
            #    outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Update Filename '{}' does NOT match Original Filename used to Publish {}: '{}'".format( updateFile, "Item" if isFileItem else "Service", outputFile)})
            #    outcome[ "success"] = False     # Set as error

            #    return outcome

            if not os.path.exists( updateFile):
                outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Update Filename '{}' does NOT Exist".format( updateFile)})
                outcome[ "success"] = False     # Set as error

                return outcome

            if not os.path.isfile( updateFile):
                outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Update Filename '{}' is NOT a File".format( updateFile)})
                outcome[ "success"] = False     # Set as error

                return outcome

            #
            # Convert data prior to Overwrite!
            #
            if converter:
                args = converter[1:]        # Get the Converter argument values
                keyargs = {}                # Define Keyword Arguments
                converter = converter[0]    # Get the Converter Module
                fileTimestamp = os.stat( updateFile).st_mtime                       # Save current File Timestamp, in case we need to re-apply
                updatePath, updateFilename = os.path.split( os.path.realpath( outputFile))    #updateFile))  # Extract Path and Filename

                # Run Conversion logic
                try:
                    if not verbose == False:
                        print( "\n - Converting Data using Module: '{}'{}".format( converter.__name__, ", Version: '{}'".format( converter.__version__) if hasattr( converter, "__version__") else ""))

                    argCount = converter.convert.__code__.co_argcount
                    argNames = converter.convert.__code__.co_varnames[1:argCount]    # excluding download filename

                    if "verbose" in argNames and (argNames.index( "verbose") + 1) > len( args): # If 'verbose' available and not in arguments provided, add it!
                        keyargs[ "verbose"] = True if verbose else False

                    if "checkPublication" in argNames and (argNames.index( "checkPublication") + 1) > len( args): # If 'checkPublication' available and not in arguments provided, add it!
                        if ignoreAge:
                            keyargs[ "checkPublication"] = False

                    if keyargs:
                        updateFile = converter.convert( updateFile, *args, **keyargs)
                    else:
                        updateFile = converter.convert( updateFile, *args)

                    if not updateFile:
                        status = "No Change in URL Data (reported by Converter)"
                        if ignoreAge:
                            status = "Converter did NOT return a converted file"

                        if verbose:
                            print( "\n * {}!".format( status))

                        if touchItems:
                            #for view in item.related_items( "Service2Service"):
                            for view in item.related_items( "Service2Data", "reverse"): # Select only Views that rely on this Service
                                touchItem( view, "\nTouching details on related View: '{}'".format( view.title), outcome)

                        outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "download", "result": status})
                        outcome[ "success"] = None  # No Action Performed

                        return outcome

                    # Check new filename and location
                    updateFile = os.path.realpath( updateFile)
                    originalFile = os.path.join( updatePath, updateFilename)

                    if os.path.split( updateFile)[0] == updatePath:
                        # Files in Same folder
                        if not os.path.split( updateFile)[-1] == updateFilename:
                            # Different filenames, rename
                            if os.path.exists( originalFile):
                                os.remove( originalFile)
                            os.rename( updateFile, originalFile)
                            updateFile = originalFile
                    else:
                        # Different folders, copy over original file
                        updateFile = shutil.copy2( updateFile, originalFile)

                except Exception as e:
                    if not verbose == False:
                        traceback.print_exc()
                    outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "converter", "result": "Data Conversion Failed, Error: '{}'!".format( e)})
                    outcome[ "success"] = False     # Set as error

                    return outcome

                if fileTimestamp:
                    # Restore Date/Time of downloaded file, to retain an accurate last update Date/Time
                    os.utime( updateFile, ( fileTimestamp, fileTimestamp))

            # Check update Filename to expected output file used to publish service
            if not os.path.split( updateFile)[-1] == os.path.split( outputFile)[-1]:
                outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "verify", "result": "Update Filename '{}' does NOT match Original Filename used to Publish {}: '{}'".format( updateFile, "Item" if isFileItem else "Service", outputFile)})
                outcome[ "success"] = False     # Set as error

                return outcome

            #
            # Check if we need to compare Download to last file
            #
            if lastFile:
                if os.stat( updateFile).st_size == lastFile[ "filesize"]:
                    # Same Size, Check contents
                    #if filecmp.cmp( updateFile, lastFile):
                    crcStart = datetime.datetime.now()
                    crcValue = _getCRC( updateFile)
                    if maxVerbose:
                        print( "\nElapsed Time to Calc CRC value on file download: {}, Value: {}".format( datetime.datetime.now() - crcStart, crcValue))

                    if lastFile[ "CRC"] == crcValue:
                        status = "No Change in URL Data (from CRC comparison)"
                        if verbose:
                            print( "\n * {}{}!".format( status, " * instructed to Ignore" if ignoreAge else ""))

                        if not ignoreAge:
                            if touchItems:
                                #for view in item.related_items( "Service2Service"):
                                for view in item.related_items( "Service2Data", "reverse"): # Select only Views that rely on this Service
                                    touchItem( view, "\nTouching details on related View: '{}'".format( view.title), outcome)

                            outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "download", "result": status})
                            outcome[ "success"] = None  # No Action Performed

                            return outcome

            #
            # Update a File Item
            #
            if isFileItem:
                if verbose:
                    print( " - Overwriting File Item, Type: '{}'".format( item.type))
                    print( " - Source Data: '{}'".format( os.path.realpath( updateFile)))

                if dryRun:
                    if verbose:
                        print( " * Dry Run * No Change!")
                    status = True
                else:
                    try:
                        status = item.update( data=updateFile)
                        if verbose:
                            print( " - {}!".format( "Success" if status else "Failed"))

                    except Exception as e:
                        if not verbose == False:
                            traceback.print_exc()
                            print( " * Failed to Update Item, Id: '{}', Outcome: '{}'".format( item.id, e))
                        status = "Failed, Outcome: '{}'".format( e)
                        status = status if not "error code" in status.lower() else status.replace( "\n", " ")

                outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "update", "result": status})
                outcome[ "success"] = (not status == False and not str( status).startswith( "Failed"))

                return outcome

            #
            # Backup Service and Item properties
            #
            _backupProperties( item, verbose=verbose, outcome=outcome, outPath=outPath)

            if outcome[ "success"] == False:
                return outcome

            #
            # Setup Delete Layers json if needed
            #
            layerProperties = item.backupLayerProperties if hasattr( item, "backupLayerProperties") else []
            tableProperties = item.backupTableProperties if hasattr( item, "backupTableProperties") else []
            dropLayers = {"layers": []}

            for layer in layerProperties + tableProperties:
                dropLayers[ "layers"].append( {"id": layer[ "id"]}) # Add Layer to drop list

            #
            # Perform Overwrite or Update!
            #
            if (not verbose == False) and headers:
                # Seperate from Download dialog
                print( "\nPerforming Overwrite...")

            if verbose:
                print( " - Source Data: '{}'".format( os.path.realpath( updateFile)))

            status = None

            try:
                #
                # Do Overwrite!
                #
                if verbose:
                    print( " - Overwriting Service...")

                start = datetime.datetime.now()
                #asyncCall = True if globals().get( "async") else False
                asyncCall = True
                createInfo = False
                outstandingIssue = False
                for loop in range( 1, -2, -1):
                    try:
                        if dryRun:
                            if verbose:
                                print( " * Dry Run * No Change!")
                            status = {"success": True}
                        else:
                            if createInfo and dataItem and not dataItem.type == "Service Definition":
                                # Create missing infoFile for service File item
                                createInfo = None

                                try:
                                    if not verbose == False:
                                        print( "\n - Missing Info File, Updating Service Item Info!")

                                    tempFile = os.path.sep.join( [tempfile.gettempdir(), "{}_itemInfo.json".format( item.id)])
                                    updateInfo = "/".join( ["content", "users", item._gis._con._username, "items", item.id, "updateInfo"])
                                    infoData = {
                                        "name": manager.properties["adminServiceInfo"]["name"],
                                        "maxRecordCount": manager.properties["maxRecordCount"],
                                        "hasStaticData": manager.properties["hasStaticData"],
                                        "layerInfo": {
                                            "capabilities": manager.properties["capabilities"]
                                        }
                                    }
                                    json.dump( infoData, open( tempFile, "w"))
                                    status = item._gis._con.post( updateInfo, files=[["file", tempFile, "publishParameters.json"]])
                                    if not (status and isinstance( status, dict) and status.get("success", False)):
                                        raise Exception( status)

                                    if not verbose == False:
                                        print( " - Continuing Overwrite...")

                                except Exception as e:
                                    if not verbose == False:
                                        traceback.print_exc()
                                        print( " * Failed to Generate Item Info File for Id: '{}', Outcome: '{}' * Skipping!".format( item.id, e))

                            if dataItem and dataItem.type == "GeoPackage":
                                # Handle Overwrite of GeoPackage item type!
                                if verbose:
                                    print( " - Updating File Item, Type: '{}'...".format( dataItem.type))
                                status = dataItem.update( data=updateFile)
                                if status:
                                    if verbose:
                                        print( " - Updating Service...")
                                    status = dataItem.publish( overwrite=True, file_type=dataItem.type)
                                    if isinstance( status, arcgis.gis.Item):
                                        status = {"success": True}
                                else:
                                    raise Exception( "data item update failed '{}'".format( status))
                            else:
                                status = manager.overwrite( updateFile)

                            _refreshManager( manager)

                        if not (status and isinstance( status, dict) and status.get( "success", False)):
                            outstandingIssue = True
                            raise Exception( status)

                        outstandingIssue = False
                        break

                    except Exception as e:
                        trace = traceback.format_exc()
                        postText = ""
                        if "item info file does not exist" in str(e).lower() and createInfo is not None:
                            createInfo = True

                        if "related_data_item.update(" in trace:
                            postText = " while Updating Related File Item (review upload content)"

                        # Too big for Overwrite?
                        if isinstance(e, ValueError) and "does not exist" in str(e):
                            raise

                        if not verbose == False:
                            if not createInfo:
                                traceback.print_exc()
                            print( " * Failed to Update Item, Id: '{}', Outcome: '{}'{}".format( item.id, e, postText))

                        if "in _add_by_part" in trace and "can only concatenate" in trace:
                            # Detect update of Item located in user defined folder. Python API issue!
                            postText = "Suggest relocating Service FILE Item to user's root folder and retrying update"
                            print( "\n * {}! *\n".format( postText))
                            outstandingIssue = True
                            if not status:
                                status = postText
                            break

                        if loop and not dryRun:
                            if "job failed" in str( e).lower():
                                # Detect 'Failed job', now attempt to drop Layers in Target before next Overwrite!
                                data = { "f": "json", "async": asyncCall, "deleteFromDefinition": json.dumps(obj=dropLayers, separators=(',', ':'))}

                                try:
                                    if not verbose == False:
                                        print( "\n - Attempting to drop existing Layers before retrying Overwrite...")

                                    if asyncCall:
                                        delStatus = _asyncJob( manager, "deleteFromDefinition", data, verbose=True if maxVerbose else (None if verbose else False), indent="  ")
                                    else:
                                        delStatus = manager._gis._con.post(  manager.url + "/" +"deleteFromDefinition", data)

                                    _refreshManager( manager)

                                    if not verbose == False:
                                        print( "   Status: {}".format( delStatus))

                                except Exception as e:
                                    if not verbose == False:
                                        traceback.print_exc()
                                        print( "\n * Error: '{}'\n    Data: '{}'".format( e, data))

                            if not verbose == False:
                                print( "\n * Retrying!")

                            time.sleep(1)
                else:
                    outstandingIssue = True

                #
                # Verify Overwrite completed successfully
                #
                #status = status if outstandingIssue else item.status()
                if not outstandingIssue and not dryRun:
                    try:
                        status = item.status()
                    except Exception as e:
                        if not verbose == False:
                            print( " * Status check failure, error: '{}', Ignored!".format( e))

                if not (status and isinstance( status, dict) and (status.get("success") or (status.get( "status", "") == "completed"))):
                    raise Exception( "Feature Service 'publish' action did NOT complete as expected, Job Details: '{}'".format( status))

                if not verbose == False:
                    print( " - Success! Elapsed Time: {}".format( datetime.datetime.now() - start))

                #
                # Cancel Automatic Layer Optimizations if needed
                #
                if wasOptimized and not noProps and not dryRun:
                    try:
                        curDetails = manager._gis._con.post(  manager.url, { "f": "json"})
                        wait = globals().get( "deoptimizewait") if globals().get( "deoptimizewait") else 60  # Seconds to wait before attempting to cancel Auto-Optimize!
                        timeout = 120
                        for layer in curDetails[ "layers"]:
                            if layer["geometryType"] in ["esriGeometryPolyline", "esriGeometryPolygon"] and "multiScaleGeometryStatus" in layer:
                                while wait:
                                    sys.stdout.write( "   - Canceling Layer Optimization in {} second{}... \r".format( wait, "s" if wait > 1 else ""))
                                    time.sleep( 1)
                                    wait -= 1
                                layerId = layer["id"]
                                for layer in manager.layers:
                                    if layer.properties.get( "id", "") == layerId:
                                        retries = 3
                                        mult = 1
                                        if verbose:
                                            print( "   - Canceling Automatic Optimization on Layer Id: {}".format( layer.properties.get( "id", "")))
                                        start = datetime.datetime.now()
                                        if dryRun:
                                            if verbose:
                                                print( " * Dry Run * No Change!")
                                            status = {"success": True}
                                        else:
                                            while retries:
                                                status = _asyncJob( layer, "updateDefinition", { "f": "json", "async": True, "updateDefinition": '{"multiScaleGeometryInfo":null}'}, verbose=True if maxVerbose else (None if verbose else False), indent="    ", noWait=noWait, timeout=timeout * mult)
                                                if str( status).lower() != "timeout":
                                                    break
                                                retries -= 1
                                                mult += 1
                                                if retries and not verbose == False:
                                                    print( "   * Retrying!")
                                            else:
                                                raise Exception( "Retry Timeout Exceeded")

                                        if not (isinstance( status, dict) and status.get( "success", False)):
                                            if not verbose == False:
                                                print( "\n * Failed! Error: {}".format( status))
                                        elif verbose:
                                            _prints( "\r   - Optimization Canceled! Elapsed Time: {}    ".format( datetime.datetime.now() - start), status.get( "progressLength", 0) if hasattr( status, "get") else 0)

                                        break

                    except Exception as e:
                        if not verbose == False:
                            print( "\n * Failed to Check/Cancel Optimization, Error: {}".format( e))

                #
                # Restore Service and Item properties from Backup
                #
                item = _restoreProperties( item, verbose=verbose, outcome=outcome, touchTimeSeries=touchTimeSeries, noIndexes=noIndexes, preserveProps=preserveProps, noWait=noWait, noProps=noProps, dryRun=dryRun)

                _refreshManager( manager)

                status = outcome.get( "success", False)

            except Exception as e:
                trace = traceback.format_exc()
                postText = ""
                if "related_data_item.update(" in trace:
                    postText = " while Updating Related File Item"
                # Too big for Overwrite?
                elif isinstance(e, ValueError) and "does not exist" in str(e):
                    postText = " * Source file too large, cannot update service item with {:,} byte file using Python API v{}".format( os.stat( updateFile).st_size, arcgis.__version__)

                if not verbose == False:
                    traceback.print_exc()
                    print( " * Failed to Update Item, Id: '{}', Outcome: '{}'{}".format( item.id, e, postText))
                status = "Failed, Outcome: '{}'{}".format( e, postText)
                status = status if not "error code" in status.lower() else status.replace( "\n", " ")

            outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": "update", "result": status})
            outcome[ "success"] = (not status == False and not str( status).startswith( "Failed"))

        elif (touchItems or touchTimeSeries):
            #
            # No Data Updates Made
            #
            if touchItem:
                outcome[ "success"] = touchItem( item, "\nTouching Item details...", outcome)
        else:
            outcome[ "items"].append( {"id": item.id, "title": item.title, "itemType": item.type, "action": None, "result": None})

    #
    # Touch related Feature Service Views, if any exist!
    #
    if (touchItems or touchTimeSeries) and outcome[ "success"] == True:
        #for view in item.related_items( "Service2Service"):
        for view in item.related_items( "Service2Data", "reverse"): # Select only Views that rely on this Service
            if view.id in ignoreItems:
                if verbose:
                    print( "\n * Ignoring 'touch' on related View: '{}'".format( view.title))
                continue

            if touchItems:
                touchItem( view, "\nTouching details on related View: '{}'".format( view.title), outcome)

                if touchTimeSeries and updateFile:
                    touchTimeInfo( view, "", outcome)
                    #touchTimeInfo( view, "\nTouching Time Series Layers...", outcome)

            elif touchTimeSeries and updateFile:
                touchTimeInfo( view, "\nTouching Time Series Layers on related View: '{}'".format( view.title), outcome)

    return (item if returnItem else outcome)

if __name__ == "__main__":
    # Import additional modules used only by the command line processing
    import configparser, socket

    #
    # Verify Inputs
    #
    help = [True for a in sys.argv if a.lower() == "-h"]

    if len( sys.argv) < 4 or help:
        print( "\n{} Usage: Python {} [-h] <profile> <item> <title> [<filename> | <url>] [-OutPath <output folder>] [-NoTimeSeries] [-NoIndexes] [-NoTouch] [-NoWait] [-NoProps | -PersistProps] [-DryRun] [-IgnoreAge] [-GetTarget | -UpdateTarget | -SwapLayers | -SwapByOrder | [-ListRelated] [-AddRelated | -RemoveRelated [<Item A id>[ <Item B id>]]]] [-Convert <module>[ <call param>[ <call param>[ ...]]]] [-AllowPWprompt] [-LessDetail | -MoreDetail] [-Password <password>]".format( version, __file__))
        print( "\n             -h: (optional) Action Switch that triggers 'usage' display and exit.")
        print( "\n      <profile>: (required) Stored Python API user Profile to connect with.")  #
        print( "                            Specify 'Pro' to leverage active ArcGIS Pro connection, also requires Arcpy!")
        print( "\n         <item>: (required) Item Id of Feature Service or Feature View to act on.")
        print( "\n        <title>: (required) Title of <item>, to verify item Id matches correct item.")
        print( "\n     <filename>: (optional) File path/name, or URL containing Service data to update Service with.")
        print( "        or <url>            If 'path' only, script will search path for original filename used to publish Service.")
        print( "                            Default: Empty string, only touch Item and/or Views in order to refresh last update date.")
        print( "\n       -OutPath: (optional) Option Switch setting output file Path used to store <url> download data during run.")
        print( "                            * Note * Ignored when specified <filename> is used!")
        print( "                            Default: User's Temporary folder.")
        print( "\n<output folder>: (optional, but required when using -OutPath option.")
        print( "\n  -NoTimeSeries: (optional) Option Switch instructing function not to touch Time Extent of Time Series enabled Layers")
        print( "                            in Service and related Views.")
        print( "                            Default: Touch Layer Time Information of Item and related Views, reflecting new data.")
        print( "\n     -NoIndexes: (optional) Option Switch instructing function not to recreate missing Layer Indexes on Service.")
        print( "                            Default: Recreate indexes if they are missing after an Overwrite action.")
        print( "\n       -NoTouch: (optional) Option Switch instructing function not to update the 'lastUpdated' item property for the")
        print( "                            Service or related Views unless an update has been made to them.")
        print( "                            Default: Update, or 'touch', the 'lastUpdate' property on the Service item and Views, even")
        print( "                                     when an update has not been made. Signifies that the process has been run.")
        print( "\n        -NoWait: (optional) Option Switch instructing function not to wait for re-application of properties like Layer")
        print( "                            Optimization to complete before continuing the update workflow. When enabled, function will")
        print( "                            report condition and supply a URL that can be used for manual status review.")
        print( "                            Default: Function will wait for properties like Layer Optimization to be re-applied before")
        print( "                                     proceeding to the next processing 'step' in the update workflow.")
        print( "\n       -NoProps: (optional) Option Switch instructing function NOT to Re-Apply Service or View properties following")
        print( "                            a successful update. This Defaults Service or View back to its Published state!")
        print( "                            Default: Re-Apply properties and do not persist Backup file beyond successful update.")
        print( "\n  -PersistProps: (optional) Option Switch instructing function to retain Service or View property Backup File")
        print( "                            after a successful Overwrite and property restoration.")
        print( "                            Default: Re-Apply properties and do not persist Backup file beyond successful update.")
        print( "\n        -DryRun: (optional) Option Switch instructing function to step through process WITHOUT updating the Service.")
        print( "                            Default: Update or Touch the Service and Item.")
        print( "\n     -IgnoreAge: (optional) Option Switch instructing function to ignore <url> download age checks, and update Service.")
        print( "                            Default: Cancel Service update if age of <url> data is not newer than last Service update.")
        print( "\n    -SwapLayers: (optional) Action Switch instructing function to Swap Layers in View, point all Layers to Target")
        print( "                            or newly updated Feature Service. Used by A/B Feature Service enabled View, whereby the")
        print( "                            View is Related to Two Feature Services, allowing the View's Layers to be swapped out,")
        print( "                            pointing them to the matching Layers of the newly updated Feature Service.")
        print( "                            Default: Overwrite action when <filename> specified and no other Action Switches included.")
        print( "\n   -SwapByOrder: (optional) Action Switch instructing function to Swap Layers in View, same operation as -SwapLayers")
        print( "                            except the Layers are mapped in the order they are presented in the service Layer List.")
        print( "                            Default: Overwrite action when <filename> specified and no other Action Switches included.")
        print( "\n     -GetTarget: (optional) Action Switch instructing function to report the selected Feature Service item Id, Title,")
        print( "                            and File Item upload Filename that should be used for the next update target. Leveraged by")
        print( "                            A/B View enabled Services to select inactive Feature Service that should be updated next.")
        print( "                            Default: Overwrite action when <filename> specified and no other switches included.")
        print( "\n  -UpdateTarget: (optional) Action Switch instructing function to update Target Service used by -SwapLayers action.")
        print( "                            This invokes the -SwapLayers workflow, but only updates the Target Service, allowing")
        print( "                            manual QA/QC before a Production Layer Swap.")
        print( "                            Default: Overwrite action when <filename> specified and no other Action Switches included.")
        print( "\n   -ListRelated: (optional) Action Switch instructing function to List all Items related to <item> View or Service")
        print( "                            Default: Overwrite action when <filename> specified and no other switches included.")
        print( "\n    -AddRelated: (optional) Action Switch instructing function to Add specified Service Items to View <item>, limit two")
        print( "                            item Ids as Related Services. Related Services are required for 'SwapLayers' action to")
        print( "                            switch a View's Layers so they point to Related 'target' Feature Service Layers.")
        print( "                            Default: If No Items specified, action will be to 'ListRelated' <item>'s Relationships.")
        print( "\n -RemoveRelated: (optional) Action Switch instructing function to Remove specified A/B Service Items from View <item>.")
        print( "                            Related Feature Services are required for 'SwapLayers' to target and switch Layers of")
        print( "                            <item> View so they point to same Layers referenced by Related 'target' Feature Service.")
        print( "                            Default: If No Items specified, action will be to 'ListRelated' <item>'s Relationships.")
        print( "\n    <Item A id>: (optional, but at least one is required when using -AddRelated or -RemoveRelated Action Switch)")
        print( " or <Item B id>             The unique Item Identifier for the Content Item managing the Service.")
        print( "                            Ex: Id for 'Active Hurricanes, Cyclones, and Typhoons' is: 248e7b5827a34b248647afb012c58787")
        print( "\n       -Convert: (optional) Action Switch instructing function to import specified Python Module and use it to")
        print( "                            transform the source data before updating the Hosted Feature Service.")
        print( "                            * Note * Only available during a Overwrite action.")
        print( "                            Default: No conversion of the input data will take place.")
        print( "\n       <module>: (optional, but required when using -Convert Action Switch) A Case-Sensitive Python Module (or script)")
        print( "                            name to leverage for data conversion prior to updating Service. Supports Python 'Dot'")
        print( "                            notation for Folder/Module/Class path access to 'convert' Function.")
        print( "                            * Note * Path is RELATIVE to 'Converters' folder in OverwriteFS.py script location!")
        print( "                            Ex: <folder>.<module>.<class>")
        print( "\n   <call param>: (optional) Additional call parameters to pass to '-Convert' Module function. Only need to include")
        print( "                            parameters that follow the download file specifier in 'convert' Function.")
        print( "\n -AllowPWprompt: (optional) Option Switch instructing function to Allow use of an undefined user account password")
        print( "                            in the Profile, allowing Python API to prompt for user entry.")
        print( "                            * Note * Not recommended for use during Unattended execution!")
        print( "                            Default: If Profile Password is not defined, function will report the condition and Exit.")
        print( "\n    -LessDetail: (optional) Option Switch instructing function to only Display major steps and error responses.")
        print( "                            Default: Display step by step processing detail, or -MoreDetail if set.")
        print( "\n    -MoreDetail: (optional) Option Switch instructing function to Display maximum Diagnostic detail and error response.")
        print( "                            Default: Display step by step processing detail, or -LessDetail if set.")
        print( "\n      -Password: (optional) Option Parameter instructing function to overwride the stored Profile password with this")
        print( "                            plain text password.")
        print( "                            Default: Use password set in Profile.")
        print( "\n     <password>: (optional, but required when using -Password Option)")

        if help:
            exit()

        exit( "\n\a * ERROR * Insufficient Input Parameters, please review 'Usage'!")

    # Extract Switches [[<name>, <enabled init value>], ...]
    validSwitches = [
        ["SwapLayers", True], ["NoTimeSeries", True], ["NoIndexes", True], ["NoTouch", True], ["NoWait", True], ["NoProps", True], ["PersistProps", True], ["GetTarget", True], ["UpdateTarget", True],
        ["ListRelated", True], ["AllowPWprompt", True], ["AddRelated", []], ["RemoveRelated", []], ["LessDetail", True], ["MoreDetail", True], ["DryRun", True], ["Password", ""],
        ["OutPath", ""], ["Convert", []], ["IgnoreAge", True], ["SwapByOrder", True],
        ["Async", True], ["OptimizeDP", True], ["DeOptimizeWait", 0]  #, ["NoLimit", True] # Hidden Parameters, accessible as lower case key in Globals!
    ]
    lowerSwitches = {}
    localVars = locals()
    for arg, enabled in validSwitches:
        localVars[ arg.lower()] = None
        lowerSwitches[ arg.lower()] = enabled

    # Process specified options
    profile, itemId, itemTitle = sys.argv[1:4]
    updateFile, updateExists, updateStatus = None, False, ", Touch item & views only!"
    lastSwitch = ""
    usingPro = profile.lower() == "pro"

    for index, arg in enumerate( sys.argv[4:]):
        lowerArg = arg.strip( "-").lower()
        hasDash = arg.startswith( "-") # or True # Staged to allow Dashed and Non-Dashed (as a transition) Option Switches
        if lowerArg in lowerSwitches and hasDash:
            localVars[ lowerArg] = lowerSwitches[ lowerArg]
            lastSwitch = lowerArg
        elif lastSwitch in ["addrelated", "removerelated", "convert"]:    # List of values
            localVars[ lastSwitch].append( arg)
        elif lastSwitch in ["password", "outpath"]:                       # Single value only
            localVars[ lastSwitch] = arg
        elif lastSwitch in ["deoptimizewait"]:                            # Single integer only
            try:
                localVars[ lastSwitch] = int( arg)
            except:
                exit( "\n\a * Integer Switch '{}' is invalid, bad value '{}'!".format( sys.argv[4:][index-1], arg))
        else:
            if os.path.exists( arg):
                updateFile = arg
                if os.path.isfile( arg):
                    # Is a File
                    updateExists = True
                    fileLastModified = datetime.datetime.utcfromtimestamp( int( os.stat( updateFile).st_mtime))
                    updateStatus = ", Last Modified: {}".format( fileLastModified.strftime( "%a, %d %b %Y %H:%M:%S GMT"))
                else:
                    # Is a Directory
                    updateExists = None
                    updateStatus = " (From Directory!)"

            elif arg.split(":")[0].lower() in ["ftp", "http", "https"]:
                # Is a URL
                updateFile = arg
                updateExists = None
                updateStatus = " (From URL!)"

            else:
                # Unknown!
                if not updateFile:
                    updateFile = arg
                    updateStatus = " (NOT Found!)"
                print( "\a * Missing Update File or Invalid Switch: '{}'".format( arg))
                exit( "\nReview help '-h' if needed and try again!")

    print( "\n           Running: {}, {}".format( __file__, version))
    print( "Python API Profile: {}{}".format( profile, " (using active ArcGIS Pro connection)" if usingPro else ""))
    print( "           Item Id: {}".format( itemId))
    print( "        Item Title: '{}' (for verification)".format( itemTitle))
    print( "   Upload Filename: '{}'{}".format( updateFile, updateStatus))
    print( "          Switches: {}".format( ", ".join( ["{}={}".format( key, "*****" if key == "Password" else localVars[key.lower()]) for key, init in validSwitches if not localVars[key.lower()] == None])))

    if updateFile and updateExists is False:
        exit( "\n\a * ERROR * Unable to locate Update File or Folder '{}' *".format( updateFile))

    # Report Update action
    if updateFile and (gettarget or listrelated or addrelated or removerelated):
        print( "\n * Update Ignored!")

    # Set Default Timeout for Web Requests if not already set!
    if not socket.getdefaulttimeout():
        socket.setdefaulttimeout( 300.0)    # 5 minutes!

    verbose = "Max" if localVars[ "moredetail"] else (None if localVars[ "lessdetail"] else True)

    #
    # Verify Login
    #
    print( "\nLoading Python API...", end="")
    import arcgis   # Import Python API
    print( "Ready!\n")

    global gis  # For Converter access
    if not usingPro:
        if not (allowpwprompt or password):
            getPassword = None
            if hasattr( arcgis.GIS, "_securely_get_password"):
                # Get password function from GIS object for Python API v1.6.0
                getPassword = arcgis.GIS()._securely_get_password
            elif hasattr( arcgis.gis, "_impl") and hasattr( arcgis.gis._impl, "_profile"):
                # Get password function from Profile Manager for Python API v1.7.0+?
                getPassword = arcgis.gis._impl._profile.ProfileManager()._securely_get_password
            else:
                print( " - Cannot check password for Python API v{}, unable to securely get password!".format( arcgis.__version__))

            if getPassword:
                # Check Profile Password
                gis_cfg_file_path = os.path.expanduser("~") + '/.arcgisprofile'
                if os.path.isfile( gis_cfg_file_path) and profile: # v1.5.0
                    # Load config, get username form Profile
                    gisConfig = configparser.ConfigParser()
                    gisConfig.read( gis_cfg_file_path)
                    username = gisConfig[ profile][ "username"] if gisConfig.has_option( profile, "username") else None

                    # Verify we have a password for username, to avoid password prompt!
                    if username is not None and username:
                        password = getPassword( profile)
                        if password is None:
                            exit( "\n\a * ERROR * Password missing for user '{}' in Profile '{}'!".format( username, profile))

        print("Accessing ArcGIS Online/Enterprise...")
        if password:
            gis = arcgis.GIS( profile=profile, password=password)	# Be sure your Profile exists!
        else:
            gis = arcgis.GIS( profile=profile)	# Be sure your Profile exists!

        if not gis._username:
            exit( "\n\a * ERROR * Login failed, please verify Profile!")
    else:
        print("Accessing ArcGIS Online/Enterprise using Pro...")
        gis = arcgis.GIS( "pro")	# Be sure your Profile exists!
        print( " - Logged in with user: '{} ({})'".format( gis.users.me.username, gis.users.me.fullName))

    #
    # Verify Item
    #
    if verbose:
        print( " - Checking item...")

    item = gis.content.get( itemId)
    if not item:
        exit( "\a * ERROR * Unable to Locate specified Item: {} *".format( itemId))

    if not item.title == itemTitle:
        exit( "\a * ERROR * Feature Service Item Title does NOT match specified Title: '{}', Found: '{}'".format( itemTitle, item.title))

    outcome = { "success": None, "items": []}
    #
    # Perform Overwrite action based on switch settings!
    #
    start = datetime.datetime.now()
    if gettarget:
        # Get Target Feature Service Details
        getFeatureServiceTarget( item, outcome=outcome, verbose=verbose, ignoreDataItemCheck=True)

    elif swaplayers or updatetarget or swapbyorder:
        # Kickoff Swap Layers process
        swapFeatureViewLayers( item, updateFile=updateFile, verbose=verbose, touchTimeSeries=not notimeseries, outcome=outcome, noIndexes=noindexes, preserveProps=persistprops, noWait=nowait, noProps=noprops, converter=convert, outPath=outpath, dryRun=dryrun, noSwap=updatetarget, ignoreAge=ignoreage, byLayerOrder=swapbyorder)
        print( "\nElapsed Time for {} Process: {}".format( "Update Target" if updatetarget else "Swap Layers", datetime.datetime.now() - start))

    elif listrelated or isinstance( addrelated, list) or isinstance( removerelated, list):
        # Handle Related Views
        listrelated = listrelated or (isinstance( addrelated, list) and not addrelated) or (isinstance( removerelated, list) and not removerelated)

        if removerelated:
            # Remove related Views
            updateRelationships( item, relateIds=removerelated, unRelate=True, verbose=verbose, outcome=outcome, dryRun=dryrun)

        if addrelated and not outcome[ "success"] == False:
            # Add related Views
            updateRelationships( item, relateIds=addrelated, verbose=verbose, outcome=outcome, dryRun=dryrun)

        if not outcome[ "success"] == False:
            # List related Views
            updateRelationships( item, relateIds=[], verbose=verbose, outcome=outcome)
    else:
        # Do Overwrite
        overwriteFeatureService( item, updateFile=updateFile, touchItems=not notouch, verbose=verbose, touchTimeSeries=not notimeseries, outcome=outcome, noIndexes=noindexes, preserveProps=persistprops, noWait=nowait, noProps=noprops, converter=convert, outPath=outpath, dryRun=dryrun, ignoreAge=ignoreage)
        print( "\nElapsed Time for Overwrite Process: {}".format( datetime.datetime.now() - start))

    if outcome[ "success"] == False:
        # Exit Errorlevel 1, Failure encountered!
        exit( "\n\a * ERROR * {}".format( outcome[ "items"][-1][ "result"]))
    elif outcome[ "success"] is None:
        # Exit Errorlevel -1, No change was made!
        exit(-1)
    #else Exit Errorlevel 0, Success!