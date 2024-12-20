# ERDDAP2AGOL v.0.6.0 

The purpose of this project is to develop a robust tool for reliably transferring datasets between ERDDAPTM services and ArcGIS Online (AGOL). Erddap2agol is a "hands-off" Python package designed to operate seamlessly within the AGOL environment. Its core functionalities include creating, managing, and updating ERDDAPTM datasets at various levels of granularity. Developed by the Gulf of Mexico Ocean Observing System (GCOOS), erddap2agol is available for use by data providers leveraging ERDDAPTM services, as well as individuals and organizations aiming to analyze and visualize ERDDAPTM data on a GIS platform.

### ERDDAP2AGOL v0.6: Major update!   

## Roadmap
The ERDDAP2AGOL tool is under active development. Stable features are indicated by the readme and version number. Currently, at version 0.4, NRT add is considered stable. To be notified when the tool is ready for use. <br />

**Subscribe to the GCOOS newsletter for updates.** <br />


## What to expect
* Your ERDDAP data will be available in three product levels.         
- NRT: 7 day moving window, Level 1. (WIP)
- Glider DAC Menu: Create multiline HFLs from the Glider DAC ERDDAP (Available now!)  
- Historical: Updated weekly, contains all data, Level 2. (WIP)
- QC Historical: QC Flags, low quality records removed, Level 3. (WIP) <br />

* Multiple install options.
- Install directly from an AGOL notebook.
- Build AGOL environment locally. <br /> 

* Monitor your ERDDAP collection with generated update logs

## Core Modules
### Das_client.py
The first point of contact with an ERDDAP server. <br />
The server response is converted from DAS to JSON and stored in-client. <br />
Time functions to assess data currency.  <br />
Relevant attributes to be encoded in the request url are identified. <br />

### ERDDAP_client.py
Contains the ERDDAPHandler class.<br />
Different ERDDAP Servers exist as objects of the ERDDAPHandler class. <br />
Class methods relate to generating request URLS and handling response content.<br />

### AGO_wrapper.py
Responsible for connecting the client to AGOL and interfacing with the ArcGIS Python API. <br />
Attributes of the DAS JSON file are used to construct the item_properties dictionary. <br />
A feature service is created and populated with the dataset returned by the ERDDAP_Client URL. <br />

## Additional Functionality

-Add ERDDAP data to ArcGIS Online with just an ERDDAP DatasetID  <br />
-ERDDAP2AGOL uses information contained within the metadata of the DAS (Data Attribute Structure) to fully populate AGOL item fields (Coming soon). <br />
-Ensure visibility of updates with update logs <br />
-Use state change or database log query to identify items for update<br />  

### Attributions
Update / Overwrite Feature Service v2.1.4 <br />
By: Deniz Karagulle & Paul Dodd, Software Product Release, Esri 


