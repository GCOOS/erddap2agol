# ERDDAP2AGOL v.0.6.6 

The purpose of this project is to develop a robust tool for reliably transferring datasets between ERDDAPTM services and ArcGIS Online (AGOL). Erddap2agol is a "hands-off" Python package designed to operate seamlessly within the AGOL environment. Its core functionalities include creating, managing, and updating ERDDAPTM datasets at various levels of granularity. Developed by the Gulf of Mexico Ocean Observing System (GCOOS), erddap2agol is available for use by data providers leveraging ERDDAPTM services, as well as individuals and organizations aiming to analyze and visualize ERDDAPTM data on a GIS platform.

### ERDDAP2AGOL v0.6: Major update!   

**Subscribe to the GCOOS newsletter for updates.** <br />

## Who is this for?
This tool was principally designed for data managers who wish to expand the utility and accessibility of their ERDDAP services. To use the data maintenance capabilities (NRT or Weekly updates), you must be an administrator of your ArcGIS Online orginization. That said, anyone can run this tool to create ERDDAP data on their ArcGIS Online account! With an easy to use command line user interface and a ready made ArcGIS Online notebook, virtually no programming experience is necessary! <br />       

**Please note that this program, if you ask it to, will obediently consume all your ArcGIS Online credits.** <br />       


## Major Features
- Manage your ERDDAP data on ArcGIS Online at a variety of time-scales or data types!         
- NRT: 7 day moving window, Level 1. (Available now!)
- Glider DAC Line Segments: Create multiline HFLs from the Glider DAC ERDDAP (Available now!)  
- Historical: Updated weekly, contains all data, Level 2. (Coming Soon!)
- QC Historical: QC Flags, low quality records removed, Level 3. (WIP) <br />

- Multiple install options (Updating README on program functionality soon!).
- Install directly into an AGOL notebook.
- Build AGOL environment locally. <br /> 


## Core Modules

### erddap_wrangler.py
Contains the ERDDAPHandler class.<br />
Different ERDDAP Servers exist as objects of the ERDDAPHandler class. <br />
Class methods relate to generating request URLS and handling response content.<br />

### data_wrangler.py
Contains the DataWrangler class. <br />
This class is responsible for receiving and processing the ERDDAP datasets.<br />
Determines the size of the requested dataset and chunks dataset requests if needed. <br />   

### ago_wrangler.py
Contains the AgolWrangler class. <br />
Responsible for connecting the client to AGOL and interfacing with the ArcGIS Python API. <br />
Attributes of the NC Global section of the DAS are used to construct the item_properties dictionary. <br />
A feature service is created and populated with the dataset returned by the ERDDAP_Client URL. <br />


### Attributions
Update / Overwrite Feature Service v2.1.4 <br />
By: Deniz Karagulle & Paul Dodd, Software Product Release, Esri 


