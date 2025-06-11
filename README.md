# ERDDAP2AGOL v.0.8.0.1
## Griddap update!
## Works with ArcGIS Enterprise
The purpose of this project is to develop a robust tool for reliably transferring datasets between ERDDAP services and ArcGIS Online (AGOL) OR ArcGIS Enterprise. Erddap2agol is a "hands-off" Python package designed to operate seamlessly within the AGOL environment. Its core functionalities include creating, managing, and updating ERDDAP datasets at various levels of granularity. Developed by the Gulf of America Ocean Observing System (GCOOS), erddap2agol is available for use by data providers leveraging ERDDAP services, as well as individuals and organizations aiming to analyze and visualize ERDDAP data on a GIS platform.

**For questions or problems with the program, please submit an issue.** <br />

## Who is this for?
This tool was principally designed for data managers who wish to expand the utility and accessibility of their ERDDAP services. To use the data maintenance capabilities (NRT or Weekly updates), you must be an administrator of your ArcGIS Online organization. That said, anyone can run this tool to create ERDDAP data on their ArcGIS Online account! With an easy to use command line user interface and a ready made ArcGIS Online notebook, virtually no programming experience is necessary! <br />       

**Please note that this program, if you ask it to, will obediently consume all your ArcGIS Online credits.** <br />       


## Major Features
- Manage your ERDDAP data on ArcGIS Online at a variety of time-scales or data types!
- Explore data on 63 ERDDAP services with search features  
- Create ERDDAP datasets in batch. (Available now!)          
- NRT: 7 day moving window, Level 1. (Available now!)
- Glider DAC Line Segments: Create multiline HFLs from the Glider DAC ERDDAP (Available now!)
- Create multidimensional hosted imagery layers from griddap layers  
- Historical: Updated weekly, contains all data, Level 2. (Coming Soon!)
- QC Historical: QC Flags, low quality records removed, Level 3. (WIP) <br />

- Multiple install options (Updating README on program functionality soon!).
- Install directly into an AGOL notebook.
- Build AGOL environment locally. <br /> 

## Note about griddap
### To publish hosted imagery layers from griddap services, erddap2agol must be run in an ArcPy enabled environment. A local environment is recommended for best performance.

## Getting Started & Installation Options
### Installation Options
Erddap2agol can be pip installed into any active ArcGIS Conda Environment, whether that be on your desktop with an ArcGIS Python interpreter, or in an ArcGIS Online Hosted Notebook. </br>

The following is an example of the code required to access the erddap2agol CUI

!pip install https://github.com/GCOOS/erddap2agol/archive/refs/heads/main.zip <br /> 

import erddap2agol <br /> 
from erddap2agol import run <br /> 
run.cui() <br /> 


### Getting Started
Functionality in erddap2agol is divided into two major components, each with their own use requirment:
1. **Command-Line User Interface (CUI)**
- To access the CUI, follow the instructions from instillation options. </br> 
2. **Standalone Notebook functions** (e.g., `_updateNRT`)
- The underscore functions within the run module are to be used in AGOL hosted notebooks. To deploy these capabilities, simply follow the pip install/import process outlined above, then schedule the notebook to run at your desired interval. </br>

### Use the Ready-To-Go Notebook on ArcGIS Online
Access the ArcGIS Online Notebook that is configured to run the CUI [here](https://gcoos.maps.arcgis.com/home/item.html?id=5984f942bfac44e9988406193d3486ee)

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


