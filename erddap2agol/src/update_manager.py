from . import erddap_client as ec
from . import das_client as dc
from logs import updatelog as ul
from src.utils import OverwriteFS
from arcgis.gis import GIS

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, List
from io import StringIO
import datetime, requests, re, math, os, pandas as pd
from datetime import timedelta, datetime


@dataclass
class UpdateManager:
    gis: Optional[GIS] = None
    items: Dict[str, str] = field(default_factory=dict)
    agol_ids: List[str] = None
    dataset_ids: List[str] = field(default_factory=list)

    def __post_init__(self):
        self.connect()

    def connect(self) -> None:
        """Establish AGOL connection"""
        try:
            self.gis = GIS("home")
            gis = self.gis
            print("\nSuccesfully connected to " + gis.properties.portalName + " on " + gis.properties.customBaseUrl)
        except Exception as e:
            print(f"AGOL connection error: {e}")

    # we will want to expand this function to optionally search at an orginizational level
    def searchContent(self) -> None:
        """Search AGOL content and populate items dictionary"""
        tags = ["erddap2agol", "e2a_nrt"]
        gis = self.gis
        
        try:
            
            tag_query = ' AND '.join(f'tags:"{tag}"' for tag in tags)
            search_query = f'({tag_query}) AND owner:{gis.users.me.username} AND type:Feature Service'
            
            print(f"Searching with query: {search_query}")
            search_results = gis.content.search(query=search_query, max_items=1000)

            if not search_results:
                print(f"No items found with tags {tags} for the logged-in user.")
                return

            # Populate items dictionary
            for item in search_results:
                dataset_id = item.title
                self.items[item.id] = dataset_id
                print(f"Found: {dataset_id} (ID: {item.id})")

            print(f"\nFound {len(self.items)} total items")
        
        except Exception as e:
            print(f"An error occurred while searching for items: {e}")


