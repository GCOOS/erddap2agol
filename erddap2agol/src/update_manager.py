from arcgis.gis import GIS
from dataclasses import dataclass, field
from typing import Optional, Dict
import sys

@dataclass
class UpdateManager:
    gis: Optional[GIS] = None
    # Remove old agol_ids, dataset_ids in favor of a single dict.
    # Key = dataset_id, Value = { 'base_url': ..., 'agol_id': ... }
    datasets: Dict[str, Dict[str, Optional[str]]] = field(default_factory=dict)

    def __post_init__(self):
        self.connect()
        # probably dont want to call search content  on init here
        self.searchContent()

    def connect(self) -> None:
        """Establish AGOL connection"""
        try:
            self.gis = GIS("home")
            print(
                f"\nSuccessfully connected to {self.gis.properties.portalName} "
                # f"as {self.gis.users.me.username}."
            )
        except Exception as e:
            print(f"UH OH! AGOL connection error: {e}")
            print(f"Since that's half the program, sys.exit!")
            sys.exit

    def searchContent(self) -> None:
        """
        Search AGOL content and populate self.datasets with:
            { dataset_id: { 'base_url': ..., 'agol_id': ... } }
        """
        tags = ["erddap2agol", "e2a_nrt"]
        try:
            tag_query = " AND ".join(f'tags:"{tag}"' for tag in tags)
            search_query = (
                f"({tag_query}) AND owner:{self.gis.users.me.username} AND type:Feature Service"
            )

            # Commented out this print statement because we will want to eventually search org
            # will need to verify user role for overwriting other datasets
            #print(f"Searching for NRT datasets from: {self.gis.users.me.username}")
            search_results = self.gis.content.search(query=search_query, max_items=5000)

            if not search_results:
                print(f"No items found with tags {tags} for the logged-in user.")
                pass

            # Populate self.datasets dict from attributes of the content item
            for item in search_results:
                # e2a items should always retain the data id, whatever comes after that can be customized
                dataset_id = item.title.split(" ")[0] if ' ' in item.title else item.title
                base_url = None

                # Check tags for one starting with https://
                for tagz in item.tags:
                    if tagz.lower().startswith("https://"):
                        base_url = tagz
                
                # Store the info by dataset_id
                self.datasets[dataset_id] = {
                    "base_url": base_url,
                    "agol_id": item.id
                }

            print(f"\nFound {len(self.datasets)} NRT datasets")

        except Exception as e:
            print(f"An error occurred while searching for items: {e}")

