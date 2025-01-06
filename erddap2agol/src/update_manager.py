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
                return

            # Populate self.datasets dict
            for item in search_results:
                dataset_id = item.title
                base_url = None

                # Check tags for one starting with https://
                for t in item.tags:
                    if t.lower().startswith("https://"):
                        base_url = t
                        break

                # Store the info by dataset_id
                self.datasets[dataset_id] = {
                    "base_url": base_url,
                    "agol_id": item.id
                }

            print(f"\nFound {len(self.datasets)} NRT datasets")

        except Exception as e:
            print(f"An error occurred while searching for items: {e}")

