import os
import sys

os.environ['AGOL_HOME'] = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'venv'))

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from erddap2agol import run
from erddap2agol.src import erddap_wrangler as ec
from erddap2agol.src import data_wrangler as lm
from erddap2agol.src import das_client as dc
from erddap2agol.src import agol_wrangler as aw
from erddap2agol.src import core
from erddap2agol.src import update_manager as um


def main():
    run.cui()
    #core.updateNRT()
    

if __name__ == "__main__":
    main()