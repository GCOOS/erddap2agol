import os
import sys

os.environ['AGOL_HOME'] = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'venv'))

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from erddap2agol import run
from erddap2agol.src import erddap_client as ec
from erddap2agol.src import level_manager as lm
from erddap2agol.logs import updatelog as ul 

def main():

    run.cui()

if __name__ == "__main__":
    main()