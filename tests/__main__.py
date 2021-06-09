import os, sys, traceback

os.environ["REDIVIS_API_ENDPOINT"] = "https://localhost:8443/api/v1"
os.environ["REDIVIS_API_TOKEN"] = "AAAAm+gRmxrGGquIXOehy7VHXLgOOPCX"


import logging
import importlib
import redivis

importlib.reload(redivis)

from read_table import list_variables, list_rows, check_type_parsing
from upload_dataset import upload_and_release
from query import run_global_query, run_scoped_query
from resumable_upload import resumable_upload

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


try:
    """Run tests"""
    # upload_and_release()
    # list_variables()
    # list_rows()
    # check_type_parsing()
    resumable_upload()
except Exception as e:
    traceback.print_exc(file=sys.stdout)
    print(e)
