import os, sys, traceback

# os.environ["REDIVIS_API_ENDPOINT"] = "https://localhost:8443/api/v1"
# os.environ["REDIVIS_API_TOKEN"] = ""


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
    # resumable_upload()

    # table = (
    #     redivis.user("stanfordphs")
    #     .dataset("Coronavirus COVID - 19 Global Cases")
    #     .table("covid19_jhu_csse_summary")
    # )

    table = redivis.user("imathews").project("demo_project").table("nyc_taxis")
    query = redivis.query(
        """
        SELECT * EXCEPT(__isUpload) FROM imathews.demo_project.nyc_taxis limit 10000
    """
    )
    df = query.to_dataframe()

    print(df)

except Exception as e:
    traceback.print_exc(file=sys.stdout)
    print(e)
