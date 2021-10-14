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
    upload_and_release()
    # list_variables()
    # list_rows()
    # check_type_parsing()
    # resumable_upload()

    query = (
        redivis.user("ddi")
        .dataset("colombia_ecd")
        .table("colombia_ecd_followup_2_younger_sibling_assessment")
    )

    # geo_coords = (
    #     redivis.organization("stanfordphs")
    #     .dataset("us_zip_codes_to_longitude_and_latitude")
    #     .query(
    #         f"SELECT latitude, longitude, state, city FROM us_zip_codes_to_longitude_and_latitude WHERE Zip = 10533"
    #     )
    #     .to_dataframe()
    # )
    # geo_coords
    # query = redivis.query(
    #     """
    #     SELECT CAST('2021-01-01' AS DATE), CAST('2021-01-01T00:00:01' AS DATETIME), CAST('12:00:00' AS TIME), FALSE, 1, 1.0, 'asdf'
    #     UNION ALL
    #     SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL
    # """
    # )
    rows = query.list_rows(1)
    print(rows)
    print(query.to_dataframe())


except Exception as e:
    traceback.print_exc(file=sys.stdout)
    print(e)
