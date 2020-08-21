import sys
import os
scriptpath = "./bigquery/"

# Add the directory containing your module to the Python path (wants absolute paths)
sys.path.append(os.path.abspath(scriptpath))

from redivis import redipackage
import pandas
import requests

user_1 = redipackage.User("kevin","EPA-2015")
dataset_l = user_1.list_datasets("EPA-2015")
tables_l = dataset_l[0][0].list_tables()
#print(tables_l)

user_2 = redipackage.Dataset("kevin", "EPA-2015")

user_3 = redipackage.Table("kevin", "EPA-2015")
print(user_3.list_variables("EPA-2015", 5))