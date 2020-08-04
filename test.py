import sys
import os
scriptpath = "./bigquery/"

# Add the directory containing your module to the Python path (wants absolute paths)
sys.path.append(os.path.abspath(scriptpath))

from redivis import redipackage

data_sets = redipackage.Dataset()
data_lists = data_sets.list_datasets()
