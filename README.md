# REDIPY


#Table of Contents
------------------

* Introduction
* Requirements
* Recommended Modules
* Installation
* Troubleshooting



-------------------
Introduction 

Redipy contains the Redivis client libraries for python. Contains support for interfacing with tables stored in BigQuery.

Full documentation for Redivis BigQuery may be found at: https://apidocs.redivis.com/client-libraries/redipy.bigquery

The remainder of the package contains a client library useful for retrieving data from the Redivis REST API. 
The package provides methods to retrieve data according to the General Structure referenced here: 
https://apidocs.redivis.com/referencing-resources#general-structure 

Methods within each class with the rely on GET requests to the api to retrieve data.

The package also includes an upload class which allows clients to upload data to Redivis.

___________________
Requirements 

Installation of this package is dependent upon installation of redivis bigquery
pipenv install -e "git+https://github.com/redivis/redipy.git#egg=redivis-bigquery&subdirectory=bigquery"

-------------------
Installation 

This package can be downloaded from pypi using pip install

--------------------
Troubleshooting

Be mindful of datetime formatting errors.


