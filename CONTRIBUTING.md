
## Contributing
For local development, clone this repository and then run
```py
source ./venv/bin/activate && python setup.py develop --user
# OR
source ./venv/bin/activate &&  python3 -m pip install .  
```
You can then run the tests, e.g.: 
```
REDIVIS_API_ENDPOINT=https://localhost:8443/api/v1 REDIVIS_API_TOKEN=YOUR_TOKEN pytest -s --disable-warnings [-k test_prefix]
```
#### To upload to PyPi:  
First, update the version in setup.py. Then:
```
python3 setup.py upload
```