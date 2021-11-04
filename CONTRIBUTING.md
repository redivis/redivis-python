
## Contributing
For local development, clone this repository and then run
```py
python3 setup.py develop
```
You can then run the tests, e.g.: 
```
REDIVIS_API_TOKEN=YOUR_TOKEN pytest -s --disable-warnings [-k test_prefix]
```
#### To upload to PyPi:  
First, update the version in setup.py. Then:
```
python3 setup.py upload
```