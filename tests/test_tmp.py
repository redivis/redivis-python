import redivis
from PIL import Image
from io import BytesIO

def test_tmp():
    """
    For running temp tests during dev
    """
    table = redivis.user('inphoredivis').dataset('543y326:sm48:v1_0').table('asdf:zy3r')
    # files = table.list_files()
    # print(files)
    table.download_files(path='/Users/ian/Desktop/test')
    # f = redivis.file("zy3r-4vp82etdm.7mUiBBlR13PNJeq686QFxQ")
    # f.get()
    # print(f)
    # f.download('/Users/ian/Desktop')
    # stream = f.read()
    # print(stream)
    # i = Image.open(stream)
    # print(i)
    # return
