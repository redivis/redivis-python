import redivis

def test_organization_list_datasets():
    datasets = redivis.organization("Demo").list_datasets()
    print(datasets)
    print(datasets[0].properties)
    assert True

def test_user_list_datasets():
    datasets = redivis.user("imathews").list_datasets()
    print(datasets)
    assert True