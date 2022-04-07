import util


def test_list_variables():
    util.populate_test_data()
    table = util.get_table()
    variables = table.list_variables()
    print(variables)


def test_list_rows():
    util.populate_test_data()
    table = util.get_table()
    print(table.list_rows(max_results=10))


def test_to_dataframe():
    util.populate_test_data()
    table = util.get_table()
    print(table.to_dataframe(max_results=10))


def test_selected_variables():
    util.populate_test_data()
    table = util.get_table()
    print(table.to_dataframe(max_results=10, variables=["ID"]))


def test_geo_dataframe():
    util.delete_test_dataset()
    util.populate_test_data("us_states.geojsonl")
    table = util.get_table()
    df = table.to_dataframe(max_results=100)
    print(df.dtypes)
    print(df)