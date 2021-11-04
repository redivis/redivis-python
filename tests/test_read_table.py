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
