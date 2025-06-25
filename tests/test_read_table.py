import util
import redivis


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


def test_file_stream():
    from io import TextIOWrapper

    file = redivis.file("1934-e2htjwa76.AnL5YRjeUXtSlz6fhPVXog")
    lines_read = 0
    # stream = file.stream()

    # for chunk in file.stream():

    # print(stream.closed)
    with TextIOWrapper(file.stream()) as f:
        print(f.closed)
        while not f.closed:
            line = f.readline()
            if not line:
                break
            lines_read += 1
            print(lines_read)

    print(lines_read)


def test_to_arrow_batch_iterator():
    table = redivis.table("demo.ghcn_daily_weather_data.daily_observations")
    row_count = 0
    for batch in table.to_arrow_batch_iterator(1e6):
        row_count += batch.num_rows

    assert row_count == 1e6  # table.properties.get("numRows")


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
