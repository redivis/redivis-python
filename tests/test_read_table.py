import util
import redivis


def test_list_variables():
    util.populate_test_data()
    table = util.get_table()
    variables = table.list_variables()
    print(variables)


def test_make_rows_request():
    util.populate_test_data()
    table = util.get_table()
    print(table.to_pandas_dataframe(max_results=10))


def test_to_dataframe():
    util.populate_test_data()
    table = util.get_table()
    print(table.to_dataframe(max_results=10))


def test_read_streams():
    import redivis
    from concurrent.futures import ThreadPoolExecutor, as_completed

    table = redivis.table(
        "demo.cms_2014_medicare_data:349j.physicians_and_other_supplier:kn00"
    )

    streams = table.to_read_streams(
        variables=["average_submitted_chrg_amt"], target_count=4
    )

    def process_stream(stream):
        """Process a single stream, returning the sum and count for the column."""
        total = 0
        count = 0
        # Each stream can be read as an Arrow record batch reader

        for batch in stream.to_arrow_batch_iterator():
            column = batch.column("average_submitted_chrg_amt")
            # Filter out nulls
            valid = column.drop_null()
            total += valid.to_pylist().__len__() and sum(valid.to_pylist()) or 0
            count += len(valid)
        return total, count

    # Process all streams in parallel using a thread pool
    grand_total = 0
    grand_count = 0

    with ThreadPoolExecutor(max_workers=len(streams)) as executor:
        futures = {
            executor.submit(process_stream, stream): stream for stream in streams
        }
        for future in as_completed(futures):
            total, count = future.result()
            grand_total += total
            grand_count += count

    average = grand_total / grand_count if grand_count > 0 else 0
    print(f"Average submitted charge amount: ${average:.2f}")
    print(f"Computed across {grand_count} rows using {len(streams)} parallel streams")


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
