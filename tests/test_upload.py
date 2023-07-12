import util
import pandas
import os


def test_linebreaks_in_cell():
    dataset = util.create_test_dataset()
    util.clear_test_data()
    table = util.get_table().create(
        description="Some info", upload_merge_strategy="replace"
    )

    with open("tests/data/line_breaks.csv", "rb") as f:
        table.upload(name="test.csv",).create(
            data=f,
            type="delimited",
            has_quoted_newlines=True,
        )


def test_upload_remove_on_failure():
    dataset = util.create_test_dataset()
    util.clear_test_data()
    table = util.get_table().create(
        description="Some info", upload_merge_strategy="replace"
    )

    table.upload(name="tiny.csv",).create(
        data='a,b\n1,2\n3,"4\n5"',
        remove_on_fail=True,
        type="delimited",
        has_quoted_newlines=False,
    )

def test_external_transfer():
    dataset = util.create_test_dataset()
    util.clear_test_data()
    table = util.get_table().create(
        description="Some info", upload_merge_strategy="replace"
    )

    table.upload(name="test.csv", ).create(
        transfer_specification={"sourceType": "gcs", "sourcePath": "redivis-data-dev/test.csv.gz", "identity": "iaan@redivis.com"},
    )

def test_upload_string():
    dataset = util.create_test_dataset()
    util.clear_test_data()
    table = util.get_table().create(
        description="Some info", upload_merge_strategy="replace"
    )

    table.upload(name="tiny.csv",).create(
        data='a,b\n1,2\n3,"4\n5"',
        type="delimited",
        has_quoted_newlines=True,
    )

def test_upload_large_string():
    dataset = util.create_test_dataset()
    util.clear_test_data()
    table = util.get_table().create(
        description="Some info", upload_merge_strategy="replace"
    )

    with open("tests/data/us_counties_500k.geojson", "rb") as f:
        data = f.read()
        table.upload(name="us_counties_500k.geojson").create(data=data, wait_for_finish=True)

def test_upload_and_release():
    dataset = util.create_test_dataset()
    util.clear_test_data()
    table = util.get_table().create(
        description="Some info", upload_merge_strategy="replace"
    )

    with open("tests/data/tiny.csv", "rb") as f:
        table.upload(name="tiny.csv").create(data=f, type="delimited", wait_for_finish=True)

    dataset.release()

    dataset = dataset.create_next_version()
    table = dataset.table(util.get_table_name())

    with open("tests/data/tiny.csv", "rb") as f:
        table.upload(name="tiny.csv", type="delimited").create(data=f)

    dataset.release()


def test_streaming_upload():
    util.create_test_dataset()
    util.clear_test_data()

    table = util.get_table()

    table.create(description="Some info", upload_merge_strategy="replace")
    upload = table.upload(name="test")
    upload.create(type="stream")

    print(
        upload.insert_rows([{"var1": 1, "var2": "a", "var3": None}], update_schema=True)
    )

    upload.insert_rows([{"var2": 1}], update_schema=True)
    upload.insert_rows(
        [{"var1": "a", "var2": 1.2, "var3": "2020-01-01"}], update_schema=True
    )
    upload.insert_rows(
        [{"var1": "a", "var2": 1.2, "var3": "2020-01-01"}], update_schema=False
    )
    upload.insert_rows(
        [{"var1": "a", "var2": 1.2, "var3": "2020-01-01T00:00:00"}], update_schema=True
    )
    variables = upload.list_variables()
    print(variables)
    print(variables[0].get(wait_for_statistics=True))
    print(table.list_rows())
    print(upload.list_rows())

def test_redivis_upload():
    util.create_test_dataset()
    util.clear_test_data()

    table = util.get_table()

    table.create(description="Some info", upload_merge_strategy="replace")
    upload = table.upload(name="test")
    upload.create(transfer_specification={"sourceType":'redivis', "sourcePath":"demo.novel_corona_virus_2019_dataset.covid_19_data"})

def test_streaming_schema_upload():
    util.create_test_dataset()
    util.clear_test_data()

    table = util.get_table()

    table.create(description="Some info", upload_merge_strategy="replace")
    upload = table.upload(name="test")
    upload.create(
        type="stream",
        schema=[
            {"name": "var1", "type": "integer"},
            {"name": "var2", "type": "integer"},
        ],
    )

    print(upload.insert_rows([{"var1": 1}], update_schema=False))

    upload.insert_rows([{"var2": 1}], update_schema=False)
    upload.insert_rows([{"var1": "a", "var2": 1.2}], update_schema=True)

    variables = upload.list_variables()
    print(variables)
    print(variables[0].get(wait_for_statistics=True))
    print(table.list_rows())
    print(upload.list_rows())


def test_resumable_upload():
    util.create_test_dataset()
    util.clear_test_data()

    table = util.get_table()
    table.create(description="Some info", upload_merge_strategy="replace")

    with open(os.path.join(os.path.dirname(__file__), "us_counties_500k.geojson"), "rb") as f:
        table.upload(name="us_counties_500k.geojson", type="geojson", data=f)

    print(table.upload("local.csv").to_dataframe(100))


def test_streaming_performance():
    util.create_test_dataset()
    util.clear_test_data()

    table = util.get_table()

    df = pandas.read_csv(os.path.join(os.path.dirname(__file__), "data/tiny.csv"))
    data = df.to_dict(orient="records")
    table.create(description="Some info")
    upload = table.upload(name="test")
    upload.create(type="stream")
    import time

    start_time = time.time()

    for x in range(20):
        upload.insert_rows(data, update_schema=True)

    print("--- %s seconds ---" % (time.time() - start_time))


def test_streaming_after_upload():
    util.create_test_dataset()
    util.clear_test_data()

    table = util.get_table()

    df = pandas.read_csv(os.path.join(os.path.dirname(__file__), "data/tiny.csv"))
    data = df.to_dict(orient="records")
    table.create(description="Some info")
    upload = table.upload(name="seed_file").create(type="delimited")

    with open("tests/data/tiny.csv", "rb") as f:
        upload.upload_file(data=f)

    upload = table.upload(name="streamed_date").create(
        type="stream",
        schema=[
            {
                "name": "id",
                "type": "integer",
            },
            {
                "name": "Numeric",
                "type": "float",
            },
            {
                "name": "Key",
                "type": "string",
            },
            {
                "name": "Timestamp",
                "type": "string",
            },
            {
                "name": "Year",
                "type": "integer",
            },
            {
                "name": "Boolean",
                "type": "boolean",
            },
            {
                "name": "Text",
                "type": "string",
            },
        ],
    )
    upload.insert_rows(data, update_schema=False)

def test_upload_raw_file():
    dataset = util.create_test_dataset()
    util.clear_test_data()
    table = util.get_table().create(is_file_index=True)

    with open("tests/data/tiny.csv", "rb") as f:
        file = table.add_file(name="tiny.csv", data=f)
        print(file)


def test_upload_metadata():
    dataset = util.create_test_dataset()
    util.clear_test_data()
    table = util.get_table().create()

    with open("tests/data/tiny.csv", "rb") as f:
        upload = table.upload("tiny.csv").create(f, metadata={"id": { "label": "foo", "description": "bar", "valueLabels": { "1": "hello world"}}})
