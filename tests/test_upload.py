import util
import os
import pandas


def test_linebreaks_in_cell():
    dataset = util.create_test_dataset()
    util.clear_test_data()
    table = util.get_table().create(
        description="Some info", upload_merge_strategy="replace"
    )

    with open("tests/data/line_breaks.csv", "rb") as f:
        table.upload(name="test.csv",).create(
            type="delimited",
            allow_quoted_newlines=True,
        ).upload_file(
            data=f,
        )


def test_upload_remove_on_failure():
    dataset = util.create_test_dataset()
    util.clear_test_data()
    table = util.get_table().create(
        description="Some info", upload_merge_strategy="replace"
    )

    table.upload(name="tiny.csv",).create(
        type="delimited",
        allow_quoted_newlines=False,
    ).upload_file(data='a,b\n1,2\n3,"4\n5"', remove_on_fail=True)


def test_upload_string():
    dataset = util.create_test_dataset()
    util.clear_test_data()
    table = util.get_table().create(
        description="Some info", upload_merge_strategy="replace"
    )

    table.upload(name="tiny.csv",).create(
        type="delimited",
        allow_quoted_newlines=True,
    ).upload_file(
        data='a,b\n1,2\n3,"4\n5"',
    )


def test_upload_and_release():
    dataset = util.create_test_dataset()
    util.clear_test_data()
    table = util.get_table().create(
        description="Some info", upload_merge_strategy="replace"
    )

    with open("tests/data/tiny.csv", "rb") as f:
        table.upload(name="tiny.csv").create(type="delimited").upload_file(data=f)

    dataset.release()

    dataset = dataset.create_next_version()
    table = dataset.table(util.get_table_name())

    with open("tests/data/tiny.csv", "rb") as f:
        table.upload(name="tiny.csv", type="delimited", data=f)

    dataset.release()


def test_streaming_upload():
    util.create_test_dataset()
    util.clear_test_data()

    table = util.get_table()

    table.create(description="Some info", upload_merge_strategy="replace")
    upload = table.upload(name="test")
    upload.create(type="stream")

    print(upload.insert_rows([{"var1": 1, "var2": "a"}], update_schema=True))

    upload.insert_rows([{"var2": 1}], update_schema=True)
    upload.insert_rows([{"var1": "a", "var2": 1.2}], update_schema=True)

    variables = upload.list_variables()
    print(variables)
    print(variables[0].get(wait_for_statistics=True))
    print(table.list_rows())
    print(upload.list_rows())


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

    with open(os.path.join(os.path.dirname(__file__), "data/wide_test.csv"), "rb") as f:
        table.upload(name="local.csv", type="delimited", data=f)

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