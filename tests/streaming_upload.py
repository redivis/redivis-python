import redivis


def streaming_upload():
    dataset = redivis.user("imathews").dataset("some random name")
    # if dataset.exists():
    #     print("Deleting dataset, already exists")
    #     dataset.delete()
    # dataset.create(public_access_level="overview")

    table = dataset.table("A table")

    # table.create(description="Some info", upload_merge_strategy="replace")
    upload = table.upload(name="test")
    # upload.create(type="stream", skip_bad_records=True)
    # print(upload.insert_rows([{"var1": 1}], update_schema=True))
    # upload.insert_rows([{"var2": 1}], update_schema=False)
    # upload.insert_rows([{"var1": "a", "var2": 1.2}], update_schema=False)
    # print(upload.get())
    # variables = upload.list_variables()
    # print(variables)
    # print(variables[0].get(wait_for_statistics=True))
    # print(table.list_rows())
    print(upload.list_rows())
