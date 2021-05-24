import redivis


def upload_and_release():
    dataset = redivis.user("imathews").dataset("some random name")
    if dataset.exists():
        print("Deleting dataset, already exists")
        dataset.delete()
    dataset.create(public_access_level="overview")
    print(dataset)

    table = dataset.table("A table").create(
        description="Some info", upload_merge_strategy="replace"
    )

    print(table)

    table.upload(name="tiny.csv", type="delimited", data="a,b\n1,2\n3,4")
    with open("tiny.csv", "rb") as f:
        table.upload(name="tiny.csv", type="delimited", data=f)

    dataset.release()

    dataset = dataset.create_next_version()
    table = dataset.table("a table")

    with open("tiny.csv", "rb") as f:
        table.upload(name="tiny.csv", type="delimited", data=f)

    dataset.release()
