import redivis


def resumable_upload():
    dataset = redivis.user("imathews").dataset("some random name")
    if dataset.exists():
        print("Deleting dataset, already exists")
        dataset.delete()
    dataset.create(public_access_level="overview")

    table = dataset.table("A table").create(
        description="Some info", upload_merge_strategy="replace"
    )

    with open("data/wide_test.csv", "rb") as f:
        table.upload(name="local.csv", type="delimited", data=f)
