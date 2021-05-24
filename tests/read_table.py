import redivis

table = (
    redivis.organization("demo")
    .dataset("cms 2014 medicare data")
    .table("hospice_providers")
)


def list_variables():
    variables = table.list_variables()
    print(variables)


def list_rows():
    rows = table.list_rows(limit=100)
    # print(rows)
    print(table.to_data_frame(limit=100))
    # print(rows[0][0])
    # print(rows[0].provider_id)
