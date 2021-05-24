import redivis


def run_global_query():
    query = redivis.query(
        "SELECT * FROM demo.cms_2014_medicare_data.hospice_providers LIMIT 100"
    )
    print(query.list_rows(limit=100))
    print(query.to_data_frame(limit=100))


def run_scoped_query():
    query = (
        redivis.organization("demo")
        .dataset("cms 2014 medicare data")
        .query("SELECT * FROM hospice_providers LIMIT 100")
    )

    print(query.list_rows(limit=100))
    print(query.to_data_frame(limit=100))
