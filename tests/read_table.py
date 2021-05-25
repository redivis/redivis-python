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
    print(table.list_rows(limit=100))
    print(table.to_dataframe(limit=100))


def check_type_parsing():
    query = redivis.query(
        """
        SELECT
            1 as int,
            1.1 as float,
            'asfd' as string,
            TRUE as bool,
            CAST('2020-01-01' AS DATE) date,
            CAST('2020-01-01 12:00:00' AS DATETIME) date_time,
            CAST('01:00:00' AS TIME) time
            
        UNION ALL 
        SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL
            
        """
    )
    print(query.to_dataframe())
