import redivis
import util


def test_run_global_query():
    util.populate_test_data()
    query = redivis.query(
        f"SELECT * EXCEPT(_upload_name) FROM `{util.get_user_name()}.{util.get_dataset_name()}:next.{util.get_table_name()}` LIMIT 100"
    )
    print(query.list_rows(max_results=10))
    print(query.to_dataframe(max_results=10))
    assert True


def test_run_scoped_query():
    query = util.get_dataset().query(
        f"SELECT * EXCEPT(__upload_name) FROM `{util.get_table_name()}` LIMIT 100"
    )

    print(query.list_rows(max_results=10))
    print(query.to_dataframe(max_results=10))
    assert True

def test_query_list_rows():
    rows = redivis.query(
        """
            SELECT 1 + 1 AS some_number, 'foo' AS some_string
            UNION ALL 
            SELECT 4, 'bar'
        """
    ).list_rows()
    print(rows)

def test_query_empty_result():
    query = redivis.query(
        """
            SELECT * FROM (
                SELECT 1 + 1 AS val
            )sub
            WHERE FALSE
        """
    )
    print(query.list_rows())
    print(query.to_dataframe())

def test_query_underscore_variable_name():
    query = redivis.query(
        """
            SELECT 1 + 1 AS _val
        """
    )
    print(query.list_rows())

def test_check_type_parsing():
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
