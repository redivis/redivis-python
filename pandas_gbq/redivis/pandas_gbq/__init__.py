name = "pandas-gbq"

import inspect
import pandas_gbq as base_pandas_gbq
import re
import redivis_bigquery

source = inspect.getsource(base_pandas_gbq.gbq)
new_source = 'import redivis_bigquery\n'+re.sub(
	re.compile('return bigquery\.Client\(.*?\)', re.S),
	'return redivis_bigquery.Client()',
	source,
)
exec(new_source, base_pandas_gbq.gbq.__dict__)

pandas_gbq = base_pandas_gbq
del base_pandas_gbq
del source
del new_source
