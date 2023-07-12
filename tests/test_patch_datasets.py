
import util

def test_patch_variable():
    util.populate_test_data()
    table = util.get_table()
    variable = table.variable('Key')
    variable.update(label="some label", description="some description", value_labels=[{ "value": "key1", "label": "a value label"}])