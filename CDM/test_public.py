import yt.wrapper as yt
import pytest
from cdm import get_table_path
import random
import os


class Case:
    def __init__(self, name: str):
        self._name = name

    def __str__(self) -> str:
        return 'test_{}'.format(self._name)


TEST_CASES = [
    Case(name='basic')
]

@pytest.mark.parametrize('test_case', TEST_CASES, ids=str)
def test_ods(test_case: Case) -> None:

    yt.config["proxy"]["enable_proxy_discovery"] = False

    path_test = get_table_path()

    assert yt.exists(path_test)

    path = os.environ.get('TABLE_CDM')

    assert (path != path_test)

    schema = yt.schema.TableSchema.from_yson_type(yt.get(path + "/@schema"))
    schema_test = yt.schema.TableSchema.from_yson_type(yt.get(path_test + "/@schema"))

    test = []

    for i in schema.columns:
        if hash(i.name + str(i.type)) in test:
            test.remove(hash(i.name + str(i.type)))
        else:
            test.append(hash(i.name + str(i.type)))

    for i in schema_test.columns:
        if hash(i.name + str(i.type)) in test:
            test.remove(hash(i.name + str(i.type)))
        else:
            test.append(hash(i.name + str(i.type)))

    assert (len(test) == 0)

    row_count = yt.get(path + "/@row_count")
    row_count_test = yt.get(path_test + "/@row_count")

    assert (row_count == row_count_test)

    test = []

    rows = yt.read_table(path + "[:50]")
    for i in rows:
        h = hash(str(i))
        if h in test:
            test.remove(h)
        else:
            test.append(h)

    rows = yt.read_table(path_test + "[:50]")
    for i in rows:
        h = hash(str(i))
        if h in test:
            test.remove(h)
        else:
            test.append(h)

    assert (len(test) == 0)

    test = []

    keys = []

    for i in range(1, 10):
        keys.append(str(random.randint(2, 1000)))


    rows = yt.read_table(path + "["+",".join(keys)+"]")
    for i in rows:
        h = hash(str(i))
        if h in test:
            test.remove(h)
        else:
            test.append(h)

    rows = yt.read_table(path_test + "["+",".join(keys)+"]")
    for i in rows:
        h = hash(str(i))
        if h in test:
            test.remove(h)
        else:
            test.append(h)

    assert (len(test) == 0)
