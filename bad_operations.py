import string
from abc import abstractmethod, ABC
from copy import deepcopy
from itertools import groupby
from operator import itemgetter

import typing as tp

TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


# Operations


class Mapper(ABC):
    """Base class for mappers"""
    @abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """
        :param row: one table row
        """
        pass


class Map(Operation):
    def __init__(self, mapper: Mapper) -> None:
        self.mapper = mapper

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in list(rows):
            yield from self.mapper(row)


class Reducer(ABC):
    """Base class for reducers"""
    @abstractmethod
    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        pass


class Reduce(Operation):
    def __init__(self, reducer: Reducer, keys: tp.Sequence[str]) -> None:
        self.reducer = reducer
        self.keys = keys

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for _, group in groupby(rows, key=itemgetter(*self.keys)):
            yield from self.reducer(tuple(self.keys), list(group))


class Joiner(ABC):
    """Base class for joiners"""
    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b

    def _merge_rows(self, keys: tp.Sequence[str], row_a: TRow, row_b: TRow) -> TRow:
        common_keys = (row_a.keys() & row_b.keys()) - set(keys)

        new_row: TRow = {}
        for k, v in row_a.items():
            if k in common_keys:
                new_row[k + self._a_suffix] = v
            else:
                new_row[k] = v

        for k, v in row_b.items():
            if k in common_keys:
                new_row[k + self._b_suffix] = v
            else:
                new_row[k] = v

        return new_row

    @abstractmethod
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass


class Join(Operation):
    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self.keys = keys
        self.joiner = joiner

    def grouper(self, records: TRowsIterable) -> \
            tp.Generator[tuple[tuple[tp.Any, ...] | None, TRowsIterable | None], None, None]:
        for key, group in groupby(records, key=itemgetter(*self.keys)):
            yield key, list(group)
        yield None, None

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        first_grouper = self.grouper(rows)
        second_grouper = self.grouper(args[0])
        first_key, first_g = next(first_grouper)
        second_key, second_g = next(second_grouper)

        while first_key is not None and second_key is not None:
            if first_key < second_key:
                for row in self.joiner(self.keys, first_g or [], []):
                    yield row
                first_key, first_g = next(first_grouper)
                continue

            if first_key == second_key:
                for row in self.joiner(self.keys, first_g or [], second_g or []):
                    yield row
                first_key, first_g = next(first_grouper)
                second_key, second_g = next(second_grouper)
                continue

            if first_key > second_key:
                for row in self.joiner(self.keys, [], second_g or []):
                    yield row
                second_key, second_g = next(second_grouper)
                continue

        while first_key is not None:
            for row in self.joiner(self.keys, first_g or [], []):
                yield row
            first_key, first_g = next(first_grouper)

        while second_key is not None:
            for row in self.joiner(self.keys, [], second_g or []):
                yield row
            second_key, second_g = next(second_grouper)


# Dummy operators


class DummyMapper(Mapper):
    """Yield exactly the row passed"""
    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row


class FirstReducer(Reducer):
    """Yield only first row from passed ones"""
    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        for row in list(rows):
            yield row
            break


# Mappers


class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols"""
    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    @staticmethod
    def _filter_punctuation(txt: str) -> str:
        punctuation_set = set(string.punctuation)
        return ''.join([char for char in txt if char not in punctuation_set])

    def __call__(self, row: TRow) -> TRowsGenerator:
        row_copy = deepcopy(row)
        row_copy[self.column] = self._filter_punctuation(row[self.column])
        yield row_copy


class LowerCase(Mapper):
    """Replace column value with value in lower case"""
    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    @staticmethod
    def _lower_case(txt: str) -> str:
        return txt.lower()

    def __call__(self, row: TRow) -> TRowsGenerator:
        row_copy = deepcopy(row)
        row_copy[self.column] = self._lower_case(row[self.column])
        yield row_copy


class Split(Mapper):
    """Split row on multiple rows by separator"""
    def __init__(self, column: str, separator: str | None = None) -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self.column = column
        self.separator = separator

    def __call__(self, row: TRow) -> TRowsGenerator:
        copies = []
        for chunk in row[self.column].split(self.separator):
            row_copy = deepcopy(row)
            row_copy[self.column] = chunk
            copies.append(row_copy)

        yield from copies


class Product(Mapper):
    """Calculates product of multiple columns"""
    def __init__(self, columns: tp.Sequence[str], result_column: str = 'product') -> None:
        """
        :param columns: column names to product
        :param result_column: column name to save product in
        """
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        new_row = deepcopy(row)
        new_row[self.result_column] = 1
        for column in self.columns:
            new_row[self.result_column] *= row[column]
        yield new_row


class Filter(Mapper):
    """Remove records that don't satisfy some condition"""
    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:
        """
        :param condition: if condition is not true - remove record
        """
        self.condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self.condition(row):
            yield row


class Project(Mapper):
    """Leave only mentioned columns"""
    def __init__(self, columns: tp.Sequence[str]) -> None:
        """
        :param columns: names of columns
        """
        self.columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield {k: row[k] for k in self.columns}


# Reducers


class TopN(Reducer):
    """Calculate top N by value"""
    def __init__(self, column: str, n: int) -> None:
        """
        :param column: column name to get top by
        :param n: number of top values to extract
        """
        self.column_max = column
        self.n = n

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        yield from sorted(rows, reverse=True, key=itemgetter(self.column_max))[: self.n]


class TermFrequency(Reducer):
    """Calculate frequency of values in column"""
    def __init__(self, words_column: str, result_column: str = 'tf') -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self.words_column = words_column
        self.result_column = result_column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        words_dict: dict[str, int] = {}
        last_row: TRow = {}
        for row in list(rows):
            word = row[self.words_column]
            if word not in words_dict:
                words_dict[word] = 0
            words_dict[word] += 1
            last_row = row

        total = sum(words_dict.values())
        for k, v in words_dict.items():
            new_row = {k: v for k, v in last_row.items() if k in group_key}
            new_row[self.words_column] = k
            new_row[self.result_column] = v / total
            yield new_row


class Count(Reducer):
    """Count rows passed and yield single row as a result"""
    def __init__(self, column: str) -> None:
        """
        :param column: name of column to count
        """
        self.column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        state = {self.column: 0}
        last_row: TRow = {}
        for row in list(rows):
            state[self.column] += 1
            last_row = row

        for key in group_key:
            state[key] = last_row[key]
        yield state


class Sum(Reducer):
    """Sum values in column passed and yield single row as a result"""
    def __init__(self, column: str) -> None:
        """
        :param column: name of column to sum
        """
        self.column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        state = {self.column: 0}
        last_row: TRow = {}
        for row in list(rows):
            state[self.column] += row[self.column]
            last_row = row

        for key in group_key:
            state[key] = last_row[key]
        yield state


# Joiners


class InnerJoiner(Joiner):
    """Join with inner strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        cache = list(rows_b)
        for row_a in list(rows_a):
            for row_b in cache:
                yield self._merge_rows(keys, row_a, row_b)


class OuterJoiner(Joiner):
    """Join with outer strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        cache_a = list(rows_a)
        cache_b = list(rows_b)

        if not cache_a:
            for row_b in cache_b:
                yield row_b

        if not cache_b:
            for row_a in cache_a:
                yield row_a

        for row_a in cache_a:
            for row_b in cache_b:
                yield self._merge_rows(keys, row_a, row_b)


class LeftJoiner(Joiner):
    """Join with left strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        cache_b = list(rows_b)
        for row_a in list(rows_a):
            for row_b in cache_b:
                yield self._merge_rows(keys, row_a, row_b)
            if not cache_b:
                yield row_a


class RightJoiner(Joiner):
    """Join with right strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        cache_a = list(rows_a)
        for row_b in list(rows_b):
            for row_a in cache_a:
                yield self._merge_rows(keys, row_b, row_a)
            if not cache_a:
                yield row_b
