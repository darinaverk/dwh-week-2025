from abc import abstractmethod, ABC
import typing as tp

from itertools import groupby, tee

import heapq
import re

TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


class Read(Operation):
    def __init__(self, filename: str, parser: tp.Callable[[str], TRow]) -> None:
        self.filename = filename
        self.parser = parser

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        with open(self.filename) as f:
            for line in f:
                yield self.parser(line)


class ReadIterFactory(Operation):
    def __init__(self, name: str) -> None:
        self.name = name

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in kwargs[self.name]():
            yield row


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
        for r in rows:
            yield from self.mapper(r)


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
        if not self.keys:
            yield from self.reducer((), rows)
        else:    
            for _, group in groupby(rows, key=lambda row: tuple(row.get(k) for k in self.keys)):
                yield from self.reducer(tuple(self.keys), group)



class Joiner(ABC):
    """Base class for joiners"""
    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b

    @abstractmethod
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass


class Join:
    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self.keys = keys
        self.joiner = joiner

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        other = args[0]

        def key_func(row: TRow) -> tp.Tuple[tp.Any, ...]:
            return tuple(row[k] for k in self.keys)

        grouped_a = groupby(rows, key=key_func)
        grouped_b = groupby(other, key=key_func)

        group_a: tp.Iterator[TRow]
        group_b: tp.Iterator[TRow]

        try:
            key_a, group_a = next(grouped_a)
        except StopIteration:
            key_a = None
            group_a = tp.cast(tp.Iterator[TRow], iter([]))

        try:
            key_b, group_b = next(grouped_b)
        except StopIteration:
            key_b = None
            group_b = tp.cast(tp.Iterator[TRow], iter([]))

        while key_a is not None or key_b is not None:
            if key_a is None or (key_b is not None and key_b < key_a):
                yield from self.joiner(self.keys, [], group_b)
                try:
                    key_b, group_b = next(grouped_b)
                except StopIteration:
                    key_b = None
                    group_b = tp.cast(tp.Iterator[TRow], iter([]))

            elif key_b is None or key_a < key_b:
                yield from self.joiner(self.keys, group_a, [])
                try:
                    key_a, group_a = next(grouped_a)
                except StopIteration:
                    key_a = None
                    group_a = tp.cast(tp.Iterator[TRow], iter([]))
            else:
                group_a1, group_a2 = tee(group_a)
                group_b1, group_b2 = tee(group_b)

                yield from self.joiner(self.keys, group_a1, group_b1)

                try:
                    key_a, group_a = next(grouped_a)
                except StopIteration:
                    key_a = None
                    group_a = tp.cast(tp.Iterator[TRow], iter([]))

                try:
                    key_b, group_b = next(grouped_b)
                except StopIteration:
                    key_b = None
                    group_b = tp.cast(tp.Iterator[TRow], iter([]))




# Dummy operators


class DummyMapper(Mapper):
    """Yield exactly the row passed"""
    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row


class FirstReducer(Reducer):
    """Yield only first row from passed ones"""
    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        for row in rows:
            self._first = row
            break
        if self._first is not None:
            yield self._first


# Mappers


class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols"""
    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        new = row.copy()
        val = new.get(self.column, '')
        if isinstance(val, str):
            cleaned = ''.join(ch for ch in val if ch.isalnum() or ch.isspace()).strip()
            new[self.column] = cleaned
        yield new


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
        new = row.copy()
        val = new.get(self.column)
        if isinstance(val, str):
            new[self.column] = val.lower()
        yield new

_WS_RE = re.compile(r'\s+')

class Split:
    def __init__(self, column: str, separator: tp.Optional[str] = None) -> None:
        self.column = column
        self.separator = separator

    def __call__(self, row: TRow) -> TRowsGenerator:
        raw = row.get(self.column)

        if not isinstance(raw, str):
            
            def _empty() -> TRowsGenerator:
                yield from ()
            return _empty()
        
        value: str = raw
        
        if self.separator is None:
           
            def part_gen() -> tp.Iterator[str]:
                v = value.strip()
                last_end = 0
                for m in _WS_RE.finditer(v):
                    start, end = m.span()
                    if last_end < start:
                        yield v[last_end:start]
                    last_end = end
                if last_end < len(v):
                    yield v[last_end:]
        else:
            sep = self.separator
            assert sep is not None
            
            def part_gen() -> tp.Iterator[str]:
                
                i = 0
                while True:
                    j = value.find(sep, i)
                    if j == -1:
                        yield value[i:]
                        break
                    yield value[i:j]
                    i = j + len(sep)

        def gen() -> TRowsGenerator:
            for part in part_gen():
                out_row = row.copy()
                out_row[self.column] = part
                yield out_row

        return gen()


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
        new = row.copy()
        prod = 1
        for c in self.columns:
            prod *= row.get(c, 0)
        new[self.result_column] = prod
        yield new


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
        yield {col: row[col] for col in self.columns if col in row}


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
        heap: list[tuple[tp.Any, int, tp.Dict[str, tp.Any]]] = []
        counter = 0
        for row in rows:
            val = row.get(self.column_max, 0)
            if len(heap) < self.n:
                heapq.heappush(heap, (val, counter, row))
            else:
                if val > heap[0][0]:
                    heapq.heapreplace(heap, (val, counter, row))
            counter += 1
        for val, _, row in sorted(heap, key=lambda x: x[0], reverse=True):
            yield row


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
        counts: tp.Dict[tp.Any, int] = {}
        total = 0
        first_row = None
        for row in rows:
            if first_row is None:
                first_row = row
            term = row.get(self.words_column)
            counts[term] = counts.get(term, 0) + 1
            total += 1
        if first_row is None:
            return
        for term, cnt in counts.items():
            out: tp.Dict[str, tp.Any] = {}
            for k in group_key:
                out[k] = first_row.get(k)
            out[self.words_column] = term
            out['tf'] = cnt / total
            yield out


class Count(Reducer):
    """
    Count records by key
    Example for group_key=('a',) and column='d'
        {'a': 1, 'b': 5, 'c': 2}
        {'a': 1, 'b': 6, 'c': 1}
        =>
        {'a': 1, 'd': 2}
    """
    def __init__(self, column: str) -> None:
        """
        :param column: name for result column
        """
        self.column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        count = 0
        first_row = None
        for row in rows:
            if first_row is None:
                first_row = row
            count += 1
        if first_row is None:
            return
        out: tp.Dict[str, tp.Any] = {}
        for k, v in zip(group_key, group_key):
            out[k] = first_row.get(k)
        out[self.column] = count
        yield out


class Sum(Reducer):
    """
    Sum values aggregated by key
    Example for key=('a',) and column='b'
        {'a': 1, 'b': 2, 'c': 4}
        {'a': 1, 'b': 3, 'c': 5}
        =>
        {'a': 1, 'b': 5}
    """
    def __init__(self, column: str) -> None:
        """
        :param column: name for sum column
        """
        self.column = column    

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        total = 0
        first_row = None
        for row in rows:
            if first_row is None:
                first_row = row
            total += row.get(self.column, 0)
        if first_row is None:
            return
        out: tp.Dict[str, tp.Any] = {}
        for k in group_key:
            out[k] = first_row.get(k)
        out[self.column] = total
        yield out


# Joiners

def merge_rows(
    keys: tp.Sequence[str],
    row_a: tp.Optional[TRow],
    row_b: tp.Optional[TRow],
    suffix_a: str,
    suffix_b: str,
) -> TRow:
    combined: TRow = {}
    if row_a is None and row_b is None:
        return combined

    row_a = row_a or {}
    row_b = row_b or {}

    cols_a = set(row_a)
    cols_b = set(row_b)
    collisions = (cols_a & cols_b) - set(keys)

    for k in keys:
        if k in row_a:
            combined[k] = row_a[k]
        else:
            combined[k] = row_b[k]

    for col in cols_a - set(keys):
        name = col + (suffix_a if col in collisions else '')
        combined[name] = row_a[col]

    for col in cols_b - set(keys):
        name = col + (suffix_b if col in collisions else '')
        combined[name] = row_b[col]

    return combined

class InnerJoiner(Joiner):
    """Join with inner strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_b_list = list(rows_b)
        for ra in rows_a:
            for rb in rows_b_list:
                yield merge_rows(keys, ra, rb, self._a_suffix, self._b_suffix)


class OuterJoiner(Joiner):
    """Join with outer strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_a = list(rows_a)
        rows_b = list(rows_b)
        if rows_a and rows_b:
            for ra in rows_a:
                for rb in rows_b:
                    yield merge_rows(keys, ra, rb, self._a_suffix, self._b_suffix)
        elif rows_a:
            for ra in rows_a:
                yield merge_rows(keys, ra, None, self._a_suffix, self._b_suffix)
        else:
            for rb in rows_b:
                yield merge_rows(keys, None, rb, self._a_suffix, self._b_suffix)


class LeftJoiner(Joiner):
    """Join with left strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_b = list(rows_b)
        if rows_b:
            for ra in rows_a:
                for rb in rows_b:
                    yield merge_rows(keys, ra, rb, self._a_suffix, self._b_suffix)
        else:
            for ra in rows_a:
                yield merge_rows(keys, ra, None, self._a_suffix, self._b_suffix)


class RightJoiner(Joiner):
    """Join with right strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_a = list(rows_a)
        if rows_a:
            for rb in rows_b:
                for ra in rows_a:
                    yield merge_rows(keys, ra, rb, self._a_suffix, self._b_suffix)
        else:
            for rb in rows_b:
                yield merge_rows(keys, None, rb, self._a_suffix, self._b_suffix)