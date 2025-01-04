import logging
import re
import string
from collections import namedtuple
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

valid_chars_name: str = string.ascii_letters + string.digits + "._`*<>+-'"
valid_chars_fn: str = valid_chars_name + "[](),=!?:/ \n\t\r"


@dataclass
class TableIndex:
    """Defines a CH table INDEX"""

    name: str
    expr: str
    type_full: str
    granularity: Optional[str] = None

    def to_datafile(self):
        granularity_expr = f"GRANULARITY {self.granularity}" if self.granularity else ""
        return f"{self.name} {self.expr} TYPE {self.type_full} {granularity_expr}"

    def to_sql(self):
        return f"INDEX {self.to_datafile()}"

    def add_index_sql(self):
        return f"ADD {self.to_sql()}"

    def drop_index_sql(self):
        return f"DROP INDEX IF EXISTS {self.name}"

    def materialize_index_sql(self):
        return f"MATERIALIZE INDEX IF EXISTS {self.name}"

    def clear_index_sql(self):
        return f"CLEAR INDEX IF EXISTS {self.name}"


@dataclass
class TableProjection:
    """Defines a CH table PROJECTION"""

    name: str
    expr: str

    def to_datafile(self):
        return f"{self.name} ({self.expr})"

    def to_sql(self):
        return f"PROJECTION {self.to_datafile()}"

    def add_projection_sql(self):
        return f"ADD {self.to_sql()}"

    def drop_projection_sql(self):
        return f"DROP PROJECTION IF EXISTS {self.name}"

    def materialize_projection_sql(self):
        return f"MATERIALIZE PROJECTION IF EXISTS {self.name}"

    def clear_projection_sql(self):
        return f"CLEAR PROJECTION IF EXISTS {self.name}"


def as_subquery(sql: str) -> str:
    return f"""(\n{sql}\n)"""


def get_format(sql: str) -> Optional[str]:
    """
    retrieves FORMAT from CH sql
    >>> get_format('select * from test')
    >>> get_format('select * from test formAt JSON')
    'JSON'
    """
    FORMAT_RE = r"\s+format\s+(\w+)\s*$"
    sql = sql.strip()
    format = re.findall(FORMAT_RE, sql, re.I)
    return format[0] if format else None


def get_format_group(sql: str) -> str:
    """
    retrieves FORMAT group from CH sql
    >>> get_format_group('select * from test')
    ''
    >>> get_format_group('select * from test formAt JSON')
    ' formAt JSON'
    """
    FORMAT_RE = r"\s+format\s+(\w+)\s*$"
    sql = sql.strip()
    format = re.search(FORMAT_RE, sql, re.I)
    return format.group() if format else ""


def wrap_finalize_aggregation(sql: str, describe_result: Dict[str, Any], fm_group: Optional[str] = None) -> str:
    if not fm_group:
        fm_group = get_format_group(sql)
        sql = sql[0 : -len(fm_group)] if fm_group else sql

    qq: str
    if describe_result:
        columns: List[str] = [
            f"finalizeAggregation({c['name']}) as {c['name']}"
            if "Aggregate" in c["type"] and "SimpleAggregate" not in c["type"]
            else f"{c['name']}"
            for c in describe_result["data"]
        ]
        columns_as_string: str = ",\n\t".join(columns)
        sql = sql.replace("\n", "\n\t")
        qq = f"SELECT \n\t{columns_as_string} \nFROM ({sql} \n) {fm_group}"
    else:
        qq = sql
    return qq


def remove_format(sql: str) -> str:
    """
    removes FORMAT from CH sql
    >>> remove_format('select * from test')
    'select * from test'
    >>> remove_format('select * from test formAt JSON')
    'select * from test'
    """
    FORMAT_RE = r"\s+(format)\s+(\w+)\s*$"
    sql = sql.strip()
    return re.sub(FORMAT_RE, "", sql, flags=re.I)


def col_name(name: str, backquotes: bool = True) -> str:
    """
    >>> col_name('`test`', True)
    '`test`'
    >>> col_name('`test`', False)
    'test'
    >>> col_name('test', True)
    '`test`'
    >>> col_name('test', False)
    'test'
    >>> col_name('', True)
    ''
    >>> col_name('', False)
    ''
    """
    if not name:
        return name
    if name[0] == "`" and name[-1] == "`":
        return name if backquotes else name[1:-1]
    return f"`{name}`" if backquotes else name


def try_to_fix_nullable_in_simple_aggregating_function(t: str) -> Optional[str]:
    # This workaround is to fix: https://github.com/ClickHouse/ClickHouse/issues/34407.
    # In the case of nullable columns and SimpleAggregateFunction  Clickhouse returns
    # Nullable(SimpleAggregateFunction(sum, Int32)) instead of SimpleAggregateFunction(sum, Nullable(Int32))
    # as it is done with other aggregate functions.
    # If not, the aggregation could return incorrect results.
    result = None
    if match := re.search(r"SimpleAggregateFunction\((\w+),\s*(?!(?:Nullable))([\w,.()]+)\)", t):
        fn = match.group(1)
        inner_type = match.group(2)
        result = f"SimpleAggregateFunction({fn}, Nullable({inner_type}))"
    return result


def schema_to_sql_columns(schema: List[Dict[str, Any]]) -> List[str]:
    """return an array with each column in SQL
    >>> schema_to_sql_columns([{'name': 'temperature', 'type': 'Float32', 'codec': None, 'default_value': None, 'nullable': False, 'normalized_name': 'temperature'}, {'name': 'temperature_delta', 'type': 'Float32', 'codec': 'CODEC(Delta(4), LZ4))', 'default_value': 'MATERIALIZED temperature', 'nullable': False, 'normalized_name': 'temperature_delta'}])
    ['`temperature` Float32', '`temperature_delta` Float32 MATERIALIZED temperature CODEC(Delta(4), LZ4))']
    >>> schema_to_sql_columns([{'name': 'temperature_delta', 'type': 'Float32', 'codec': '', 'default_value': 'MATERIALIZED temperature', 'nullable': False, 'normalized_name': 'temperature_delta'}])
    ['`temperature_delta` Float32 MATERIALIZED temperature']
    >>> schema_to_sql_columns([{'name': 'temperature_delta', 'type': 'Float32', 'codec': 'CODEC(Delta(4), LZ4))', 'default_value': '', 'nullable': False, 'normalized_name': 'temperature_delta'}])
    ['`temperature_delta` Float32 CODEC(Delta(4), LZ4))']
    >>> schema_to_sql_columns([{'name': 'temperature_delta', 'type': 'Float32', 'nullable': False, 'normalized_name': 'temperature_delta'}])
    ['`temperature_delta` Float32']
    >>> schema_to_sql_columns([{'name': 'temperature_delta', 'type': 'Float32', 'nullable': False, 'normalized_name': 'temperature_delta', 'jsonpath': '$.temperature_delta'}])
    ['`temperature_delta` Float32 `json:$.temperature_delta`']
    >>> schema_to_sql_columns([{'name': 'aggregation', 'type': 'SimpleAggregateFunction(sum, Int32)', 'nullable': True, 'normalized_name': 'aggregation', 'jsonpath': '$.aggregation'}])
    ['`aggregation` SimpleAggregateFunction(sum, Nullable(Int32)) `json:$.aggregation`']
    """
    columns: List[str] = []
    for x in schema:
        name = x["normalized_name"] if "normalized_name" in x else x["name"]
        if x["nullable"]:
            if (_type := try_to_fix_nullable_in_simple_aggregating_function(x["type"])) is None:
                _type = "Nullable(%s)" % x["type"]
        else:
            _type = x["type"]
        parts = [col_name(name, backquotes=True), _type]
        if x.get("jsonpath", None):
            parts.append(f"`json:{x['jsonpath']}`")
        if "default_value" in x and x["default_value"] not in ("", None):
            parts.append(x["default_value"])
        if "codec" in x and x["codec"] not in ("", None):
            parts.append(x["codec"])
        c = " ".join([x for x in parts if x]).strip()
        columns.append(c)
    return columns


def mark_error_string(s: str, i: int, line: int = 1) -> str:
    """
    >>> mark_error_string('0123456789', 0)
    '0123456789\\n^---'
    >>> mark_error_string('0123456789', 9)
    '0123456789\\n         ^---'
    >>> mark_error_string('01234\\n56789', 1)
    '01234\\n ^---'
    """
    marker = "^---"
    ss = s.splitlines()[line - 1] if s else ""
    start = 0
    end = len(ss)
    return ss[start:end] + "\n" + (" " * (i - start)) + marker


def format_parse_error(
    table_structure: str,
    i: int,
    position: int,
    hint: Optional[str] = None,
    line: int = 0,
    keyword: Optional[str] = None,
) -> str:
    adjusted_position = position - (len(keyword) if keyword else 0)
    message = f"{hint}\n" if hint else ""
    message += mark_error_string(table_structure, adjusted_position - 1, line=line)

    if keyword:
        message += f" found at position {adjusted_position - len(keyword)}"
    else:
        message += (
            f" found {repr(table_structure[i]) if len(table_structure)>i else 'EOF'} at position {adjusted_position}"
        )
    return message


def parse_indexes_structure(indexes: Optional[List[str]]) -> List[TableIndex]:
    """
    >>> parse_indexes_structure(["index_name a TYPE set(100) GRANULARITY 100", "index_name_bf mapValues(d) TYPE bloom_filter(0.001) GRANULARITY 16"])
    [TableIndex(name='index_name', expr='a', type_full='set(100)', granularity='100'), TableIndex(name='index_name_bf', expr='mapValues(d)', type_full='bloom_filter(0.001)', granularity='16')]
    >>> parse_indexes_structure(["INDEX index_name a TYPE set(100) GRANULARITY 100", " INDEX  index_name_bf mapValues(d) TYPE bloom_filter(0.001) GRANULARITY 16"])
    [TableIndex(name='index_name', expr='a', type_full='set(100)', granularity='100'), TableIndex(name='index_name_bf', expr='mapValues(d)', type_full='bloom_filter(0.001)', granularity='16')]
    >>> parse_indexes_structure(["index_name type TYPE set(100) GRANULARITY 100", "index_name_bf mapValues(d) TYPE bloom_filter(0.001) GRANULARITY 16"])
    [TableIndex(name='index_name', expr='type', type_full='set(100)', granularity='100'), TableIndex(name='index_name_bf', expr='mapValues(d)', type_full='bloom_filter(0.001)', granularity='16')]
    >>> parse_indexes_structure(["index_name a TYPE set(100) GRANULARITY 100,", "index_name_bf mapValues(d) TYPE bloom_filter(0.001) GRANULARITY 16"])
    [TableIndex(name='index_name', expr='a', type_full='set(100)', granularity='100'), TableIndex(name='index_name_bf', expr='mapValues(d)', type_full='bloom_filter(0.001)', granularity='16')]
    >>> parse_indexes_structure(["index_name a TYPE set(100)", "index_name_bf mapValues(d) TYPE bloom_filter(0.001)"])
    [TableIndex(name='index_name', expr='a', type_full='set(100)', granularity=None), TableIndex(name='index_name_bf', expr='mapValues(d)', type_full='bloom_filter(0.001)', granularity=None)]
    >>> parse_indexes_structure(["index_name u64 * length(s) TYPE set(100)", "index_name_bf mapValues(d) TYPE bloom_filter"])
    [TableIndex(name='index_name', expr='u64 * length(s)', type_full='set(100)', granularity=None), TableIndex(name='index_name_bf', expr='mapValues(d)', type_full='bloom_filter', granularity=None)]
    >>> parse_indexes_structure(["index_name path TYPE ngrambf_v1(4,1024,1,42) GRANULARITY 1"])
    [TableIndex(name='index_name', expr='path', type_full='ngrambf_v1(4,1024,1,42)', granularity='1')]
    >>> parse_indexes_structure(["index_name path TYPE ngrambf_v1(4, 1024, 1, 42) GRANULARITY 1"])
    [TableIndex(name='index_name', expr='path', type_full='ngrambf_v1(4, 1024, 1, 42)', granularity='1')]
    >>> parse_indexes_structure(["index_name u64 * length(s)"])
    Traceback (most recent call last):
    ...
    ValueError: invalid INDEX format. Usage: `name expr TYPE type_full GRANULARITY granularity`
    >>> parse_indexes_structure(["index_name a TYPE set(100) GRANULARITY 100, index_name_bf mapValues(d) TYPE bloom_filter(0.001) GRANULARITY 16"])
    Traceback (most recent call last):
    ...
    ValueError: invalid INDEX format. Usage: `name expr TYPE type_full GRANULARITY granularity`
    >>> parse_indexes_structure(["my_index m['key'] TYPE ngrambf_v1(1, 1024, 1, 42) GRANULARITY 1"])
    [TableIndex(name='my_index', expr="m['key']", type_full='ngrambf_v1(1, 1024, 1, 42)', granularity='1')]
    >>> parse_indexes_structure(["my_index_lambda arrayMap(x -> tupleElement(x,'message'), column_name) TYPE ngrambf_v1(1, 1024, 1, 42) GRANULARITY 1"])
    [TableIndex(name='my_index_lambda', expr="arrayMap(x -> tupleElement(x,'message'), column_name)", type_full='ngrambf_v1(1, 1024, 1, 42)', granularity='1')]
    >>> parse_indexes_structure(["ip_range_minmax_idx (toIPv6(ip_range_start), toIPv6(ip_range_end)) TYPE minmax GRANULARITY 1"])
    [TableIndex(name='ip_range_minmax_idx', expr='(toIPv6(ip_range_start), toIPv6(ip_range_end))', type_full='minmax', granularity='1')]
    """
    parsed_indices: List[TableIndex] = []
    if not indexes:
        return parsed_indices

    for index in indexes:
        index = index.strip().rstrip(",")
        index = index.lstrip("INDEX").strip()
        if index.count("TYPE") != 1:
            raise ValueError("invalid INDEX format. Usage: `name expr TYPE type_full GRANULARITY granularity`")

        match = re.match(
            r"(\w+)\s+([\w\s*\[\]\*\(\),\'\"-><.]+)\s+TYPE\s+(\w+)(?:\(([\w\s*.,]+)\))?(?:\s+GRANULARITY\s+(\d+))?",
            index,
        )
        if match:
            index_name, a, index_type, value, granularity = match.groups()
            index_expr = f"{index_type}({value})" if value else index_type
            parsed_indices.append(TableIndex(index_name, a.strip(), f"{index_expr}", granularity))
        else:
            raise ValueError("invalid INDEX format. Usage: `name expr TYPE type_full GRANULARITY granularity`")
    return parsed_indices


def parse_table_structure(schema: str) -> List[Dict[str, Any]]:
    """This parses the SQL schema for a CREATE TABLE
    Columns follow the syntax: name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1][,]
    Reference: https://clickhouse.tech/docs/en/sql-reference/statements/create/table/#syntax-forms

    >>> parse_table_structure('c Float32, b String')
    [{'name': 'c', 'type': 'Float32', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'c'}, {'name': 'b', 'type': 'String', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'b'}]

    >>> parse_table_structure('c Float32,--comment\\nb String')
    [{'name': 'c', 'type': 'Float32', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'c'}, {'name': 'b', 'type': 'String', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'b'}]

    >>> parse_table_structure('c Float32,--comment\\nb String --another-comment')
    [{'name': 'c', 'type': 'Float32', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'c'}, {'name': 'b', 'type': 'String', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'b'}]

    >>> parse_table_structure('c Float32 --first-comment\\n,--comment\\nb String --another-comment')
    [{'name': 'c', 'type': 'Float32', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'c'}, {'name': 'b', 'type': 'String', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'b'}]

    >>> parse_table_structure('--random comment here\\nc Float32 --another comment\\n,--another one\\nb String --this is the last one')
    [{'name': 'c', 'type': 'Float32', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'c'}, {'name': 'b', 'type': 'String', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'b'}]

    >>> parse_table_structure('--extra comment\\nc--extra comment\\nFloat32--extra comment\\n,--extra comment\\nb--extra comment\\nString--extra comment')
    [{'name': 'c', 'type': 'Float32', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'c'}, {'name': 'b', 'type': 'String', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'b'}]

    >>> parse_table_structure('c Nullable(Float32)')
    [{'name': 'c', 'type': 'Float32', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': True, 'normalized_name': 'c'}]

    >>> parse_table_structure('c Nullable(Float32) DEFAULT NULL')
    [{'name': 'c', 'type': 'Float32', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': True, 'normalized_name': 'c'}]

    >>> parse_table_structure("c String DEFAULT 'bla'")
    [{'name': 'c', 'type': 'String', 'codec': None, 'default_value': "DEFAULT 'bla'", 'jsonpath': None, 'nullable': False, 'normalized_name': 'c'}]

    >>> parse_table_structure('`foo.bar` UInt64')
    [{'name': 'foo.bar', 'type': 'UInt64', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'foo.bar'}]

    >>> parse_table_structure('double_value Float64 CODEC(LZ4HC(2))')
    [{'name': 'double_value', 'type': 'Float64', 'codec': 'CODEC(LZ4HC(2))', 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'double_value'}]
    >>> parse_table_structure('doubl/e_value Float64 CODEC(LZ4HC(2))')
    Traceback (most recent call last):
    ...
    ValueError: wrong value, please check the schema syntax
    doubl/e_value Float64 CODEC(LZ4HC(2))
         ^--- found '/' at position 6
    >>> parse_table_structure('`c` Nullable(Float32)')
    [{'name': 'c', 'type': 'Float32', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': True, 'normalized_name': 'c'}]
    >>> parse_table_structure('wadus INT UNSIGNED')
    [{'name': 'wadus', 'type': 'INT UNSIGNED', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'wadus'}]
    >>> parse_table_structure('c Int32 CODEC(Delta, LZ4)\\n')
    [{'name': 'c', 'type': 'Int32', 'codec': 'CODEC(Delta, LZ4)', 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'c'}]
    >>> parse_table_structure('c  SimpleAggregateFunction(sum, Int32),\\np SimpleAggregateFunction(sum, Int32)')
    Traceback (most recent call last):
    ...
    ValueError: Incompatible data types between aggregate function 'sum' which returns Int64 and column storage type Int32
    >>> parse_table_structure('c Int32 CODEC(Delta, LZ4) Materialized b*2\\n')
    Traceback (most recent call last):
    ...
    ValueError: Unexpected MATERIALIZED after CODEC
    c Int32 CODEC(Delta, LZ4) Materialized b*2
                             ^--- found ' ' at position 26
    >>> parse_table_structure('c Int32 CODEC(Delta, LZ4) Materialized ifNull(b*2, 0)\\n')
    Traceback (most recent call last):
    ...
    ValueError: Unexpected MATERIALIZED after CODEC
    c Int32 CODEC(Delta, LZ4) Materialized ifNull(b*2, 0)
                             ^--- found ' ' at position 26
    >>> parse_table_structure('c Int32 Materialized b*2\\n')
    [{'name': 'c', 'type': 'Int32', 'codec': None, 'default_value': 'MATERIALIZED b*2', 'jsonpath': None, 'nullable': False, 'normalized_name': 'c'}]
    >>> parse_table_structure('c Int32 Materialized b != 1 ? b*2: pow(b, 3)\\n')
    [{'name': 'c', 'type': 'Int32', 'codec': None, 'default_value': 'MATERIALIZED b != 1 ? b*2: pow(b, 3)', 'jsonpath': None, 'nullable': False, 'normalized_name': 'c'}]
    >>> parse_table_structure('')
    []
    >>> parse_table_structure('`date` Date,`timezone` String,`offset` Int32')
    [{'name': 'date', 'type': 'Date', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'date'}, {'name': 'timezone', 'type': 'String', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'timezone'}, {'name': 'offset', 'type': 'Int32', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'offset'}]
    >>> parse_table_structure('c Int32 Materialized b*2 CODEC(Delta, LZ4)\\n')
    [{'name': 'c', 'type': 'Int32', 'codec': 'CODEC(Delta, LZ4)', 'default_value': 'MATERIALIZED b*2', 'jsonpath': None, 'nullable': False, 'normalized_name': 'c'}]
    >>> parse_table_structure('c Int32 Materialized ifNull(b*2, 0) CODEC(Delta, LZ4)\\n')
    [{'name': 'c', 'type': 'Int32', 'codec': 'CODEC(Delta, LZ4)', 'default_value': 'MATERIALIZED ifNull(b*2, 0)', 'jsonpath': None, 'nullable': False, 'normalized_name': 'c'}]
    >>> parse_table_structure('`temperature_delta` Float32 MATERIALIZED temperature CODEC(Delta(4), LZ4)')
    [{'name': 'temperature_delta', 'type': 'Float32', 'codec': 'CODEC(Delta(4), LZ4)', 'default_value': 'MATERIALIZED temperature', 'jsonpath': None, 'nullable': False, 'normalized_name': 'temperature_delta'}]
    >>> parse_table_structure('foo^bar Float32')
    Traceback (most recent call last):
    ...
    ValueError: wrong value, please check the schema syntax
    foo^bar Float32
       ^--- found '^' at position 4
    >>> parse_table_structure('foo Float#32')
    Traceback (most recent call last):
    ...
    ValueError: wrong value, please check the schema syntax
    foo Float#32
             ^--- found '#' at position 10
    >>> parse_table_structure('foo Float32 DEFAULT 13, bar UInt64')
    [{'name': 'foo', 'type': 'Float32', 'codec': None, 'default_value': 'DEFAULT 13', 'jsonpath': None, 'nullable': False, 'normalized_name': 'foo'}, {'name': 'bar', 'type': 'UInt64', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'bar'}]
    >>> parse_table_structure('foo Float32 DEFAULT 1$$$3')
    Traceback (most recent call last):
    ...
    ValueError: wrong value, please check the schema syntax
    foo Float32 DEFAULT 1$$$3
                         ^--- found '$' at position 22
    >>> parse_table_structure('foo Float32 CODEC(Delta(4), LZ#4)')
    Traceback (most recent call last):
    ...
    ValueError: wrong value, please check the schema syntax
    foo Float32 CODEC(Delta(4), LZ#4)
                                  ^--- found '#' at position 31
    >>> parse_table_structure('\\n    `temperature` Float32,\\n    `temperature_delta` Float32 MATERIALIZED temperature CODEC(Delta(4), LZ4)\\n    ')
    [{'name': 'temperature', 'type': 'Float32', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'temperature'}, {'name': 'temperature_delta', 'type': 'Float32', 'codec': 'CODEC(Delta(4), LZ4)', 'default_value': 'MATERIALIZED temperature', 'jsonpath': None, 'nullable': False, 'normalized_name': 'temperature_delta'}]
    >>> parse_table_structure('temperature Float32, temperature_delta Float32 MATERIALIZED temperature Codec(Delta(4)), temperature_doubledelta Float32 MATERIALIZED temperature Codec(DoubleDelta), temperature_doubledelta_lz4 Float32 MATERIALIZED temperature Codec(DoubleDelta, LZ4)')
    [{'name': 'temperature', 'type': 'Float32', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'temperature'}, {'name': 'temperature_delta', 'type': 'Float32', 'codec': 'CODEC(Delta(4))', 'default_value': 'MATERIALIZED temperature', 'jsonpath': None, 'nullable': False, 'normalized_name': 'temperature_delta'}, {'name': 'temperature_doubledelta', 'type': 'Float32', 'codec': 'CODEC(DoubleDelta)', 'default_value': 'MATERIALIZED temperature', 'jsonpath': None, 'nullable': False, 'normalized_name': 'temperature_doubledelta'}, {'name': 'temperature_doubledelta_lz4', 'type': 'Float32', 'codec': 'CODEC(DoubleDelta, LZ4)', 'default_value': 'MATERIALIZED temperature', 'jsonpath': None, 'nullable': False, 'normalized_name': 'temperature_doubledelta_lz4'}]
    >>> parse_table_structure('t UInt8  CODEC(Delta(1), LZ4)')
    [{'name': 't', 'type': 'UInt8', 'codec': 'CODEC(Delta(1), LZ4)', 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 't'}]
    >>> parse_table_structure('tt UInt8  MATERIALIZED t')
    [{'name': 'tt', 'type': 'UInt8', 'codec': None, 'default_value': 'MATERIALIZED t', 'jsonpath': None, 'nullable': False, 'normalized_name': 'tt'}]
    >>> parse_table_structure('tt UInt8  MATERIALIZED t  CODEC(Delta(1), LZ4)')
    [{'name': 'tt', 'type': 'UInt8', 'codec': 'CODEC(Delta(1), LZ4)', 'default_value': 'MATERIALIZED t', 'jsonpath': None, 'nullable': False, 'normalized_name': 'tt'}]
    >>> parse_table_structure('tt SimpleAggregateFunction(any, Nullable(UInt8))')
    [{'name': 'tt', 'type': 'SimpleAggregateFunction(any, Nullable(UInt8))', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'tt'}]
    >>> parse_table_structure("timestamp DateTime MATERIALIZED toDateTime(JSONExtractInt(JSONExtractRaw(record, 'payload'), 'timestamp') / 1000)")
    [{'name': 'timestamp', 'type': 'DateTime', 'codec': None, 'default_value': "MATERIALIZED toDateTime(JSONExtractInt(JSONExtractRaw(record, 'payload'), 'timestamp') / 1000)", 'jsonpath': None, 'nullable': False, 'normalized_name': 'timestamp'}]
    >>> parse_table_structure("`test_default_cast` DEFAULT plus(13,1)")
    [{'name': 'test_default_cast', 'type': '', 'codec': None, 'default_value': 'DEFAULT plus(13,1)', 'jsonpath': None, 'nullable': False, 'normalized_name': 'test_default_cast'}]
    >>> parse_table_structure("hola Int, `materialized` String MATERIALIZED upper(no_nullable_string)")
    [{'name': 'hola', 'type': 'Int', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'hola'}, {'name': 'materialized', 'type': 'String', 'codec': None, 'default_value': 'MATERIALIZED upper(no_nullable_string)', 'jsonpath': None, 'nullable': False, 'normalized_name': 'materialized'}]
    >>> parse_table_structure('`a2` String `json:$.a2`, `a3` String `json:$.a3`\\n')
    [{'name': 'a2', 'type': 'String', 'codec': None, 'default_value': None, 'jsonpath': '$.a2', 'nullable': False, 'normalized_name': 'a2'}, {'name': 'a3', 'type': 'String', 'codec': None, 'default_value': None, 'jsonpath': '$.a3', 'nullable': False, 'normalized_name': 'a3'}]
    >>> parse_table_structure("`arr` Array(String) DEFAULT ['-']")
    [{'name': 'arr', 'type': 'Array(String)', 'codec': None, 'default_value': "DEFAULT ['-']", 'jsonpath': None, 'nullable': False, 'normalized_name': 'arr'}]
    >>> parse_table_structure("`arr` Array(String) DEFAULT array('-')")
    [{'name': 'arr', 'type': 'Array(String)', 'codec': None, 'default_value': "DEFAULT array('-')", 'jsonpath': None, 'nullable': False, 'normalized_name': 'arr'}]
    >>> parse_table_structure('`a2` Float32 CODEC(Delta, ZSTD(4)) `json:$.a2`, `a3` String `json:$.a3`\\n')
    [{'name': 'a2', 'type': 'Float32', 'codec': 'CODEC(Delta, ZSTD(4))', 'default_value': None, 'jsonpath': '$.a2', 'nullable': False, 'normalized_name': 'a2'}, {'name': 'a3', 'type': 'String', 'codec': None, 'default_value': None, 'jsonpath': '$.a3', 'nullable': False, 'normalized_name': 'a3'}]
    >>> parse_table_structure('`a` String, INDEX index_name a TYPE set(100) GRANULARITY 100')
    [{'name': 'a', 'type': 'String', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'a'}]
    >>> parse_table_structure('`a` String, INDEX index_name a TYPE set(100, 1) GRANULARITY 100')
    [{'name': 'a', 'type': 'String', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'a'}]
    >>> parse_table_structure('`index` String, INDEX index_name a TYPE set(100, 1) GRANULARITY 100')
    [{'name': 'index', 'type': 'String', 'codec': None, 'default_value': None, 'jsonpath': None, 'nullable': False, 'normalized_name': 'index'}]
    >>> parse_table_structure('`a2` String `json:$.a--2`, `a3` String `json:$.a3`\\n')
    [{'name': 'a2', 'type': 'String', 'codec': None, 'default_value': None, 'jsonpath': '$.a--2', 'nullable': False, 'normalized_name': 'a2'}, {'name': 'a3', 'type': 'String', 'codec': None, 'default_value': None, 'jsonpath': '$.a3', 'nullable': False, 'normalized_name': 'a3'}]
    """
    return _parse_table_structure(schema)


def clean_comments(schema_to_clean: str) -> str:
    """Remove the comments from the schema
    if the comments are between backticks, they will not be removed
    >>> clean_comments(None) is None
    True
    >>> clean_comments('')
    ''
    >>> clean_comments('    ')
    ''
    >>> clean_comments('\\n')
    ''
    >>> clean_comments('\\n\\n\\n\\n')
    ''
    >>> clean_comments('c Float32')
    'c Float32'
    >>> clean_comments('c Float32\\n')
    'c Float32'
    >>> clean_comments('c Float32\\n--this is a comment')
    'c Float32'
    >>> clean_comments('c Float32\\n--this is a comment\\n')
    'c Float32'
    >>> clean_comments('c Float32\\t-- this is a comment\\t\\n')
    'c Float32'
    >>> clean_comments('c Float32\\n--this is a comment\\r\\n')
    'c Float32'
    >>> clean_comments('c Float32\\n--this is a comment\\n--this is a comment2\\n')
    'c Float32'
    >>> clean_comments('c Float32\\n--this is a ```comment\\n')
    'c Float32'
    >>> clean_comments('c Float32\\n--this is a ```comment\\n')
    'c Float32'
    >>> clean_comments('c Float32, -- comment\\nd Float32 -- comment2')
    'c Float32,\\nd Float32'
    >>> clean_comments('c Float32, -- comment\\n   -- comment \\nd Float32 -- comment2')
    'c Float32,\\nd Float32'
    >>> clean_comments('c Float32 `json:$.aa--aa`\\n--this is a ```comment\\n')
    'c Float32 `json:$.aa--aa`'
    >>> clean_comments('c Float32 `json:$.cc--cc`\\nd Float32 `json:$.dd--dd`\\n--this is a ```comment\\n')
    'c Float32 `json:$.cc--cc`\\nd Float32 `json:$.dd--dd`'
    >>> clean_comments('c--c Float32 `json:$.cc--cc`\\n')
    'c'
    >>> clean_comments('`c--c` Float32 `json:$.cc--cc`\\n')
    '`c'
    """

    def clean_line_comments(line: str) -> str:
        if not line:
            return line
        i = 0
        inside_json_path = False
        while i < len(line):
            if i + 1 < len(line) and line[i] == "-" and line[i + 1] == "-" and not inside_json_path:
                return line[:i].strip()

            if not inside_json_path and line[i:].startswith("`json:"):
                inside_json_path = True
            elif inside_json_path and line[i] == "`":
                inside_json_path = False
            i += 1
        return line

    if schema_to_clean is None:
        return schema_to_clean

    cleaned_schema = ""
    for line in schema_to_clean.splitlines():
        cleaned_line = clean_line_comments(line)
        if cleaned_line:
            cleaned_schema += cleaned_line + "\n"
    return cleaned_schema.strip()


SyntaxExpr = namedtuple("SyntaxExpr", ["name", "regex"])

NULL = SyntaxExpr("NULL", re.compile(r"\s+NULL([^a-z0-9_]|$)", re.IGNORECASE))
NOTNULL = SyntaxExpr("NOTNULL", re.compile(r"\s+NOT\s+NULL([^a-z0-9_]|$)", re.IGNORECASE))
DEFAULT = SyntaxExpr("DEFAULT", re.compile(r"\s+DEFAULT([^a-z0-9_]|$)", re.IGNORECASE))
MATERIALIZED = SyntaxExpr("MATERIALIZED", re.compile(r"\s+MATERIALIZED([^a-z0-9_]|$)", re.IGNORECASE))
ALIAS = SyntaxExpr("ALIAS", re.compile(r"\s+ALIAS([^a-z0-9_]|$)", re.IGNORECASE))
CODEC = SyntaxExpr("CODEC", re.compile(r"\s+CODEC([^a-z0-9_]|$)", re.IGNORECASE))
TTL = SyntaxExpr("TTL", re.compile(r"\s+TTL([^a-z0-9_]|$)", re.IGNORECASE))
JSONPATH = SyntaxExpr("JSONPATH", re.compile(r"\s+`json:", re.IGNORECASE))
COMMA = SyntaxExpr("COMMA", re.compile(r",", re.IGNORECASE))
NEW_LINE = SyntaxExpr("NEW_LINE", re.compile(r"\s$"))
TYPE = SyntaxExpr("TYPE", re.compile(r""))  # TYPE doesn't have a fixed initial string

REGEX_WHITESPACE = re.compile(r"\s*")
REGEX_COMMENT = re.compile(r"\-\-[^\n\r]*[\n\r]")


def _parse_table_structure(schema: str) -> List[Dict[str, Any]]:
    # CH syntax from https://clickhouse.com/docs/en/sql-reference/statements/create/table/
    # name1 [type1] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1]
    try:
        schema = clean_comments(schema + "\n")
    except Exception as e:
        logging.exception(f"Error cleaning comments: {e}")
        schema = REGEX_COMMENT.sub(" ", schema + "\n").strip()

    if REGEX_WHITESPACE.fullmatch(schema):
        return []

    i: int = 0

    # For error feedback only
    line: int = 1
    pos: int = 1

    # Find the first SyntaxExpr in lookup that matches the schema at the current offset
    def lookahead_matches(lookup: Iterable) -> Optional[SyntaxExpr]:
        s = schema[i:]
        match = next((x for x in lookup if x.regex.match(s)), None)
        return match

    def advance_single_char() -> None:
        nonlocal i, line, pos
        if schema[i] == "\n":
            line += 1
            pos = 1
        else:
            pos += 1
        i += 1

    # Advance all whitespaces characters and then len(s) more chars
    def advance(s: str) -> None:
        if i < len(schema):
            while schema[i] in " \t\r\n":
                advance_single_char()
            for _ in s:
                advance_single_char()

    def get_backticked() -> str:
        begin = i
        while i < len(schema):
            c = schema[i]
            advance_single_char()
            if c == "`":
                return schema[begin : i - 1]
        raise ValueError(format_parse_error(schema, i, pos, "expecting ending backtick", line=line))

    def parse_name() -> str:
        nonlocal i, line, pos
        if schema[i] != "`":
            # regular name
            begin = i
            while i < len(schema):
                c = schema[i]
                if c in " \t\r\n":
                    return schema[begin:i]
                if c not in valid_chars_name:
                    raise ValueError(
                        format_parse_error(schema, i, pos, "wrong value, please check the schema syntax", line=line)
                    )
                advance_single_char()
            return schema[begin:i]
        else:
            # backticked name
            advance_single_char()
            return get_backticked()

    def parse_expr(lookup: Iterable[SyntaxExpr]) -> str:
        nonlocal i, line, pos

        begin: int = i
        context_stack: List[Optional[str]] = [None]
        while i < len(schema):
            context = context_stack[-1]
            c = schema[i]

            if (context == "'" and c == "'") or (context == '"' and c == '"') or (context == "(" and c == ")"):
                context_stack.pop()
            elif c == "'" and (context is None or context == "("):
                context_stack.append("'")
            elif c == '"' and (context is None or context == "("):
                context_stack.append('"')
            elif c == "(" and (context is None or context == "("):
                context_stack.append("(")
            elif context is None and lookahead_matches(lookup):
                return schema[begin:i].strip(" \t\r\n")
            elif (context is None and c not in valid_chars_fn) or (context == "(" and c not in valid_chars_fn):
                raise ValueError(
                    format_parse_error(schema, i, pos, "wrong value, please check the schema syntax", line=line)
                )
            advance_single_char()
        if i == begin:
            raise ValueError(format_parse_error(schema, i, pos, "wrong value", line=line))
        return schema[begin:].strip(" \t\r\n")

    columns: List[Dict[str, Any]] = []

    name: str = ""
    _type: str = ""
    default: str = ""
    materialized: str = ""
    codec: str = ""
    jsonpath: str = ""
    last: Optional[SyntaxExpr] = None

    def add_column(found: str) -> None:
        nonlocal name, _type, default, materialized, codec, jsonpath
        if name == "INDEX":
            return
        if not name:
            raise ValueError(
                format_parse_error(schema, i, pos, f"Syntax error: expecting NAME, found {found}", line=line)
            )
        default = "" if not default else f"DEFAULT {default}"
        materialized = "" if not materialized else f"MATERIALIZED {materialized}"
        codec = "" if not codec else f"CODEC{codec}"
        columns.append(
            {
                "name": name,
                "type": _type,
                "codec": codec,
                "default_value": default or materialized,
                "jsonpath": jsonpath,
            }
        )
        name = ""
        _type = ""
        default = ""
        materialized = ""
        codec = ""
        jsonpath = ""

    valid_next: List[SyntaxExpr] = [TYPE]
    while i < len(schema):
        if not name:
            advance("")
            valid_next = [NULL, NOTNULL, DEFAULT, MATERIALIZED, ALIAS, CODEC, TTL, JSONPATH, COMMA, TYPE]
            name = parse_name()
            continue
        found = lookahead_matches(
            [NULL, NOTNULL, DEFAULT, MATERIALIZED, ALIAS, CODEC, TTL, JSONPATH, COMMA, NEW_LINE, TYPE]
            if name != "INDEX"
            else [COMMA, NEW_LINE]
        )
        if found and found not in valid_next:
            after = f" after {last.name}" if last else ""
            raise ValueError(format_parse_error(schema, i, pos, f"Unexpected {found.name}{after}", line=line))
        if found == TYPE:
            advance("")
            valid_next = [NULL, NOTNULL, DEFAULT, MATERIALIZED, ALIAS, CODEC, TTL, JSONPATH, COMMA, NEW_LINE]
            detected_type = parse_expr([NULL, NOTNULL, DEFAULT, MATERIALIZED, ALIAS, CODEC, TTL, JSONPATH, COMMA])
            try:
                # Imported in the body to be compatible with the CLI
                from chtoolset.query import check_compatible_types

                # Check compatibility of the type with itself to verify it's a known type
                check_compatible_types(detected_type, detected_type)
            except ModuleNotFoundError:
                pass
            _type = detected_type
        elif found == NULL:
            # Not implemented
            raise ValueError(
                format_parse_error(schema, i, pos, "NULL column syntax not supported", line=line, keyword="NULL")
            )
        elif found == NOTNULL:
            # Not implemented
            raise ValueError(
                format_parse_error(
                    schema, i, pos, "NOT NULL column syntax not supported", line=line, keyword="NOT NULL"
                )
            )
        elif found == DEFAULT:
            advance("DEFAULT")
            valid_next = [CODEC, TTL, COMMA]
            default = parse_expr([NOTNULL, DEFAULT, MATERIALIZED, ALIAS, CODEC, TTL, JSONPATH, COMMA])
        elif found == MATERIALIZED:
            advance("MATERIALIZED")
            valid_next = [CODEC, TTL, COMMA]
            materialized = parse_expr([NOTNULL, DEFAULT, MATERIALIZED, ALIAS, CODEC, TTL, JSONPATH, COMMA])
        elif found == ALIAS:
            # Not implemented
            raise ValueError(format_parse_error(schema, i, pos, "ALIAS not supported", line=line, keyword="ALIAS"))
        elif found == CODEC:
            advance("CODEC")
            valid_next = [TTL, COMMA, JSONPATH]
            codec = parse_expr([NOTNULL, DEFAULT, MATERIALIZED, ALIAS, CODEC, TTL, JSONPATH, COMMA])
        elif found == TTL:
            advance("TTL")
            # Not implemented
            raise ValueError(format_parse_error(schema, i, pos, "column TTL not supported", line=line, keyword="TTL"))
        elif found == JSONPATH:
            advance("`json:")
            jsonpath = get_backticked()
        elif found == COMMA:
            if name == "INDEX":
                advance(",")
                continue
            advance(",")
            valid_next = []
            add_column("COMMA")
        elif found == NEW_LINE or (name == "INDEX" and not found):
            i += 1
        else:
            raise ValueError(
                format_parse_error(
                    schema,
                    i,
                    pos,
                    "wrong value, expected a NULL, NOT NULL, DEFAULT, MATERIALIZED, CODEC, TTL expressions, a column data type, a comma, a new line or a jsonpath",
                    line=line,
                )
            )
        last = found
    add_column("EOF")

    # normalize columns
    for column in columns:
        nullable = column["type"].lower().startswith("nullable")
        column["type"] = column["type"] if not nullable else column["type"][len("Nullable(") : -1]  # ')'
        column["nullable"] = nullable
        column["codec"] = column["codec"] if column["codec"] else None
        column["name"] = column["name"]
        column["normalized_name"] = column["name"]
        column["jsonpath"] = column["jsonpath"] if column["jsonpath"] else None
        default_value = column["default_value"] if column["default_value"] else None
        if nullable and default_value and default_value.lower() == "default null":
            default_value = None
        column["default_value"] = default_value

    return columns


def engine_can_be_replicated(engine: Optional[str]) -> bool:
    """
    >>> engine_can_be_replicated('MergeTree() order by tuple()')
    True
    >>> engine_can_be_replicated('JOIN(ANY, LEFT, foo)')
    False
    >>> engine_can_be_replicated('ReplicatingMergeTree() order by tuple()')
    True
    >>> engine_can_be_replicated(None)
    False
    >>> engine_can_be_replicated("ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/d_e7a588.t_ba45cd49a39c4649a1c7c2ec7adcaf43_t_be23eb990c394399854f8271c550fc36_staging', '{replica}', insert_date)")
    False
    """
    if not engine:
        return False
    lower_engine = engine.lower()
    return not lower_engine.startswith("Replicated".lower()) and "mergetree" in lower_engine


def engine_supports_delete(engine: Optional[str]) -> bool:
    """
    >>> engine_supports_delete('MergeTree() order by tuple()')
    True
    >>> engine_supports_delete('JOIN(ANY, LEFT, foo)')
    False
    >>> engine_supports_delete('ReplicatingMergeTree() order by tuple()')
    True
    >>> engine_supports_delete(None)
    False
    """
    if not engine:
        return False
    return "mergetree" in engine.lower()


def engine_replicated_to_local(engine: str) -> str:
    """
    >>> engine_replicated_to_local("ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/test.foo','{replica}') order by (test)")
    'MergeTree() order by (test)'
    >>> engine_replicated_to_local("ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/test.foo', '{replica}', timestamp) order by (test)")
    'ReplacingMergeTree(timestamp) order by (test)'
    >>> engine_replicated_to_local("Join(ANY, LEFT, test)")
    'Join(ANY, LEFT, test)'
    >>> engine_replicated_to_local("ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/{layer}-{shard}/test.foo', '{replica}', sign,version) ORDER BY pk TTL toDate(local_timeplaced) + toIntervalDay(3) SETTINGS index_granularity = 8192")
    'VersionedCollapsingMergeTree(sign, version) ORDER BY pk TTL toDate(local_timeplaced) + toIntervalDay(3) SETTINGS index_granularity = 8192'
    """

    def _replace(m):
        parts = m.groups()
        s = parts[0] + "MergeTree("
        if parts[1]:
            tk = parts[1].split(",")
            if len(tk) > 2:  # remove key and {replica} part
                s += ", ".join([x.strip() for x in tk[2:]])
        s += ")" + parts[2]
        return s

    if "Replicated" not in engine:
        return engine

    return re.sub(r"Replicated(.*)MergeTree\(([^\)]*)\)(.*)", _replace, engine.strip())


def engine_patch_replicated_engine(engine: str, engine_full: Optional[str], new_table_name: str) -> Optional[str]:
    """
    >>> engine_patch_replicated_engine("ReplicatedMergeTree", "ReplicatedMergeTree('/clickhouse/tables/1-1/table_name', 'replica') PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192", 'table_name_staging')
    "ReplicatedMergeTree('/clickhouse/tables/1-1/table_name_staging', 'replica') PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192"
    >>> engine_patch_replicated_engine("ReplicatedMergeTree", "ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/sales_product_rank_rt_replicated_2', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY (purchase_location, sku_rank_lc, date)", 'sales_product_rank_rt_replicated_2_staging')
    "ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/sales_product_rank_rt_replicated_2_staging', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY (purchase_location, sku_rank_lc, date)"
    >>> engine_patch_replicated_engine("ReplicatedMergeTree", None, 't_000') is None
    True
    >>> engine_patch_replicated_engine("Log", "Log()", 't_000')
    'Log()'
    >>> engine_patch_replicated_engine("MergeTree", "MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_date, event_time) SETTINGS index_granularity = 1024", 't_000')
    'MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_date, event_time) SETTINGS index_granularity = 1024'
    """
    if not engine_full:
        return None
    if engine.lower().startswith("Replicated".lower()):
        parts = re.split(r"(Replicated.*MergeTree\(')([^']*)('.*)", engine_full)
        paths = parts[2].split("/")
        paths[-1] = new_table_name
        zoo_path = "/".join(paths)
        return "".join(parts[:2] + [zoo_path] + parts[3:])
    return engine_full


if __name__ == "__main__":
    print(  # noqa: T201
        _parse_table_structure(
            """hola Int --comment\n, `materialized` String --otro comment\n MATERIALIZED upper(no_nullable_string)"""
        )
    )
    # print(_parse_table_structure('@'))
    # print(mark_error_string('012345678901234567890123456789', 30))
    # print(_parse_table_structure('@'))
    # print(_parse_table_structure('`test_default_cast DEFAULT plus(13,1)'))
    # print(_parse_table_structure('`test_default_cast` DEFAULT plus(13,1)'))
    # print(_parse_table_structure('hola Int32'))
    # _parse_table_structure('hola')
    # print(parse_table_structure("timestamp DateTime MATERIALIZED toDateTime(JSONExtractInt(JSONExtractRaw(record, 'payload'), 'timestamp') / 1000)"))
