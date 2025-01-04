import logging
import re
from collections import defaultdict
from dataclasses import asdict
from typing import Any, Callable, Dict, Iterable, List, Optional

from ..sql import (
    TableIndex,
    TableProjection,
    col_name,
    engine_replicated_to_local,
    parse_indexes_structure,
    parse_table_structure,
)

DEFAULT_EMPTY_PARAMETERS = ["ttl", "partition_key", "sorting_key"]
DEFAULT_JOIN_EMPTY_PARAMETERS = ["join_strictness", "join_type", "key_columns"]

# Currently we only support the simplest TTLs
# f(X) + toIntervalZ(N)
# * `f()` is an optional CH function or chain of functions (we don't care)
# * `X` is a column. We accept also spaces and / as some TTL do (column / 1000)
# * `+` is the exact char
# * `toInverval` are exact chars
# * `Z` is [AZaZ]*
# * `N` is a number
SIMPLE_TTL_DEFINITION = re.compile(r"""^([a-zA-Z0-9_\-\.\(\)\ \/\*]*) \+ (toInterval[a-zA-Z]*\([0-9]+\))$""")


class TableDetails:
    """
    >>> ed = TableDetails({})
    >>> ed.engine_full == None
    True
    >>> ed.engine == ''
    True
    >>> ed.to_json()
    {'engine_full': None, 'engine': ''}
    >>> ed.to_datafile()
    ''

    >>> ed = TableDetails({ "engine_full": "MergeTree() PARTITION BY toYear(timestamp) ORDER BY (timestamp, cityHash64(location)) SAMPLE BY cityHash64(location) SETTINGS index_granularity = 32, index_granularity_bytes = 2048", "engine": "MergeTree", "partition_key": "toYear(timestamp)", "sorting_key": "timestamp, cityHash64(location)", "primary_key": "timestamp, cityHash64(location)", "sampling_key": "cityHash64(location)", "settings": "index_granularity = 32, index_granularity_bytes = 2048", "ttl": None })
    >>> ed.diff_ttl("toDate(timestamp) + toIntervalDay(1)")
    True
    >>> ed = TableDetails({ "engine_full": "MergeTree() PARTITION BY toYear(timestamp) ORDER BY (timestamp, cityHash64(location)) SAMPLE BY cityHash64(location) SETTINGS index_granularity = 32, index_granularity_bytes = 2048 TTL toDate(timestamp) + INTERVAL 1 DAY", "engine": "MergeTree", "partition_key": "toYear(timestamp)", "sorting_key": "timestamp, cityHash64(location)", "primary_key": "timestamp, cityHash64(location)", "sampling_key": "cityHash64(location)", "settings": "index_granularity = 32, index_granularity_bytes = 2048", "ttl": "toDate(timestamp) + INTERVAL 1 DAY" })
    >>> ed.diff_ttl("toDate(timestamp) + toIntervalDay(1)")
    False
    >>> ed.diff_ttl("toDate(timestamp) + toIntervalDay(2)")
    True
    >>> ed.diff_ttl("toDate(timestamp) + INTERVAL DAY 2")
    True
    >>> ed.diff_ttl("toDate(timestamp) + INTERVAL 1 DAY")
    False
    >>> ed.diff_ttl("")
    True
    >>> ed.engine_full
    'MergeTree() PARTITION BY toYear(timestamp) ORDER BY (timestamp, cityHash64(location)) SAMPLE BY cityHash64(location) SETTINGS index_granularity = 32, index_granularity_bytes = 2048 TTL toDate(timestamp) + INTERVAL 1 DAY'
    >>> ed.engine
    'MergeTree'
    >>> ed.to_json()
    {'engine_full': 'MergeTree() PARTITION BY toYear(timestamp) ORDER BY (timestamp, cityHash64(location)) SAMPLE BY cityHash64(location) SETTINGS index_granularity = 32, index_granularity_bytes = 2048 TTL toDate(timestamp) + INTERVAL 1 DAY', 'engine': 'MergeTree', 'partition_key': 'toYear(timestamp)', 'sorting_key': 'timestamp, cityHash64(location)', 'sampling_key': 'cityHash64(location)', 'settings': 'index_granularity = 32, index_granularity_bytes = 2048', 'ttl': 'toDate(timestamp) + INTERVAL 1 DAY'}
    >>> ed.to_datafile()
    'ENGINE "MergeTree"\\nENGINE_PARTITION_KEY "toYear(timestamp)"\\nENGINE_SORTING_KEY "timestamp, cityHash64(location)"\\nENGINE_SAMPLING_KEY "cityHash64(location)"\\nENGINE_SETTINGS "index_granularity = 32, index_granularity_bytes = 2048"\\nENGINE_TTL "toDate(timestamp) + INTERVAL 1 DAY"'

    >>> ed = TableDetails({"engine_full": "Join(ANY, LEFT, id)", "engine": "Join", "partition_key": "", "sorting_key": "", "primary_key": "", "sampling_key": ""})
    >>> ed.engine_full
    'Join(ANY, LEFT, id)'
    >>> ed.engine
    'Join'
    >>> ed.to_json()
    {'engine_full': 'Join(ANY, LEFT, id)', 'engine': 'Join', 'join_strictness': 'ANY', 'join_type': 'LEFT', 'key_columns': 'id'}
    >>> ed.to_datafile()
    'ENGINE "Join"\\nENGINE_JOIN_STRICTNESS "ANY"\\nENGINE_JOIN_TYPE "LEFT"\\nENGINE_KEY_COLUMNS "id"'

    >>> ed = TableDetails({"database": "d_01", "name": "t_01", "engine": "Join", "join_strictness": "ANY", "join_type": "LEFT", "key_columns": "id"})
    >>> ed.engine_full == None
    True
    >>> ed.engine
    'Join'
    >>> ed.to_json()
    {'engine_full': None, 'engine': 'Join', 'join_strictness': 'ANY', 'join_type': 'LEFT', 'key_columns': 'id'}
    >>> ed.to_datafile()
    'ENGINE "Join"\\nENGINE_JOIN_STRICTNESS "ANY"\\nENGINE_JOIN_TYPE "LEFT"\\nENGINE_KEY_COLUMNS "id"'
    >>> ed = TableDetails({ "engine_full": "MergeTree() PARTITION BY toYear(timestamp) ORDER BY (timestamp, cityHash64(location)) SAMPLE BY cityHash64(location) SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, merge_with_ttl_timeout = 1800 TTL toDate(timestamp) + INTERVAL 1 DAY"})
    >>> ed.engine_full
    'MergeTree() PARTITION BY toYear(timestamp) ORDER BY (timestamp, cityHash64(location)) SAMPLE BY cityHash64(location) SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, merge_with_ttl_timeout = 1800 TTL toDate(timestamp) + INTERVAL 1 DAY'

    >>> x = TableDetails({'database': 'd_01', 'name': 't_01', 'create_table_query': "CREATE TABLE d_01.t_01 (`project_id` String, `project_name` String, `project_repo` String, `owner_id` String, `updated_at` DateTime64(3)) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/d_test_1ad5e496b29246e1ade99117e9180f6f.t_1bac899a56b34b33921fbf468b4500f7', '{replica}', updated_at) PARTITION BY tuple() PRIMARY KEY project_id ORDER BY project_id SETTINGS index_granularity = 32", 'engine': 'ReplicatedReplacingMergeTree', 'partition_key': 'tuple()', 'sorting_key': 'project_id', 'primary_key': 'project_id', 'sampling_key': '', 'engine_full': "ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/d_01.t_01', '{replica}', updated_at) PARTITION BY tuple() PRIMARY KEY project_id ORDER BY project_id SETTINGS index_granularity = 32", 'settings': 'index_granularity = 32', 'ttl': ''})
    >>> x.primary_key

    """

    def __init__(self, details: Optional[Dict[str, Any]] = None):
        self.details = details or {}

    def __bool__(self):
        return bool(self.details)

    @property
    def engine_full(self) -> Optional[str]:
        _engine_full: Optional[str] = self.details.get("engine_full", None)
        if not _engine_full:
            return None

        settings = self.details.get("settings", None)
        # We cannot remove index_granularity = 8192 blindly because it might be followed by other settings
        if settings and settings.strip().lower() == "index_granularity = 8192":
            _engine_full = _engine_full.replace(" SETTINGS index_granularity = 8192", "")
        return engine_replicated_to_local(_engine_full)

    @property
    def original_engine_full(self) -> Optional[str]:
        return self.details.get("engine_full", None)

    @property
    def name(self) -> str:
        return self.details.get("name", "")

    @property
    def database(self) -> str:
        return self.details.get("database", "")

    @property
    def engine(self) -> str:
        _engine = self.details.get("engine", "")
        return _engine and _engine.replace("Replicated", "")

    @property
    def original_engine(self) -> Optional[str]:
        return self.details.get("engine", None)

    @property
    def version(self):
        _version = self.details.get("version", None)
        return _version

    def is_replicated(self):
        return "Replicated" in self.details.get("engine", None)

    def is_mergetree_family(self) -> bool:
        return self.engine is not None and "mergetree" in self.engine.lower()

    def supports_alter_add_column(self) -> bool:
        return self.is_mergetree_family() or (self.engine is not None and self.engine.lower() == "null")

    def is_replacing_engine(self) -> bool:
        if self.engine:
            engine_lower = self.engine.lower()
            is_aggregating = "aggregatingmergetree" in engine_lower
            is_replacing = "replacingmergetree" in engine_lower
            is_collapsing = "collapsingmergetree" in engine_lower
            return is_aggregating or is_replacing or is_collapsing
        return False

    def diff_ttl(self, new_ttl: str) -> bool:
        try:
            from tinybird.sql_toolset import format_sql

            current_ttl = format_sql(f"select {self.ttl}")[7:]
            new_ttl = format_sql(f"select {new_ttl}")[7:]
            return current_ttl != new_ttl
        except Exception:
            return self.ttl != new_ttl

    @property
    def partition_key(self) -> Optional[str]:
        return self.details.get("partition_key", None)

    @property
    def sorting_key(self) -> Optional[str]:
        _sorting_key = self.details.get("sorting_key", None)
        if self.is_replacing_engine() and not _sorting_key:
            raise ValueError(f"SORTING_KEY must be defined for the {self.engine} engine")
        if self.is_mergetree_family():
            return _sorting_key or "tuple()"
        return _sorting_key

    @property
    def primary_key(self) -> Optional[str]:
        _primary_key = self.details.get("primary_key", None)
        # When querying `system.tables`, it will return the `sorting_key` as `primary_key` even if it was not specify
        # So we need to ignore it
        if self.sorting_key == _primary_key:
            return None
        return _primary_key

    @property
    def sampling_key(self) -> Optional[str]:
        return self.details.get("sampling_key", None)

    @property
    def settings(self):
        settings = self.details.get("settings", None)
        if settings and settings.strip().lower() != "index_granularity = 8192":
            return settings

    @property
    def ttl(self):
        return self.details.get("ttl", None)

    @property
    def ver(self):
        _ver = self.details.get("ver", None)
        return _ver

    @property
    def is_deleted(self):
        _is_deleted = self.details.get("is_deleted", None)
        return _is_deleted

    @property
    def columns(self):
        _columns = self.details.get("columns", None)
        return _columns

    @property
    def sign(self):
        _sign = self.details.get("sign", None)
        return _sign

    @property
    def join_strictness(self):
        _join_strictness = self.details.get("join_strictness", None)
        return _join_strictness

    @property
    def join_type(self):
        _join_type = self.details.get("join_type", None)
        return _join_type

    @property
    def key_columns(self):
        _key_columns = self.details.get("key_columns", None)
        return _key_columns

    @property
    def statistics(self) -> Dict[str, Any]:
        return {
            "bytes": self.details.get("total_bytes", None),
            "row_count": self.details.get("total_rows", None),
        }

    @property
    def indexes(self) -> List[TableIndex]:
        return _parse_indexes(str(self.details.get("create_table_query", "")))

    @property
    def projections(self) -> List[TableProjection]:
        return _parse_projections(self.details.get("create_table_query", ""))

    def to_json(self, exclude: Optional[List[str]] = None, include_empty_details: bool = False):
        # name, database are not exported since they are not part of the engine
        d: Dict[str, Any] = {
            "engine_full": self.engine_full,
            "engine": self.engine,
        }
        if self.partition_key:
            d["partition_key"] = self.partition_key
        if self.sorting_key:
            d["sorting_key"] = self.sorting_key
        if self.primary_key:
            d["primary_key"] = self.primary_key
        if self.sampling_key:
            d["sampling_key"] = self.sampling_key
        if self.settings:
            d["settings"] = self.settings
        if self.join_strictness:
            d["join_strictness"] = self.join_strictness
        if self.join_type:
            d["join_type"] = self.join_type
        if self.key_columns:
            d["key_columns"] = self.key_columns
        if self.ver:
            d["ver"] = self.ver
        if self.is_deleted:
            d["is_deleted"] = self.is_deleted
        if self.sign:
            d["sign"] = self.sign
        if self.version:
            d["version"] = self.version
        if self.ttl:
            d["ttl"] = self.ttl.strip()
        if self.indexes:
            d["indexes"] = [asdict(index) for index in self.indexes]

        if self.engine_full:
            engine_params = engine_params_from_engine_full(self.engine_full)
            d = {**d, **engine_params}

        if include_empty_details:
            if self.engine and self.engine.lower() == "join":
                d = set_empty_details(d, DEFAULT_JOIN_EMPTY_PARAMETERS)
            else:
                d = set_empty_details(d, DEFAULT_EMPTY_PARAMETERS)

        if exclude:
            for attr in exclude:
                if attr in d:
                    del d[attr]

        return d

    def to_datafile(self, include_empty_details: bool = False) -> str:
        d: Dict[str, Any] = self.to_json(
            exclude=["engine", "engine_full", "indexes"], include_empty_details=include_empty_details
        )
        engine: str = self.engine

        datafile: str = ""
        if engine:
            datafile += "\n".join(
                [f'ENGINE "{engine}"'] + [f'ENGINE_{k.upper()} "{v}"' for k, v in d.items() if v is not None]
            )

        return datafile


def set_empty_details(details: Dict[str, str], parameters: Iterable[str]):
    for parameter in parameters:
        if parameter not in details:
            details[parameter] = ""

    return details


class EngineOption:
    def __init__(
        self,
        name: str,
        sql: str,
        required: Optional[bool] = None,
        default_value=None,
        is_valid: Optional[Callable[[List[Dict[str, Any]], str], Optional[str]]] = None,
        tb_param: Optional[str] = None,
    ):
        self.name = name
        self.sql = sql
        self.required = required
        self.default_value = default_value
        self.is_valid = is_valid
        self.tb_param = tb_param if tb_param else "_".join(["engine", name])


class EngineParam:
    def __init__(
        self,
        name: str,
        required: Optional[bool] = None,
        default_value=None,
        is_valid: Optional[Callable[[List[Dict[str, Any]], str], Optional[str]]] = None,
        tb_param: Optional[str] = None,
    ):
        self.name = name
        self.required = required
        self.default_value = default_value
        self.is_valid = is_valid
        self.tb_param = tb_param if tb_param else "_".join(["engine", name])


def engine_config(name: str, params: Optional[List[EngineParam]] = None, options: Optional[List[EngineOption]] = None):
    params = params or []
    options = options or []
    return (name, (params, options))


def column_is_valid(columns: List[Dict[str, Any]], column_name: str) -> str:
    schema_columns = [col_name(c["name"], backquotes=False) for c in columns]
    if column_name not in schema_columns:
        raise ValueError(f"'{column_name}' column is not present in schema")
    return col_name(column_name, backquotes=False)


def columns_are_valid(columns: List[Dict[str, Any]], column_names: str) -> str:
    schema_columns = [col_name(c["name"], backquotes=False) for c in columns]
    new_column_names = []
    for column_name in [x.strip() for x in column_names.split(",")]:
        if column_name not in schema_columns:
            raise ValueError(f"'{column_name}' column is not present in schema")
        new_column_names.append(col_name(column_name, backquotes=False))
    return ", ".join(new_column_names)


def sorting_key_is_valid(columns: List[Dict[str, Any]], value: Optional[str]) -> str:
    INVALID_SORTING_KEYS = ["tuple()"]

    if not value:
        raise ValueError("Sorting key can not be empty")
    if value in INVALID_SORTING_KEYS:
        raise ValueError(f"'{value}' is not a valid sorting key")
    return value


def case_insensitive_check(valid_values: List[str]) -> Callable[[List[Dict[str, Any]], str], Optional[str]]:
    """
    >>> valid_values = ['ANY', 'ALL']
    >>> checker = case_insensitive_check(valid_values)
    >>> checker([],'ALL')

    >>> valid_values = ['ANY', 'ALL']
    >>> checker = case_insensitive_check(valid_values)
    >>> checker([],'any')

    >>> valid_values = ['ANY', 'ALL']
    >>> checker = case_insensitive_check(valid_values)
    >>> checker([],'foo')
    Traceback (most recent call last):
    ...
    ValueError: valid values are ANY, ALL
    """

    def checker(columns: List[Dict[str, Any]], value: str):
        if value.upper() not in valid_values:
            raise ValueError(f"valid values are {', '.join(valid_values)}")

    return checker


# [PARTITION BY expr]
# [ORDER BY expr]
# [PRIMARY KEY expr]
# [SAMPLE BY expr]
# [TTL expr [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'], ...]
# [SETTINGS name=value, ...]
MERGETREE_OPTIONS = [
    EngineOption(name="partition_key", sql="PARTITION BY"),
    EngineOption(name="sorting_key", sql="ORDER BY", default_value="tuple()"),
    EngineOption(name="primary_key", sql="PRIMARY KEY"),
    EngineOption(name="sampling_key", sql="SAMPLE BY"),
    EngineOption(name="ttl", sql="TTL"),
    EngineOption(name="settings", sql="SETTINGS"),
]
REPLACINGMERGETREE_OPTIONS = [
    EngineOption(name="partition_key", sql="PARTITION BY"),
    EngineOption(name="sorting_key", sql="ORDER BY", required=True, is_valid=sorting_key_is_valid),
    EngineOption(name="primary_key", sql="PRIMARY KEY"),
    EngineOption(name="sampling_key", sql="SAMPLE BY"),
    EngineOption(name="ttl", sql="TTL"),
    EngineOption(name="settings", sql="SETTINGS"),
]
ENABLED_ENGINES = [
    # MergeTree()
    engine_config("MergeTree", options=MERGETREE_OPTIONS),
    # ReplacingMergeTree([ver])
    engine_config(
        "ReplacingMergeTree",
        [EngineParam(name="ver", is_valid=column_is_valid), EngineParam(name="is_deleted", is_valid=column_is_valid)],
        REPLACINGMERGETREE_OPTIONS,
    ),
    # SummingMergeTree([columns])
    engine_config(
        "SummingMergeTree",
        [
            # This should check the columns are numeric ones
            EngineParam(name="columns", is_valid=columns_are_valid),
        ],
        MERGETREE_OPTIONS,
    ),
    # AggregatingMergeTree()
    engine_config("AggregatingMergeTree", options=REPLACINGMERGETREE_OPTIONS),
    # CollapsingMergeTree(sign)
    engine_config(
        "CollapsingMergeTree",
        [EngineParam(name="sign", required=True, is_valid=column_is_valid)],
        REPLACINGMERGETREE_OPTIONS,
    ),
    # VersionedCollapsingMergeTree(sign, version)
    engine_config(
        "VersionedCollapsingMergeTree",
        [
            EngineParam(name="sign", required=True, is_valid=column_is_valid),
            EngineParam(name="version", required=True, is_valid=column_is_valid),
        ],
        MERGETREE_OPTIONS,
    ),
    # Join(join_strictness, join_type, k1[, k2, ...])
    engine_config(
        "Join",
        [
            # https://github.com/ClickHouse/ClickHouse/blob/fa8e4e4735b932f08b6beffcb2d069b72de34401/src/Storages/StorageJoin.cpp
            EngineParam(
                name="join_strictness", required=True, is_valid=case_insensitive_check(["ANY", "ALL", "SEMI", "ANTI"])
            ),
            EngineParam(
                name="join_type", required=True, is_valid=case_insensitive_check(["LEFT", "INNER", "RIGHT", "FULL"])
            ),
            EngineParam(name="key_columns", required=True, is_valid=columns_are_valid),
        ],
    ),
    # Null()
    engine_config("Null"),
]


def get_engine_config(engine: str):
    for name, config in ENABLED_ENGINES:
        if engine.lower() == name.lower():
            return (name, config)
    raise ValueError(
        f"Engine {engine} is not supported, supported engines include: {', '.join([e[0] for e in ENABLED_ENGINES])}"
    )


def engine_params(columns, params: List[EngineParam], args: Dict):
    params_values = []
    for p in params:
        if p.required and p.name not in args:
            raise ValueError(f"Missing required parameter '{p.name}'")
        param_value = args.get(p.name, None) or p.default_value
        if not param_value:
            continue
        if p.is_valid:
            check_is_valid(
                valid_check=p.is_valid, check_type="parameter", columns=columns, tb_param=p.tb_param, value=param_value
            )
        params_values.append(param_value)
    return params_values


def engine_options(columns, options: List[EngineOption], args: Dict):
    options_values = []
    engine_settings = ""

    for o in options:
        if o.required and o.name not in args:
            raise ValueError(f"Missing required option '{o.name}'")
        option_value = args.get(o.name) or o.default_value
        if o.is_valid:
            check_is_valid(
                valid_check=o.is_valid, check_type="option", columns=columns, tb_param=o.tb_param, value=option_value
            )

        if option_value:
            if o.sql.lower() == "settings":
                engine_settings = f"{o.sql} {option_value}"
            else:
                options_values.append(f"{o.sql} ({option_value})")

    if engine_settings:
        options_values.append(engine_settings)

    return options_values


def check_is_valid(
    valid_check: Callable[[List[Dict[str, Any]], str], Optional[str]],
    check_type: str,
    columns: List[Dict[str, Any]],
    tb_param: str,
    value: str,
):
    """
    >>> check_is_valid(sorting_key_is_valid, 'option', ['column-name'], 'sorting_key', 'date')

    >>> check_is_valid(sorting_key_is_valid, 'option', ['column-name'], 'sorting_key', 'tuple()')
    Traceback (most recent call last):
    ...
    ValueError: Invalid value 'tuple()' for option 'sorting_key', reason: 'tuple()' is not a valid sorting key
    """
    try:
        new_value = valid_check(columns, value)
        if new_value:
            value = new_value
    except Exception as e:
        raise ValueError(f"Invalid value '{value}' for {check_type} '{tb_param}', reason: {e}")


def build_engine(
    engine: str, columns: Optional[List], params: List[EngineParam], options: List[EngineOption], args: Dict
):
    return f"{engine}({', '.join(engine_params(columns, params, args))}) {' '.join(engine_options(columns, options, args))}".strip()


def engine_full_from_dict(
    engine: str, args: dict, schema: Optional[str] = None, columns: Optional[List[Dict[str, Any]]] = None
):
    """
    >>> schema = ''
    >>> engine_full_from_dict('wadus', {}, schema=schema)
    Traceback (most recent call last):
    ...
    ValueError: Engine wadus is not supported, supported engines include: MergeTree, ReplacingMergeTree, SummingMergeTree, AggregatingMergeTree, CollapsingMergeTree, VersionedCollapsingMergeTree, Join, Null
    >>> schema = ''
    >>> engine_full_from_dict('null', {}, schema=schema)
    'Null()'
    >>> schema = ''
    >>> engine_full_from_dict('null', {}, columns=[])
    'Null()'

    >>> schema = 'cid Int32'
    >>> engine_full_from_dict('Join', {'join_strictness': 'ANY', 'join_type': 'LEFT', 'key_columns': 'cid'}, schema=schema)
    'Join(ANY, LEFT, cid)'
    >>> engine_full_from_dict('Join', {'join_strictness': 'ANY', 'join_type': 'LEFT', 'key_columns': 'cid'}, columns=[{'name': 'cid', 'type': 'Int32', 'codec': None, 'default_value': None, 'nullable': False, 'normalized_name': 'cid'}])
    'Join(ANY, LEFT, cid)'
    >>> schema = 'cid1 Int32, cid2 Int8'
    >>> engine_full_from_dict('Join', {'join_strictness': 'ANY', 'join_type': 'LEFT', 'key_columns': 'cid1, cid2'}, schema=schema)
    'Join(ANY, LEFT, cid1, cid2)'
    >>> engine_full_from_dict('Join', {'join_strictness': 'ANY', 'join_type': 'OUTER', 'key_columns': 'cid'}, schema=schema)
    Traceback (most recent call last):
    ...
    ValueError: Invalid value 'OUTER' for parameter 'engine_join_type', reason: valid values are LEFT, INNER, RIGHT, FULL

    >>> schema = ''
    >>> engine_full_from_dict('MergeTree', {}, schema=schema)
    'MergeTree() ORDER BY (tuple())'
    >>> engine_full_from_dict('MergeTree', {'sorting_key': 'local_date, cod_store'}, schema=schema)
    'MergeTree() ORDER BY (local_date, cod_store)'
    >>> engine_full_from_dict('MergeTree', {'partition_key': 'toDate(timestamp)', 'sorting_key': 'local_date, cod_store', 'settings': 'index_granularity = 32, index_granularity_bytes = 2048', 'ttl': 'toDate(local_date) + INTERVAL 1 DAY'}, schema=schema)
    'MergeTree() PARTITION BY (toDate(timestamp)) ORDER BY (local_date, cod_store) TTL (toDate(local_date) + INTERVAL 1 DAY) SETTINGS index_granularity = 32, index_granularity_bytes = 2048'

    >>> schema = ''
    >>> engine_full_from_dict('CollapsingMergeTree', {'sign': 'sign_column'}, schema=schema)
    Traceback (most recent call last):
    ...
    ValueError: Invalid value 'sign_column' for parameter 'engine_sign', reason: 'sign_column' column is not present in schema

    >>> schema = 'sign_column Int8'
    >>> engine_full_from_dict('CollapsingMergeTree', {'sign': 'sign_column'}, schema=schema)
    Traceback (most recent call last):
    ...
    ValueError: Missing required option 'sorting_key'

    >>> schema = 'sign_column Int8, key_column Int8'
    >>> engine_full_from_dict('CollapsingMergeTree', {'sign': 'sign_column', 'sorting_key': 'key_column'}, schema=schema)
    'CollapsingMergeTree(sign_column) ORDER BY (key_column)'

    >>> columns=[]
    >>> columns.append({'name': 'sign_column', 'type': 'Int8', 'codec': None, 'default_value': None, 'nullable': False, 'normalized_name': 'sign_column'})
    >>> columns.append({'name': 'key_column', 'type': 'Int8', 'codec': None, 'default_value': None, 'nullable': False, 'normalized_name': 'key_column'})
    >>> engine_full_from_dict('CollapsingMergeTree', {'sign': 'sign_column', 'sorting_key': 'key_column' }, columns=columns)
    'CollapsingMergeTree(sign_column) ORDER BY (key_column)'

    >>> schema = 'sign_column Int8'
    >>> engine_full_from_dict('AggregatingMergeTree', {}, schema=schema)
    Traceback (most recent call last):
    ...
    ValueError: Missing required option 'sorting_key'

    >>> columns=[]
    >>> columns.append({'name': 'key_column', 'type': 'Int8', 'codec': None, 'default_value': None, 'nullable': False, 'normalized_name': 'key_column'})
    >>> engine_full_from_dict('AggregatingMergeTree', { 'sorting_key': 'key_column' }, columns=columns)
    'AggregatingMergeTree() ORDER BY (key_column)'

    >>> schema = 'ver_column Int8, key_column Int8'
    >>> engine_full_from_dict('ReplacingMergeTree', {}, schema=schema)
    Traceback (most recent call last):
    ...
    ValueError: Missing required option 'sorting_key'

    >>> engine_full_from_dict('ReplacingMergeTree', {'sorting_key': 'key_column'}, schema=schema)
    'ReplacingMergeTree() ORDER BY (key_column)'

    >>> engine_full_from_dict('ReplacingMergeTree', {'ver': 'ver_column', 'sorting_key': 'key_column'}, schema=schema)
    'ReplacingMergeTree(ver_column) ORDER BY (key_column)'

    >>> engine_full_from_dict('ReplacingMergeTree', {'ver': 'other_column'}, schema=schema)
    Traceback (most recent call last):
    ...
    ValueError: Invalid value 'other_column' for parameter 'engine_ver', reason: 'other_column' column is not present in schema

    >>> schema = 'col0 Int8, col1 Int8, col2 Int8'
    >>> engine_full_from_dict('SummingMergeTree', {}, schema=schema)
    'SummingMergeTree() ORDER BY (tuple())'
    >>> engine_full_from_dict('SummingMergeTree', {'columns': 'col0'}, schema=schema)
    'SummingMergeTree(col0) ORDER BY (tuple())'
    >>> engine_full_from_dict('SummingMergeTree', {'columns': 'col0, col2'}, schema=schema)
    'SummingMergeTree(col0, col2) ORDER BY (tuple())'
    >>> engine_full_from_dict('SummingMergeTree', {'columns': 'col1, other_column'}, schema=schema)
    Traceback (most recent call last):
    ...
    ValueError: Invalid value 'col1, other_column' for parameter 'engine_columns', reason: 'other_column' column is not present in schema
    >>> engine_full_from_dict('SummingMergeTree', {'columns': 'col1, other_column'}, schema=schema, columns=[])
    Traceback (most recent call last):
    ...
    ValueError: You can not use 'schema' and 'columns' at the same time
    >>> engine_full_from_dict('ReplacingMergeTree',  {'partition_key': 'tuple()', 'sorting_key': 'project_id', 'settings': 'index_granularity = 32', 'ver': 'updated_at'}, "`project_id` String, `project_name` String, `project_repo` String, `owner_id` String, `updated_at` DateTime64(3)")
    'ReplacingMergeTree(updated_at) PARTITION BY (tuple()) ORDER BY (project_id) SETTINGS index_granularity = 32'
    """

    if schema is not None and columns is not None:
        raise ValueError("You can not use 'schema' and 'columns' at the same time")
    engine_config = get_engine_config(engine)
    name, (params, options) = engine_config
    if columns is None and schema is not None:
        columns = parse_table_structure(schema)

    engine_settings = {key.replace("engine_", ""): value for key, value in args.items()}

    for arg in engine_settings:
        if not hasattr(TableDetails, arg):
            raise ValueError(f"engine_{arg} is not a valid option")

    return build_engine(name, columns, params, options, engine_settings)


def engine_params_from_engine_full(engine_full: str) -> Dict[str, Any]:
    """
    >>> engine_params_from_engine_full("Null()")
    {}
    >>> engine_params_from_engine_full("Join(ANY, LEFT, id)")
    {'join_strictness': 'ANY', 'join_type': 'LEFT', 'key_columns': 'id'}
    >>> engine_params_from_engine_full("Join(ANY, LEFT, k1, k2)")
    {'join_strictness': 'ANY', 'join_type': 'LEFT', 'key_columns': 'k1, k2'}
    >>> engine_params_from_engine_full("AggregatingMergeTree('/clickhouse/tables/{layer}-{shard}/d_f837aa.sales_by_country_rt__v0_staging_t_00c3091e7530472caebda05e97288a1d', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY (purchase_location, cod_device, date) SETTINGS index_granularity = 8192")
    {}
    >>> engine_params_from_engine_full("ReplicatedSummingMergeTree('/clickhouse/tables/{layer}-{shard}/d_abcf3e.t_69f9da31f4524995b8911e1b24c80ab4', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY (date, purchase_location, sku_rank_lc) SETTINGS index_granularity = 8192")
    {}
    >>> engine_params_from_engine_full("ReplicatedSummingMergeTree('/clickhouse/tables/{layer}-{shard}/d_abcf3e.t_69f9da31f4524995b8911e1b24c80ab4', '{replica}', c1, c2) PARTITION BY toYYYYMM(date) ORDER BY (date, purchase_location, sku_rank_lc) SETTINGS index_granularity = 8192")
    {'columns': 'c1, c2'}
    >>> engine_params_from_engine_full("ReplacingMergeTree(insert_date) ORDER BY date")
    {'ver': 'insert_date'}
    >>> engine_params_from_engine_full("ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/d_f837aa.t_d3aaad001dee4d9e9e3067ccb789fb59_n1', '{replica}', insert_date) ORDER BY pk TTL toDate(local_timeplaced) + toIntervalDay(3) SETTINGS index_granularity = 8192")
    {'ver': 'insert_date'}
    >>> engine_params_from_engine_full("ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/{layer}-{shard}/test.foo', '{replica}', sign_c,version_c) ORDER BY pk TTL toDate(local_timeplaced) + toIntervalDay(3) SETTINGS index_granularity = 8192")
    {'sign': 'sign_c', 'version': 'version_c'}
    >>> engine_params_from_engine_full("ReplacingMergeTree(updated_at) PARTITION BY tuple() PRIMARY KEY project_id ORDER BY project_id SETTINGS index_granularity = 32")
    {'ver': 'updated_at'}
    """
    engine_full = engine_replicated_to_local(engine_full)
    for engine, (params, _options) in ENABLED_ENGINES:
        if engine_full.startswith(engine):
            m = re.search(rf"{engine}\(([^\)]*)\).*", engine_full)
            params_used = []
            if m:
                params_used = [x.strip() for x in m.group(1).split(",")]
            params_dict = defaultdict(list)
            param = None
            for i, v in enumerate(params_used):
                if i < len(params):
                    param = params[i]
                if param and v:
                    params_dict[param.name].append(v)

            return {k: ", ".join(v) for k, v in params_dict.items()}
    return {}


def engine_local_to_replicated(engine: str, database: str, name: str) -> str:
    """
    transforms an engine definition to a replicated one

    >>> engine_local_to_replicated('MergeTree() order by (test)', 'test', 'foo')
    "ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/test.foo','{replica}') order by (test)"
    >>> engine_local_to_replicated('MergeTree order by (test)', 'test', 'foo')
    "ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/test.foo','{replica}') order by (test)"
    >>> engine_local_to_replicated('ReplacingMergeTree(timestamp) order by (test)', 'test', 'foo')
    "ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/test.foo','{replica}',timestamp) order by (test)"
    >>> engine_local_to_replicated('AggregatingMergeTree order by (test)', 'test', 'foo')
    "ReplicatedAggregatingMergeTree('/clickhouse/tables/{layer}-{shard}/test.foo','{replica}') order by (test)"
    >>> engine_local_to_replicated('AggregatingMergeTree order by (test) settings index_granularity = 8129', 'test', 'foo')
    "ReplicatedAggregatingMergeTree('/clickhouse/tables/{layer}-{shard}/test.foo','{replica}') order by (test) settings index_granularity = 8129"
    """

    def _replace(m: Any) -> str:
        parts = m.groups()

        engine_type = parts[0]
        engine_args = f",{parts[2]}" if parts[2] else ""
        engine_settings = parts[3]
        replication_args = f"'/clickhouse/tables/{{layer}}-{{shard}}/{database}.{name}','{{replica}}'"

        return f"Replicated{engine_type}MergeTree({replication_args}{engine_args}){engine_settings}"

    return re.sub(r"(.*)MergeTree(\(([^\)]*)\))*(.*)", _replace, engine.strip())


def ttl_condition_from_engine_full(engine_full: Optional[str]) -> Optional[str]:
    """
    >>> ttl_condition_from_engine_full(None)

    >>> ttl_condition_from_engine_full("ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/{layer}-{shard}/test.foo', '{replica}', sign_c,version_c) ORDER BY pk TTL toDate(local_timeplaced) + toIntervalDay(3) SETTINGS index_granularity = 8192")
    'toDate(local_timeplaced) >= now() - toIntervalDay(3)'
    >>> ttl_condition_from_engine_full("ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/{layer}-{shard}/test.foo', '{replica}', sign_c,version_c) ORDER BY pk TTL local_timeplaced + toIntervalDay(3) SETTINGS index_granularity = 8192")
    'local_timeplaced >= now() - toIntervalDay(3)'
    >>> ttl_condition_from_engine_full("ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/{layer}-{shard}/test.foo', '{replica}', sign_c,version_c) ORDER BY pk TTL toDate(local_timeplaced / 1000) + toIntervalDay(3) SETTINGS index_granularity = 8192")
    'toDate(local_timeplaced / 1000) >= now() - toIntervalDay(3)'
    >>> ttl_condition_from_engine_full("ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/{layer}-{shard}/test.foo', '{replica}', sign_c,version_c) ORDER BY pk TTL toStartOfWeek(local_timeplaced) + toIntervalDay(3) SETTINGS index_granularity = 8192")
    'toStartOfWeek(local_timeplaced) >= now() - toIntervalDay(3)'
    >>> ttl_condition_from_engine_full("ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/{layer}-{shard}/test.foo', '{replica}', sign_c,version_c) ORDER BY pk TTL toDateTime(fromUnixTimestamp64Milli(-sortingNegativeTS)) + toIntervalDay(3) SETTINGS index_granularity = 8192")
    'toDateTime(fromUnixTimestamp64Milli(-sortingNegativeTS)) >= now() - toIntervalDay(3)'
    >>> ttl_condition_from_engine_full("ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/{layer}-{shard}/test.foo', '{replica}', sign_c,version_c) ORDER BY pk SETTINGS index_granularity = 8192")

    >>> ttl_condition_from_engine_full("ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/d_test_4683b7e9803547218ba5010eb0364233.t_42fc0805b31f4763991cccefa71eeda3', '{replica}') PARTITION BY toYear(date) ORDER BY date TTL toDate(date) + toIntervalDay(1) SETTINGS index_granularity = 8192")
    'toDate(date) >= now() - toIntervalDay(1)'
    >>> ttl_condition_from_engine_full("MergeTree() PARTITION BY toYear(date) ORDER BY date TTL toDate(date) + toIntervalDay(1)")
    'toDate(date) >= now() - toIntervalDay(1)'
    >>> ttl_condition_from_engine_full("ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/d_03d680.t_ea9e3e784ef149caa1fcd5d772e61c42', '{replica}') PARTITION BY toStartOfHour(snapshot_id) ORDER BY (snapshot_id, ID_LOCALIZACION, ID_INSTALACION_RFID, COD_PRODUCTO_AS400, MODELO, CALIDAD, COLOR, TALLA, UBICACION_RFID) TTL snapshot_id + toIntervalHour(1) SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, merge_with_ttl_timeout = 1800")
    'snapshot_id >= now() - toIntervalHour(1)'
    >>> ttl_condition_from_engine_full("ReplicatedAggregatingMergeTree('/clickhouse/tables/{layer}-{shard}/d_03d680.t_9ffe6f6790be4fae908685d3da4ee6f1', '{replica}') PARTITION BY snapshot_id ORDER BY (snapshot_id, COD_PRODUCTO_AS400, MODELO, CALIDAD) TTL snapshot_id + toIntervalHour(1) SETTINGS index_granularity = 8192")
    'snapshot_id >= now() - toIntervalHour(1)'
    >>> ttl_condition_from_engine_full("MergeTree() PARTITION BY toYYYYMM(t) ORDER BY (t, c) TTL t + toIntervalDay(90) SETTINGS index_granularity = 8192")
    't >= now() - toIntervalDay(90)'
    >>> ttl_condition_from_engine_full("MergeTree() PARTITION BY toYYYYMM(t) ORDER BY (t, c) TTL t + toIntervalDay(90)")
    't >= now() - toIntervalDay(90)'
    >>> ttl_condition_from_engine_full("MergeTree ORDER BY a TTL (toStartOfDay(a) + toIntervalSecond(b)) + toIntervalDay(1) SETTINGS index_granularity = 8192")

    >>> ttl_condition_from_engine_full("MergeTree ORDER BY a TTL (toStartOfDay(a) - toIntervalSecond(b)) + toIntervalSecond(1800) SETTINGS index_granularity = 8192")
    '(toStartOfDay(a) - toIntervalSecond(b)) >= now() - toIntervalSecond(1800)'
    >>> ttl_condition_from_engine_full("MergeTree ORDER BY col TTL parseDateTimeBestEffortOrZero(toString(round(epoch / 1000))) + toIntervalDay(2)")
    'parseDateTimeBestEffortOrZero(toString(round(epoch / 1000))) >= now() - toIntervalDay(2)'
    >>> ttl_condition_from_engine_full("MergeTree ORDER BY col TTL toDateTime(fromUnixTimestamp64Milli(-sortingNegativeTS)) + toIntervalDay(10)")
    'toDateTime(fromUnixTimestamp64Milli(-sortingNegativeTS)) >= now() - toIntervalDay(10)'

    # Unsupported currently
    >>> ttl_condition_from_engine_full("MergeTree ORDER BY col TTL toDate(__timestamp) + 60")

    >>> ttl_condition_from_engine_full("ttl")

    """
    if not engine_full:
        return None

    try:
        ttl_array = engine_full.split(" TTL ")
        if len(ttl_array) <= 1:
            return None
        settings_array = engine_full.split(" SETTINGS ")
        settings = " SETTINGS " + settings_array[1] if len(settings_array) > 1 else None
        ttl = ttl_array[1][: -(len(settings))] if settings else ttl_array[1]

        groups = SIMPLE_TTL_DEFINITION.search(ttl)
        if not groups:
            return None

        return f"{groups[1]} >= now() - {groups[2]}"

    except Exception as e:
        logging.error(str(e))
        return None


def _parse_indexes(create_table_query_expr: str) -> List[TableIndex]:
    if create_table_query_expr == "":
        return []
    try:
        from tinybird.sql_toolset import format_sql

        indexes = [
            line.strip()
            for line in format_sql(create_table_query_expr).splitlines()
            if line.strip().startswith("INDEX")
        ]
    except ModuleNotFoundError:
        # this is not needed from CLI
        return []

    return parse_indexes_structure(indexes)


def _parse_projections(create_table_query_expr: str) -> List[TableProjection]:
    return [
        TableProjection(name, expr)
        for name, expr in re.findall(
            r"PROJECTION\s+(\w+)\s*\(((?:[^()]|\((?:[^()]|\([^()]*\))*\))*)\)", create_table_query_expr
        )
    ]
