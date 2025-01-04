import copy
import logging
from collections import defaultdict
from datetime import datetime
from functools import lru_cache
from typing import Any, FrozenSet, List, Optional, Set, Tuple

from chtoolset import query as chquery
from toposort import toposort

from tinybird.ch_utils.constants import COPY_ENABLED_TABLE_FUNCTIONS, ENABLED_TABLE_FUNCTIONS

VALID_REMOTE = "VALID_REMOTE"


class InvalidFunction(ValueError):
    def __init__(self, msg: str = "", table_function_name: str = ""):
        if any([fn for fn in COPY_ENABLED_TABLE_FUNCTIONS if fn in msg]):
            msg = msg.replace("is restricted", "is restricted to Copy Pipes")

        if table_function_name:
            if table_function_name in COPY_ENABLED_TABLE_FUNCTIONS:
                self.msg = f"The {table_function_name} table function is only allowed in Copy Pipes"
            else:
                self.msg = f"The query uses disabled table functions: '{table_function_name}'"
        else:
            self.msg = msg
        super().__init__(self.msg)


class InvalidResource(ValueError):
    def __init__(self, database: str, table: str, default_database: str = ""):
        if default_database and database == default_database:
            database = ""
        self.msg = f"{database}.{table}" if database else table
        self.msg = f"Resource '{self.msg}' not found"
        super().__init__(self.msg)
        self.database = database
        self.table = table


def format_sql(sql: str) -> str:
    return chquery.format(sql)


def format_where_for_mutation_command(where_clause: str) -> str:
    """
    >>> format_where_for_mutation_command("numnights = 99")
    'DELETE WHERE numnights = 99'
    >>> format_where_for_mutation_command("\\nnumnights = 99")
    'DELETE WHERE numnights = 99'
    >>> format_where_for_mutation_command("reservationid = 'foo'")
    "DELETE WHERE reservationid = \\\\'foo\\\\'"
    >>> format_where_for_mutation_command("reservationid = '''foo'")
    "DELETE WHERE reservationid = \\\\'\\\\\\\\\\\\'foo\\\\'"
    >>> format_where_for_mutation_command("reservationid = '\\\\'foo'")
    "DELETE WHERE reservationid = \\\\'\\\\\\\\\\\\'foo\\\\'"
    """
    formatted_condition = chquery.format(f"""SELECT {where_clause}""").split("SELECT ")[1]
    formatted_condition = formatted_condition.replace("\\", "\\\\").replace("'", "''")
    quoted_condition = chquery.format(f"SELECT '{formatted_condition}'").split("SELECT ")[1]
    return f"DELETE WHERE {quoted_condition[1:-1]}"


@lru_cache(maxsize=2**13)
def sql_get_used_tables_cached(
    sql: str,
    raising: bool = False,
    default_database: str = "",
    table_functions: bool = True,
    function_allow_list: Optional[FrozenSet[str]] = None,
) -> List[Tuple[str, str, str]]:
    """More like: get used sql names

    Returns a list of tuples: (database_or_namespace, table_name, table_func).
    >>> sql_get_used_tables("SELECT 1 FROM the_table")
    [('', 'the_table', '')]
    >>> sql_get_used_tables("SELECT 1 FROM the_database.the_table")
    [('the_database', 'the_table', '')]
    >>> sql_get_used_tables("SELECT * from numbers(100)")
    [('', '', 'numbers')]
    >>> sql_get_used_tables("SELECT * FROM table1, table2")
    [('', 'table1', ''), ('', 'table2', '')]
    >>> sql_get_used_tables("SELECT * FROM table1, table2", table_functions=False)
    [('', 'table1', ''), ('', 'table2', '')]
    >>> sql_get_used_tables("SELECT * FROM numbers(100)", table_functions=False)
    []
    >>> sql_get_used_tables("SELECT * FROM table1, numbers(100)", table_functions=False)
    [('', 'table1', '')]
    >>> sql_get_used_tables("SELECT * FROM `d_d3926a`.`t_976af08ec4b547419e729c63e754b17b`", table_functions=False)
    [('d_d3926a', 't_976af08ec4b547419e729c63e754b17b', '')]
    """
    try:
        _function_allow_list = list() if function_allow_list is None else list(function_allow_list)

        tables: List[Tuple[str, str, str]] = chquery.tables(
            sql, default_database=default_database, function_allow_list=_function_allow_list
        )
        if not table_functions:
            return [(t[0], t[1], "") for t in tables if t[0] or t[1]]
        return tables
    except ValueError as e:
        if raising:
            msg = str(e)
            if "is restricted. Contact support@tinybird.co" in msg:
                raise InvalidFunction(msg=msg) from e
            elif "Unknown function tb_secret" in msg:
                raise InvalidFunction(msg="Unknown function tb_secret. Usage: {{tb_secret('secret_name')}}") from e
            elif "Unknown function tb_var" in msg:
                raise InvalidFunction(msg="Unknown function tb_var. Usage: {{tb_var('var_name')}}") from e
            raise
        return [(default_database, sql, "")]


def sql_get_used_tables(
    sql: str,
    raising: bool = False,
    default_database: str = "",
    table_functions: bool = True,
    function_allow_list: Optional[FrozenSet[str]] = None,
) -> List[Tuple[str, str, str]]:
    """More like: get used sql names

    Returns a list of tuples: (database_or_namespace, table_name, table_func).
    """
    hashable_list = frozenset() if function_allow_list is None else function_allow_list

    return copy.copy(
        sql_get_used_tables_cached(sql, raising, default_database, table_functions, function_allow_list=hashable_list)
    )


class ReplacementsDict(dict):
    def __getitem__(self, key):
        v = super().__getitem__(key)
        if isinstance(v, tuple):
            k, r = v
            if callable(r):
                r = r()
                super().__setitem__(key, (k, r))
            return k, r
        if callable(v):
            v = v()
            super().__setitem__(key, v)
        return v


def tables_or_sql(replacement: dict, table_functions=False) -> set:
    try:
        return set(
            sql_get_used_tables(
                replacement[1], default_database=replacement[0], raising=True, table_functions=table_functions
            )
        )
    except Exception as e:
        if replacement[1][0] == "(":
            raise e
        return {replacement}


def _separate_as_tuple_if_contains_database_and_table(definition: str) -> Any:
    if "." in definition:
        database_and_table_separated = definition.split(".")
        return database_and_table_separated[0], database_and_table_separated[1]
    return definition


def replacements_to_tuples(replacements: dict) -> dict:
    parsed_replacements = {}
    for k, v in replacements.items():
        parsed_replacements[_separate_as_tuple_if_contains_database_and_table(k)] = (
            _separate_as_tuple_if_contains_database_and_table(v)
        )
    return parsed_replacements


@lru_cache(maxsize=2**13)
def replace_tables_chquery_cached(
    sql: str,
    sorted_replacements: Optional[tuple] = None,
    default_database: str = "",
    output_one_line: bool = False,
    timestamp: Optional[datetime] = None,
    function_allow_list: Optional[FrozenSet[str]] = None,
) -> str:
    replacements = dict(sorted_replacements) if sorted_replacements else {}
    _function_allow_list = list() if function_allow_list is None else list(function_allow_list)

    return chquery.replace_tables(
        sql,
        replacements,
        default_database=default_database,
        one_line=output_one_line,
        function_allow_list=_function_allow_list,
    )


def replace_tables(
    sql: str,
    replacements: dict,
    default_database: str = "",
    check_functions: bool = False,
    only_replacements: bool = False,
    valid_tables: Optional[Set[Tuple[str, str]]] = None,
    output_one_line: bool = False,
    timestamp: Optional[datetime] = None,
    function_allow_list: Optional[FrozenSet[str]] = None,
) -> str:
    """
    Given a query and a list of table replacements, returns the query after applying the table replacements.
    It takes into account dependencies between replacement subqueries (if any)
    It also validates the sql to verify it's valid and doesn't use unknown or prohibited functions
    """
    hashable_list = frozenset() if function_allow_list is None else function_allow_list
    if not replacements:
        # Always call replace_tables to do validation and formatting
        return replace_tables_chquery_cached(
            sql, None, output_one_line=output_one_line, timestamp=timestamp, function_allow_list=hashable_list
        )

    _replaced_with = set()
    _replacements = ReplacementsDict()
    for k, r in replacements.items():
        rk = k if isinstance(k, tuple) else (default_database, k)
        _replacements[rk] = r if isinstance(r, tuple) else (default_database, r)
        _replaced_with.add(r)

    deps: defaultdict = defaultdict(set)
    _tables = sql_get_used_tables(
        sql,
        default_database=default_database,
        raising=True,
        table_functions=check_functions,
        function_allow_list=function_allow_list,
    )
    seen_tables = set()
    table: Tuple[str, str] | Tuple[str, str, str]
    if function_allow_list is None:
        _enabled_table_functions = ENABLED_TABLE_FUNCTIONS
    else:
        _enabled_table_functions = ENABLED_TABLE_FUNCTIONS.union(set(function_allow_list))
    while _tables:
        table = _tables.pop()
        if len(table) == 3:
            first_table, second_table, last_table = table
            if last_table and last_table not in _enabled_table_functions:
                raise InvalidFunction(table_function_name=last_table)
            if first_table or second_table:
                table = (first_table, second_table)
            else:
                continue
        seen_tables.add(table)
        if table in _replacements:
            replacement = _replacements[table]
            dependent_tables = tables_or_sql(replacement, table_functions=check_functions)
            deps[table] |= {(d[0], d[1]) for d in dependent_tables}
            for dependent_table in list(dependent_tables):
                if len(dependent_table) == 3:
                    if (
                        dependent_table[2]
                        and dependent_table[2] not in _enabled_table_functions
                        and not (dependent_table[2] in ["cluster"] and replacement[0] == VALID_REMOTE)
                    ):
                        raise InvalidFunction(table_function_name=dependent_table[2])
                    if dependent_table[0] or dependent_table[1]:
                        dependent_table = (dependent_table[0], dependent_table[1])
                    else:
                        continue
                if dependent_table not in seen_tables:
                    _tables.append(dependent_table)
        else:
            deps[table] |= set()
    deps_sorted = list(reversed(list(toposort(deps))))

    if not deps_sorted:
        return replace_tables_chquery_cached(
            sql, None, output_one_line=output_one_line, timestamp=timestamp, function_allow_list=hashable_list
        )

    for current_deps in deps_sorted:
        current_replacements = {}
        for r in current_deps:
            if r in _replacements:
                replacement = _replacements[r]
                current_replacements[r] = replacement
            else:
                if only_replacements:
                    continue
                database, table_name = r
                if (
                    table_name
                    and default_database != ""
                    and is_invalid_resource(r, database, default_database, _replaced_with, valid_tables)
                ):
                    logging.info(
                        "Resource not found in replace_tables in sql_toolset: %s",
                        {
                            "r": r,
                            "default_database": default_database,
                            "_replaced_with": _replaced_with,
                            "valid_tables": valid_tables,
                        },
                    )
                    raise InvalidResource(database, table_name, default_database=default_database)

        if current_replacements:
            # We need to transform the dictionary into something cacheable, so a sorted tuple of tuples it is
            r = tuple(sorted([(k, v) for k, v in current_replacements.items()]))
            sql = replace_tables_chquery_cached(
                sql,
                r,
                default_database=default_database,
                output_one_line=output_one_line,
                timestamp=timestamp,
                function_allow_list=hashable_list,
            )
        else:
            sql = replace_tables_chquery_cached(
                sql, None, output_one_line=output_one_line, timestamp=timestamp, function_allow_list=hashable_list
            )

    return sql


def is_invalid_resource(
    r: Tuple[str, str],
    database: str,
    default_database: str,
    _replaced_with: Set[Tuple[str, str]],
    valid_tables: Optional[Set[Tuple[str, str]]] = None,
) -> bool:
    return is_invalid_resource_from_other_workspace(
        r, database, default_database, _replaced_with
    ) or is_invalid_resource_from_current_workspace(r, database, default_database, _replaced_with, valid_tables)


def is_invalid_resource_from_other_workspace(
    r: Tuple[str, str], database: str, default_database: str, _replaced_with: Set[Tuple[str, str]]
) -> bool:
    return database not in [default_database, "tinybird", VALID_REMOTE] and r not in _replaced_with


def is_invalid_resource_from_current_workspace(
    r: Tuple[str, str],
    database: str,
    default_database: str,
    _replaced_with: Set[Tuple[str, str]],
    valid_tables: Optional[Set[Tuple[str, str]]],
) -> bool:
    return bool(database == default_database and valid_tables and r not in valid_tables and r not in _replaced_with)
