import ast
import builtins
import linecache
import logging
import re
from datetime import datetime
from functools import lru_cache
from io import StringIO
from json import loads
from typing import Any, Dict, List, Optional, Tuple, Union

from tornado import escape
from tornado.util import ObjectDict, exec_in, unicode_type

from tinybird.context import ff_preprocess_parameters_circuit_breaker, ff_split_to_array_escape

from .datatypes import testers
from .tornado_template import VALID_CUSTOM_FUNCTION_NAMES, SecurityException, Template

TB_SECRET_IN_TEST_MODE = "tb_secret_dont_raise"
TB_SECRET_PREFIX = "tb_secret_"
CH_PARAM_PREFIX = "param_"
REQUIRED_PARAM_NOT_DEFINED = "Required parameter is not defined"


def secret_template_key(secret_name: str) -> str:
    return f"{TB_SECRET_PREFIX}{secret_name}"


def is_secret_template_key(key: str) -> bool:
    return key.startswith(TB_SECRET_PREFIX)


class TemplateExecutionResults(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.template_params = set()
        self.ch_params = set()

    def add_template_param(self, param: str):
        self.template_params.add(param)

    def add_ch_param(self, param: str):
        self.ch_params.add(param)

    def update_all(self, other: "TemplateExecutionResults"):
        self.update(other)
        self.ch_params.update(other.ch_params)
        self.template_params.update(other.template_params)


class SQLTemplateCustomError(Exception):
    def __init__(self, err, code=400):
        self.code = code
        self.err = err
        super().__init__(err)


class SQLTemplateException(ValueError):
    def __init__(self, err, documentation=None):
        self.documentation = documentation
        super().__init__(f"Template Syntax Error: {str(err)}")


# t = Template(""" SELECT * from test where lon between {{Float32(lon1, 0)}} and {{Float32(lon2, 0)}} """)
# names = get_var_names(t)
# print(generate(t, **{x: '' for x in names}))

# t = Template(""" SELECT * from test where lon between {{lon1}} and {{lon2}} """)
# names = get_var_names(t)
# replace_vars_smart(t)
# print(generate(t, **{x: '' for x in names}))


DEFAULT_PARAM_NAMES = ["format", "q"]
RESERVED_PARAM_NAMES = [
    "__tb__semver",
    "debug_source_tables",
    "debug",
    "explain",
    "finalize_aggregations",
    "output_format_json_quote_64bit_integers",
    "output_format_json_quote_denormals",
    "output_format_parquet_string_as_string",
    "pipeline",
    "playground",
    "q",
    "query_id",
    "release_replacements",
    "tag",
    "template_parameters",
    "token",
]

parameter_types = [
    "String",
    "Boolean",
    "DateTime64",
    "DateTime",
    "Date",
    "Float32",
    "Float64",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "Int128",
    "Int256",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "UInt128",
    "UInt256",
    "Array",
    "JSON",
]


def transform_type(
    tester, transform, placeholder=None, required=None, description=None, enum=None, example=None, format=None
):
    def _f(x, default=None, defined=True, required=None, description=None, enum=None, example=None, format=None):
        if isinstance(x, Placeholder):
            if default:
                x = default
            else:
                x = placeholder
        elif x is None:
            x = default
            if x is None:
                if defined:
                    raise SQLTemplateException(REQUIRED_PARAM_NOT_DEFINED, documentation="/cli/advanced-templates.html")
                else:
                    return None
        if tester == "String":
            if x is not None:
                return transform(x)
        elif testers[tester](str(x)):
            return transform(x)
        raise SQLTemplateException(
            f"Error validating '{x}' to type {tester}", documentation="/cli/advanced-templates.html"
        )

    return _f


def _and(*args, **kwargs):
    operands = {"in": "in", "not_in": "not in", "gt": ">", "lt": "<", "gte": ">=", "lte": "<="}

    def _name(k):
        tk = k.rsplit("__", 1)
        return tk[0]

    def _op(k):
        tk = k.rsplit("__", 1)
        if len(tk) == 1:
            return "="
        else:
            if tk[1] in operands:
                return operands[tk[1]]
            raise SQLTemplateException(
                f"operand {tk[1]} not supported", documentation="/cli/advanced-templates.html#sql_and"
            )

    return Expression(
        " and ".join([f"{_name(k)} {_op(k)} {expression_wrapper(v, k)}" for k, v in kwargs.items() if v is not None])
    )


def error(s, code=400):
    raise ValueError(s)


def custom_error(s, code=400):
    raise SQLTemplateCustomError(s, code)


class Expression(str):
    pass


class Comment:
    def __init__(self, s):
        self.text = s

    def __str__(self):
        return self.text


class Placeholder:
    def __init__(self, name=None, line=None):
        self.name = name if name else "__no_value__"
        self.line = line or "unknown"

    def __str__(self):
        return "__no_value__"

    def __getitem__(self, i):
        if i > 2:
            raise IndexError()
        return Placeholder()

    def __add__(self, s):
        return Placeholder()

    def __call__(self, *args, **kwargs):
        raise SQLTemplateException(
            f"'{self.name}' is not a valid function, line {self.line}", documentation="/cli/advanced-templates.html"
        )

    def split(self, ch):
        return [Placeholder(), Placeholder()]

    def startswith(self, c):
        return False


class Symbol:
    def __init__(self, x):
        self.x = x

    def __str__(self):
        return self.x


class Integer(int):
    def __new__(self, value, type):
        return int.__new__(self, value)

    def __init__(self, value, type):
        int.__init__(value)
        self.type = type

    def __str__(self):
        return f"to{self.type}('{int(self)}')"


class Float(float):
    def __new__(self, value, type):
        return float.__new__(self, value)

    def __init__(self, value, type):
        float.__init__(value)
        self.type = type

    def __str__(self):
        return f"to{self.type}('{float(self)}')"


def columns(x, default=None, fn=None):
    if x is None or isinstance(x, Placeholder):
        if default is None:
            raise SQLTemplateException(
                "Missing columns() default value, use `columns(column_names, 'default_column_name')`",
                documentation="/cli/advanced-templates.html#columns",
            )
        x = default

    try:
        _columns = [c.strip() for c in x.split(",")]
    except AttributeError:
        raise SQLTemplateException(
            "The 'columns' function expects a String not an Array", documentation="/cli/advanced-templates.html#columns"
        )

    if fn:
        return Expression(",".join(f"{fn}({str(column(c, c))}) as {c}" for c in _columns))
    else:
        return Expression(",".join(str(column(c, c)) for c in _columns))


def column(x, default=None):
    if x is None or isinstance(x, Placeholder):
        if default is None:
            raise SQLTemplateException(
                "Missing column() default value, use `column(column_name, 'default_column_name')`",
                documentation="/cli/advanced-templates.html#column",
            )
        x = default
    return Symbol("`" + sqlescape(x) + "`")


def symbol(x, quote="`"):
    if isinstance(x, Placeholder):
        return Symbol("`placeholder`")
    return Symbol(quote + sqlescape(x) + quote)


def table(x, quote="`"):
    if isinstance(x, Placeholder):
        return Symbol("placeholder")
    return Symbol(sqlescape(x))


# ClickHouse does not have a boolean type. Docs suggest to use 1/0:
#
#     https://clickhouse.com/docs/en/sql-reference/data-types/boolean/
#
def boolean(x, default=None):
    """
    >>> boolean(True)
    1
    >>> boolean(False)
    0
    >>> boolean('TRUE')
    1
    >>> boolean('FALSE')
    0
    >>> boolean('true')
    1
    >>> boolean('false')
    0
    >>> boolean(1)
    1
    >>> boolean(0)
    0
    >>> boolean('1')
    1
    >>> boolean('0')
    0
    >>> boolean(None)
    0
    >>> boolean(None, default=True)
    1
    >>> boolean(None, default=False)
    0
    >>> boolean(None, default='TRUE')
    1
    >>> boolean(None, default='FALSE')
    0
    >>> boolean(Placeholder())
    0
    >>> boolean(Placeholder(), default=True)
    1
    >>> boolean(Placeholder(), default=False)
    0
    >>> boolean(Placeholder(), default='TRUE')
    1
    >>> boolean(Placeholder(), default='FALSE')
    0
    """
    if x is None:
        if default is None:
            return 0
        return boolean(default)
    elif isinstance(x, Placeholder):
        return boolean(default)
    elif isinstance(x, str):
        if x == "0" or x.lower() == "false":
            return 0

    return int(bool(x))


def defined(x=None):
    if isinstance(x, Placeholder) or x is None:
        return False
    return True


def array_type(types):
    def _f(
        x, _type=None, default=None, defined=True, required=None, description=None, enum=None, example=None, format=None
    ):
        try:
            if isinstance(x, Placeholder):
                if default:
                    x = default
                else:
                    if _type and _type in types:
                        x = ",".join(map(str, [types[_type](x) for _ in range(2)]))
                    else:
                        x = ",".join([f"__no_value__{i}" for i in range(2)])
            elif x is None:
                x = default
                if x is None:
                    if defined:
                        raise SQLTemplateException(
                            REQUIRED_PARAM_NOT_DEFINED, documentation="/cli/advanced-templates.html"
                        )
                    else:
                        return None
            values = []
            list_values = x if type(x) == list else x.split(",")  # noqa: E721
            for i, t in enumerate(list_values):
                if _type in testers:
                    if testers[_type](str(t)):
                        values.append(expression_wrapper(types[_type](t), str(t)))
                    else:
                        raise SQLTemplateException(
                            f"Error validating {x}[{i}]({t}) to type {_type}",
                            documentation="/cli/advanced-templates.html",
                        )
                else:
                    values.append(expression_wrapper(types.get(_type, lambda x: x)(t), str(t)))
            return Expression(f"[{','.join(map(str, values))}]")
        except AttributeError as e:
            logging.warning(f"AttributeError on Array: {e}")
            raise SQLTemplateException(
                "transform type function Array is not well defined", documentation="/cli/advanced-templates.html"
            )

    return _f


def sql_unescape(x, what=""):
    """
    unescapes specific characters in a string. It allows to allow some
    special characters to be used, for example in like condictionals

    {{sql_unescape(String(like_filter), '%')}}


    >>> sql_unescape('testing%', '%')
    "'testing%'"
    >>> sql_unescape('testing%', '$')
    "'testing\\\\%'"
    """
    return Expression("'" + sqlescape(x).replace(f"\\{what}", what) + "'")


def split_to_array(x, default="", separator: str = ","):
    try:
        if isinstance(x, Placeholder) or x is None:
            x = default
        return [s.strip() for s in x.split(separator)]
    except AttributeError as e:
        logging.warning(f"warning on split_to_array: {str(e)}")
        raise SQLTemplateException(
            "First argument of split_to_array has to be a value that can be split to a list of elements, but found a PlaceHolder with no value instead",
            documentation="/cli/advanced-templates.html#split_to_array",
        )


def enumerate_with_last(arr):
    """
    >>> enumerate_with_last([1, 2])
    [(False, 1), (True, 2)]
    >>> enumerate_with_last([1])
    [(True, 1)]
    """
    arr_len = len(arr)
    return [(arr_len == i + 1, x) for i, x in enumerate(arr)]


def string_type(x, default=None):
    if isinstance(x, Placeholder):
        if default:
            x = default
        else:
            x = "__no_value__"
    return x


def day_diff(d0, d1, default=None):
    """
    >>> day_diff('2019-01-01', '2019-01-01')
    0
    >>> day_diff('2019-01-01', '2019-01-02')
    1
    >>> day_diff('2019-01-02', '2019-01-01')
    1
    >>> day_diff('2019-01-02', '2019-02-01')
    30
    >>> day_diff('2019-02-01', '2019-01-02')
    30
    >>> day_diff(Placeholder(), Placeholder())
    0
    >>> day_diff(Placeholder(), '')
    0
    """
    try:
        return date_diff_in_days(d0, d1, date_format="%Y-%m-%d")
    except Exception:
        raise SQLTemplateException(
            "invalid date format in function `day_diff`, it must be ISO format date YYYY-MM-DD, e.g. 2018-09-26. For other fotmats, try `date_diff_in_days`",
            documentation="/cli/advanced-templates.html#date_diff_in_days",
        )


def date_diff_in_days(
    d0: Union[Placeholder, str],
    d1: Union[Placeholder, str],
    date_format: str = "%Y-%m-%d",
    default=None,
    backup_date_format=None,
    none_if_error=False,
):
    """
    >>> date_diff_in_days('2019-01-01', '2019-01-01')
    0
    >>> date_diff_in_days('2019-01-01', '2019-01-02')
    1
    >>> date_diff_in_days('2019-01-02', '2019-01-01')
    1
    >>> date_diff_in_days('2019-01-02', '2019-02-01')
    30
    >>> date_diff_in_days('2019-02-01 20:00:00', '2019-01-02 20:00:00', date_format="%Y-%m-%d %H:%M:%S")
    30
    >>> date_diff_in_days('2019-02-01', '2019-01-02')
    30
    >>> date_diff_in_days(Placeholder(), Placeholder())
    0
    >>> date_diff_in_days(Placeholder(), '')
    0
    >>> date_diff_in_days('2019-01-01', '2019/01/01', backup_date_format='%Y/%m/%d')
    0
    >>> date_diff_in_days('2019-01-01', '2019/01/04', backup_date_format='%Y/%m/%d')
    3
    >>> date_diff_in_days('2019/01/04', '2019-01-01', backup_date_format='%Y/%m/%d')
    3
    >>> date_diff_in_days('2019-02-01T20:00:00z', '2019-02-15', '%Y-%m-%dT%H:%M:%Sz', backup_date_format='%Y-%m-%d')
    13
    >>> date_diff_in_days('2019-02-01 20:00:00', '2019-02-15', '%Y-%m-%dT%H:%M:%Sz', backup_date_format='%Y-%m-%d', none_if_error=True) is None
    True
    >>> date_diff_in_days('2019-01-01', '2019-00-02', none_if_error=True) is None
    True
    >>> date_diff_in_days('2019-01-01 00:00:00', '2019-01-02 00:00:00', none_if_error=True) is None
    True
    """
    if isinstance(d0, Placeholder) or isinstance(d1, Placeholder):
        if default:
            return default
        return 0
    try:
        return __date_diff(d0, d1, date_format, backup_date_format, "days", none_if_error)
    except Exception:
        raise SQLTemplateException(
            "invalid date format in function `date_diff_in_days`, it must be ISO format date YYYY-MM-DD, e.g. 2018-09-26",
            documentation="/cli/advanced-templates.html#date_diff_in_days",
        )


def date_diff_in_hours(
    d0: Union[Placeholder, str],
    d1: Union[Placeholder, str],
    date_format: str = "%Y-%m-%d %H:%M:%S",
    default=None,
    backup_date_format=None,
    none_if_error=False,
):
    """
    >>> date_diff_in_hours('2022-12-19T18:42:23.521Z', '2022-12-19T18:42:23.521Z', date_format='%Y-%m-%dT%H:%M:%S.%fz')
    0
    >>> date_diff_in_hours('2022-12-19T20:43:22Z', '2022-12-19T18:42:23Z','%Y-%m-%dT%H:%M:%Sz')
    2
    >>> date_diff_in_hours('2022-12-14 18:42:22', '2022-12-19 18:42:22')
    120
    >>> date_diff_in_hours('2022-12-19 18:42:23.521', '2022-12-19 18:42:24.521','%Y-%m-%d %H:%M:%S.%f')
    0
    >>> date_diff_in_hours(Placeholder(), Placeholder())
    0
    >>> date_diff_in_hours(Placeholder(), '')
    0
    >>> date_diff_in_hours('2022-12-19T03:22:12.102Z', '2022-12-19', date_format='%Y-%m-%dT%H:%M:%S.%fz', backup_date_format='%Y-%m-%d')
    3
    >>> date_diff_in_hours('2022-12-19', '2022-12-19', '%Y-%m-%dT%H:%M:%Sz', backup_date_format='%Y-%m-%d')
    0
    >>> date_diff_in_hours('2022-12-19', '2022-12-18', '%Y-%m-%dT%H:%M:%Sz', backup_date_format='%Y-%m-%d')
    24
    >>> date_diff_in_hours('2022-12-19', '2022-12-19 02:01:00', backup_date_format='%Y-%m-%d')
    2
    >>> date_diff_in_hours('2022-25-19T00:00:03.521Z', '2022-12-19', date_format='%Y-%m-%dT%H:%M:%S.%fz', backup_date_format='%Y-%m-%d', none_if_error=True) is None
    True
    >>> date_diff_in_hours('2022-25-19 00:00:03', '2022-12-19', date_format='%Y-%m-%dT%H:%M:%S.%fz', backup_date_format='%Y-%m-%d', none_if_error=True) is None
    True
    >>> date_diff_in_hours('2022-12-19', '2022-25-19', '%Y-%m-%dT%H:%M:%Sz', backup_date_format='%Y-%m-%d', none_if_error=True) is None
    True
    >>> date_diff_in_hours('2022-12-19', '2022-25-19 00:01:00', backup_date_format='%Y-%m-%d', none_if_error=True) is None
    True
    >>> date_diff_in_hours('2022-12-32 18:42:22', '2022-12-19 18:42:22', none_if_error=True) is None
    True
    >>> date_diff_in_hours('2022-12-18T18:42:22Z', '2022-12-19T18:42:22Z', none_if_error=True) is None
    True
    """
    if isinstance(d0, Placeholder) or isinstance(d1, Placeholder):
        if default:
            return default
        return 0
    try:
        return __date_diff(d0, d1, date_format, backup_date_format, "hours", none_if_error)
    except Exception:
        raise SQLTemplateException(
            "invalid date_format in function `date_diff_in_hours`, defaults to YYYY-MM-DD hh:mm:ss. Or %Y-%m-%d %H:%M:%S [.ssssss]Z, e.g. ms: 2022-12-19T18:42:22.591Z s:2022-12-19T18:42:22Z",
            documentation="/cli/advanced-templates.html#date_diff_in_hours",
        )


def date_diff_in_minutes(
    d0: Union[Placeholder, str],
    d1: Union[Placeholder, str],
    date_format: str = "%Y-%m-%d %H:%M:%S",
    default=None,
    backup_date_format=None,
    none_if_error=False,
):
    """
    >>> date_diff_in_minutes('2022-12-19T18:42:23.521Z', '2022-12-19T18:42:23.521Z', date_format='%Y-%m-%dT%H:%M:%S.%fz')
    0
    >>> date_diff_in_minutes('2022-12-19T18:43:22Z', '2022-12-19T18:42:23Z','%Y-%m-%dT%H:%M:%Sz')
    0
    >>> date_diff_in_minutes('2022-12-14 18:42:22', '2022-12-19 18:42:22')
    7200
    >>> date_diff_in_minutes('2022-12-19 18:42:23.521', '2022-12-19 18:42:24.521','%Y-%m-%d %H:%M:%S.%f')
    0
    >>> date_diff_in_minutes(Placeholder(), Placeholder())
    0
    >>> date_diff_in_minutes(Placeholder(), '')
    0
    >>> date_diff_in_minutes('2022-12-19T03:22:12.102Z', '2022-12-19', date_format='%Y-%m-%dT%H:%M:%S.%fz', backup_date_format='%Y-%m-%d')
    202
    >>> date_diff_in_minutes('2022-12-19', '2022-12-19', '%Y-%m-%dT%H:%M:%Sz', backup_date_format='%Y-%m-%d')
    0
    >>> date_diff_in_minutes('2022-12-19', '2022-12-18', '%Y-%m-%dT%H:%M:%Sz', backup_date_format='%Y-%m-%d')
    1440
    >>> date_diff_in_minutes('2022-12-19', '2022-12-19 00:01:00', backup_date_format='%Y-%m-%d')
    1
    >>> date_diff_in_minutes('2022-25-19T00:00:03.521Z', '2022-12-19', date_format='%Y-%m-%dT%H:%M:%S.%fz', backup_date_format='%Y-%m-%d', none_if_error=True) is None
    True
    >>> date_diff_in_minutes('2022-12-19', '2022-25-19', '%Y-%m-%dT%H:%M:%Sz', backup_date_format='%Y-%m-%d', none_if_error=True) is None
    True
    >>> date_diff_in_minutes('2022-12-19', '2022-25-19 00:01:00', backup_date_format='%Y-%m-%d', none_if_error=True) is None
    True
    >>> date_diff_in_minutes('2022-25-19T00:00:03.521Z', '2022-12-19 00:23:12', date_format='%Y-%m-%dT%H:%M:%S.%fz', backup_date_format='%Y-%m-%d', none_if_error=True) is None
    True
    >>> date_diff_in_minutes('2022-12-14 18:42:22', '2022/12/19 18:42:22', none_if_error=True) is None
    True
    >>> date_diff_in_minutes('2022-12-14 18:42:22', '2022/12/19 18:42:22', date_format='%Y/%m/%dT%H:%M:%S.%fz', none_if_error=True) is None
    True
    """
    if isinstance(d0, Placeholder) or isinstance(d1, Placeholder):
        if default:
            return default
        return 0
    try:
        return __date_diff(d0, d1, date_format, backup_date_format, "minutes", none_if_error)
    except Exception:
        raise SQLTemplateException(
            "invalid date_format in function `date_diff_in_seconds`, defaults to YYYY-MM-DD hh:mm:ss. Or %Y-%m-%d %H:%M:%S [.ssssss]Z, e.g. ms: 2022-12-19T18:42:22.591Z s:2022-12-19T18:42:22Z",
            documentation="/cli/advanced-templates.html#date_diff_in_minutes",
        )


def date_diff_in_seconds(
    d0: Union[Placeholder, str],
    d1: Union[Placeholder, str],
    date_format: str = "%Y-%m-%d %H:%M:%S",
    default=None,
    backup_date_format=None,
    none_if_error=False,
):
    """
    >>> date_diff_in_seconds('2022-12-19T18:42:23.521Z', '2022-12-19T18:42:23.521Z', date_format='%Y-%m-%dT%H:%M:%S.%fz')
    0
    >>> date_diff_in_seconds('2022-12-19T18:42:22Z', '2022-12-19T18:42:23Z','%Y-%m-%dT%H:%M:%Sz')
    1
    >>> date_diff_in_seconds('2022-12-19 18:42:22', '2022-12-19 18:43:22')
    60
    >>> date_diff_in_seconds('2022-12-14 18:42:22', '2022-12-19 18:42:22')
    432000
    >>> date_diff_in_seconds('2022-12-19T18:42:23.521Z', '2022-12-19T18:42:23.531Z','%Y-%m-%dT%H:%M:%S.%fz')
    0
    >>> date_diff_in_seconds('2022-12-19 18:42:23.521', '2022-12-19 18:42:24.521','%Y-%m-%d %H:%M:%S.%f')
    1
    >>> date_diff_in_seconds('2022-12-19T18:42:23.521Z', '2022-12-19T18:44:23.531Z','%Y-%m-%dT%H:%M:%S.%fz')
    120
    >>> date_diff_in_seconds(Placeholder(), Placeholder())
    0
    >>> date_diff_in_seconds(Placeholder(), '')
    0
    >>> date_diff_in_seconds('2022-12-19T00:00:03.521Z', '2022-12-19', date_format='%Y-%m-%dT%H:%M:%S.%fz', backup_date_format='%Y-%m-%d')
    3
    >>> date_diff_in_seconds('2022-12-19', '2022-12-19', '%Y-%m-%dT%H:%M:%Sz', backup_date_format='%Y-%m-%d')
    0
    >>> date_diff_in_seconds('2022-12-19', '2022-12-19 00:01:00', backup_date_format='%Y-%m-%d')
    60
    >>> date_diff_in_seconds('2022-25-19T00:00:03.521Z', '2022-12-19', date_format='%Y-%m-%dT%H:%M:%S.%fz', backup_date_format='%Y-%m-%d', none_if_error=True) is None
    True
    >>> date_diff_in_seconds('2022-12-19', '2022-25-19', '%Y-%m-%dT%H:%M:%Sz', backup_date_format='%Y-%m-%d', none_if_error=True) is None
    True
    >>> date_diff_in_seconds('2022-12-19', '2022-25-19 00:01:00', backup_date_format='%Y-%m-%d', none_if_error=True) is None
    True
    >>> date_diff_in_seconds('2022-10-19T00:00:03.521Z', '2022/12/19', date_format='%Y-%m-%dT%H:%M:%S.%fz', backup_date_format='%Y-%m-%d', none_if_error=True) is None
    True
    >>> date_diff_in_seconds('2022-10-19 00:00:03', '2022-10-19 00:05:03', date_format='%Y-%m-%dT%H:%M:%S.%fz', none_if_error=True) is None
    True
    >>> date_diff_in_seconds('2022/12/19 00:00:03', '2022-10-19 00:05:03', none_if_error=True) is None
    True
    >>> date_diff_in_seconds('2022-25-19 00:00:03', '2022-10-19 00:05:03', none_if_error=True) is None
    True
    """
    if isinstance(d0, Placeholder) or isinstance(d1, Placeholder):
        if default:
            return default
        return 0
    try:
        return __date_diff(d0, d1, date_format, backup_date_format, "seconds", none_if_error)
    except Exception:
        raise SQLTemplateException(
            "invalid date_format in function `date_diff_in_seconds`, defaults to YYYY-MM-DD hh:mm:ss. Or %Y-%m-%d %H:%M:%S [.ssssss]Z, e.g. ms: 2022-12-19T18:42:22.591Z s:2022-12-19T18:42:22Z",
            documentation="/cli/advanced-templates.html#date_diff_in_seconds",
        )


def __date_diff(
    d0: Union[Placeholder, str],
    d1: Union[Placeholder, str],
    date_format: str = "%Y-%m-%d %H:%M:%S",
    backup_date_format=None,
    unit: str = "seconds",
    none_if_error=False,
):
    try:
        formatted_d0 = _parse_datetime(d0, date_format, backup_date_format)
        formatted_d1 = _parse_datetime(d1, date_format, backup_date_format)
        diff = abs(formatted_d1 - formatted_d0).total_seconds()

        if unit == "days":
            return int(diff / 86400)
        elif unit == "hours":
            return int(diff / 3600)
        elif unit == "minutes":
            return int(diff / 60)
        else:
            return int(diff)
    except Exception:
        if none_if_error:
            return None

        raise SQLTemplateException(
            "invalid date_format in function date_diff_* function", documentation="/cli/advanced-templates.html"
        )


def _parse_datetime(date_string, date_format, backup_date_format=None):
    formats = [date_format]
    if backup_date_format:
        formats.append(backup_date_format)

    for fmt in formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue

    raise SQLTemplateException(
        "invalid date_format in function date_diff_* function", documentation="/cli/advanced-templates.html"
    )


def json_type(x, default=None):
    """
    >>> json_type(None, '[]')
    []
    >>> json_type(None)
    {}
    >>> json_type('{"a": 1}')
    {'a': 1}
    >>> json_type('[{"a": 1}]')
    [{'a': 1}]
    >>> json_type({"a": 1})
    {'a': 1}
    >>> json_type([{"a": 1}])
    [{'a': 1}]
    """
    if isinstance(x, Placeholder):
        if default:
            x = default
        else:
            x = "__no_value__"

    try:
        if x is None:
            if isinstance(default, str):
                x = default
            else:
                x = "{}"

        value = ""  # used for exception message
        if isinstance(x, (str, bytes, bytearray)):
            if len(x) > 16:
                value = x[:16] + "..."
            else:
                value = x

            parsed = loads(x)
            x = parsed
    except Exception as e:
        msg = f"Error parsing JSON: '{value}' - {str(e)}"
        raise SQLTemplateException(msg)

    return x


function_list = {
    "columns": columns,
    "table": table,
    "TABLE": table,
    "error": error,
    "custom_error": custom_error,
    "sql_and": _and,
    "defined": defined,
    "column": column,
    "enumerate_with_last": enumerate_with_last,
    "split_to_array": split_to_array,
    "day_diff": day_diff,
    "date_diff_in_days": date_diff_in_days,
    "date_diff_in_hours": date_diff_in_hours,
    "date_diff_in_minutes": date_diff_in_minutes,
    "date_diff_in_seconds": date_diff_in_seconds,
    "sql_unescape": sql_unescape,
    "JSON": json_type,
    # 'enumerate': enumerate
}


def get_transform_types(placeholders=None):
    if placeholders is None:
        placeholders = {}
    types = {
        "bool": boolean,
        "Boolean": boolean,
        "DateTime": transform_type(
            "DateTime",
            str,
            placeholders.get("DateTime", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "DateTime64": transform_type(
            "DateTime64",
            str,
            placeholders.get("DateTime64", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "Date": transform_type(
            "Date",
            str,
            placeholders.get("Date", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "Float32": transform_type(
            "Float32",
            lambda x: Float(x, "Float32"),
            placeholders.get("Float32", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "Float64": transform_type(
            "Float64",
            lambda x: Float(x, "Float64"),
            placeholders.get("Float64", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "Int": transform_type(
            "Int32",
            int,
            placeholders.get("Int", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "Integer": transform_type(
            "Int32",
            int,
            placeholders.get("Int32", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "Int8": transform_type(
            "Int8",
            lambda x: Integer(x, "Int8"),
            placeholders.get("Int8", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "Int16": transform_type(
            "Int16",
            lambda x: Integer(x, "Int16"),
            placeholders.get("Int16", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "Int32": transform_type(
            "Int32",
            lambda x: Integer(x, "Int32"),
            placeholders.get("Int32", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "Int64": transform_type(
            "Int64",
            lambda x: Integer(x, "Int64"),
            placeholders.get("Int64", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "Int128": transform_type(
            "Int128",
            lambda x: Integer(x, "Int128"),
            placeholders.get("Int128", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "Int256": transform_type(
            "Int256",
            lambda x: Integer(x, "Int256"),
            placeholders.get("Int256", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "UInt8": transform_type(
            "UInt8",
            lambda x: Integer(x, "UInt8"),
            placeholders.get("UInt8", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "UInt16": transform_type(
            "UInt16",
            lambda x: Integer(x, "UInt16"),
            placeholders.get("UInt16", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "UInt32": transform_type(
            "UInt32",
            lambda x: Integer(x, "UInt32"),
            placeholders.get("UInt32", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "UInt64": transform_type(
            "UInt64",
            lambda x: Integer(x, "UInt64"),
            placeholders.get("UInt64", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "UInt128": transform_type(
            "UInt128",
            lambda x: Integer(x, "UInt128"),
            placeholders.get("UInt128", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "UInt256": transform_type(
            "UInt256",
            lambda x: Integer(x, "UInt256"),
            placeholders.get("UInt256", None),
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "Symbol": symbol,
        "Column": symbol,
        "String": transform_type(
            "String",
            str,
            placeholder="__no_value__",
            required=None,
            description=None,
            enum=None,
            example=None,
            format=None,
        ),
        "JSON": json_type,
    }

    types["Array"] = array_type(
        {
            "bool": boolean,
            "Boolean": boolean,
            "DateTime": transform_type(
                "DateTime",
                str,
                placeholders.get("DateTime", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "DateTime64": transform_type(
                "DateTime64",
                str,
                placeholders.get("DateTime64", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "Date": transform_type(
                "Date",
                str,
                placeholders.get("Date", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "Float32": transform_type(
                "Float32",
                float,
                placeholders.get("Float32", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "Float64": transform_type(
                "Float64",
                float,
                placeholders.get("Float64", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "Int": transform_type(
                "Int32",
                int,
                placeholders.get("Int", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "Integer": transform_type(
                "Int32",
                int,
                placeholders.get("Int32", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "Int8": transform_type(
                "Int8",
                int,
                placeholders.get("Int8", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "Int16": transform_type(
                "Int16",
                int,
                placeholders.get("Int16", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "Int32": transform_type(
                "Int32",
                int,
                placeholders.get("Int32", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "Int64": transform_type(
                "Int64",
                int,
                placeholders.get("Int64", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "Int128": transform_type(
                "Int128",
                int,
                placeholders.get("Int128", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "Int256": transform_type(
                "Int256",
                int,
                placeholders.get("Int256", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "UInt8": transform_type(
                "UInt8",
                int,
                placeholders.get("UInt8", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "UInt16": transform_type(
                "UInt16",
                int,
                placeholders.get("UInt16", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "UInt32": transform_type(
                "UInt32",
                int,
                placeholders.get("UInt32", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "UInt64": transform_type(
                "UInt64",
                int,
                placeholders.get("UInt64", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "UInt128": transform_type(
                "UInt128",
                int,
                placeholders.get("UInt128", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "UInt256": transform_type(
                "UInt256",
                int,
                placeholders.get("UInt256", None),
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
            "Symbol": symbol,
            "Column": symbol,
            "String": transform_type(
                "String",
                str,
                placeholder="__no_value__",
                required=None,
                description=None,
                enum=None,
                example=None,
                format=None,
            ),
        }
    )

    types.update(function_list)
    return types


type_fns = get_transform_types()
type_fns_check = get_transform_types(
    {
        "DateTime64": "2019-01-01 00:00:00.000",
        "DateTime": "2019-01-01 00:00:00",
        "Date": "2019-01-01",
        "Float32": 0.0,
        "Float64": 0.0,
        "Int": 0,
        "Integer": 0,
        "UInt8": 0,
        "UInt16": 0,
        "UInt32": 0,
        "UInt64": 0,
        "UInt128": 0,
        "UInt256": 0,
        "Int8": 0,
        "Int16": 0,
        "Int32": 0,
        "Int64": 0,
        "Int128": 0,
        "Int256": 0,
        "Symbol": "symbol",
        "JSON": "{}",
    }
)


# from https://github.com/elouajib/sqlescapy/
# MIT license
def sqlescape_generator(translations):
    def sqlscape(str):
        return str.translate(str.maketrans(translations))

    return sqlscape


sqlescape = sqlescape_generator(
    {
        "\0": "\\0",
        "\r": "\\r",
        "\x08": "\\b",
        "\x09": "\\t",
        "\x1a": "\\z",
        "\n": "\\n",
        '"': "",
        "'": "\\'",
        "\\": "\\\\",
        "%": "\\%",
        "`": "\\`",
    }
)

# sqlescape_for_string_expression is only meant to be used when escaping
# string expressions (column=<string expression>) within SQL templates.
# This version includes a specific translation on top of the ones in the
# sqlescape above to escape double quotes (" will be translated into \")
# instead of removing them.
# It'll allow users to use parameter values with strings including double quotes.
sqlescape_for_string_expression = sqlescape_generator(
    {
        "\0": "\\0",
        "\r": "\\r",
        "\x08": "\\b",
        "\x09": "\\t",
        "\x1a": "\\z",
        "\n": "\\n",
        '"': '\\"',
        "'": "\\'",
        "\\": "\\\\",
        "%": "\\%",
        "`": "\\`",
    }
)


def escape_single_quote_str(s):
    return "'" + s.replace("'", "''") + "'"


def expression_wrapper(x, name, escape_arrays: bool = False):
    if type(x) in (unicode_type, bytes, str):
        return "'" + sqlescape_for_string_expression(x) + "'"
    elif isinstance(x, Placeholder):
        return "'__no_value__'"
    elif isinstance(x, Comment):
        return "-- {x} \n"
    if x is None:
        truncated_name = name[:20] + "..." if len(name) > 20 else name
        raise SQLTemplateException(
            f'expression "{truncated_name}" evaluated to null', documentation="/cli/advanced-templates.html"
        )
    if isinstance(x, list) and escape_arrays:
        logging.warning(f"expression_wrapper -> list :{x}:")

        try:
            result = (
                f"[{','.join(escape_single_quote_str(item) if isinstance(item, str) else str(item) for item in x)}]"
            )
            return result
        except Exception as e:
            logging.error(f"Error escaping array: {e}")
    return x


_namespace = {
    "column": column,
    "symbol": symbol,
    "error": error,
    "custom_error": custom_error,
    "_tt_utf8": escape.utf8,  # for internal use
    "_tt_string_types": (unicode_type, bytes),
    "xhtml_escape": lambda x: x,
    "expression_wrapper": expression_wrapper,
    # disable __builtins__ and some other functions
    # they raise a pretty non understandable error but if someone
    # is using them they know what they are trying to do
    # read https://anee.me/escaping-python-jails-849c65cf306e on how to escape from python jails
    "__buildins__": {},
    "__import__": {},
    "__debug__": {},
    "__doc__": {},
    "__name__": {},
    "__package__": {},
    "open": None,
    "close": None,
    "print": None,
    "input": None,
}


reserved_vars = set(["_tt_tmp", "_tt_append", "isinstance", "str", "error", "custom_error", *list(vars(builtins))])
for p in DEFAULT_PARAM_NAMES:  # we handle these in an specific manner
    reserved_vars.discard(p)  # `format` is part of builtins
error_vars = ["error", "custom_error"]


def generate(self, **kwargs) -> Tuple[str, TemplateExecutionResults]:
    """Generate this template with the given arguments."""
    namespace = {}
    template_execution_results = TemplateExecutionResults()
    for key in kwargs.get("tb_secrets", []):
        if is_secret_template_key(key):
            template_execution_results.add_template_param(key)

    if TB_SECRET_IN_TEST_MODE in kwargs:
        template_execution_results[TB_SECRET_IN_TEST_MODE] = None

    def set_tb_secret(x):
        try:
            key = secret_template_key(x)
            if key in template_execution_results.template_params:
                template_execution_results.add_ch_param(x)
                return Symbol("{" + sqlescape(x) + ": String}")
            else:
                is_test_mode = TB_SECRET_IN_TEST_MODE in template_execution_results
                if is_test_mode:
                    return Symbol("{" + sqlescape(x) + ": String}")
                else:
                    raise SQLTemplateException(
                        f"Cannot access secret '{x}'. Check the secret exists in the Workspace and the token has the required scope."
                    )
        except Exception:
            raise SQLTemplateException(
                f"Cannot access secret '{x}'. Check the secret exists in the Workspace and the token has the required scope."
            )

    def set_max_threads(x):
        try:
            template_execution_results["max_threads"] = int(x)
            return Expression(f"-- max_threads {x}\n")
        except Exception:
            return Expression(f"-- max_threads: wrong argument {x}\n")

    def set_backend_hint(hint):
        template_execution_results["backend_hint"] = str(hint)
        if hint is None or hint is False:
            template_execution_results["backend_hint"] = None
        return Expression(f"-- backend_hint {hint}\n")

    def set_cache_ttl(ttl_expression):
        valid_ttl_expressions = ("5s", "1m", "5m", "30m", "1h")
        if ttl_expression not in valid_ttl_expressions:
            raise SQLTemplateException(f"Invalid TTL cache expression, valid expressions are {valid_ttl_expressions}")
        template_execution_results["cache_ttl"] = ttl_expression
        return Expression(f"-- cache_ttl {ttl_expression}\n")

    def set_activate(feature):
        valid_features = ("analyzer", "parallel_replicas")
        if feature not in valid_features:
            raise SQLTemplateException(f"'{feature}' is not a valid 'activate' argument")
        template_execution_results["activate"] = feature
        return Expression(f"-- activate {feature}\n")

    namespace.update(_namespace)
    namespace.update(kwargs)
    namespace.update(
        {
            # __name__ and __loader__ allow the traceback mechanism to find
            # the generated source code.
            "__name__": self.name.replace(".", "_"),
            "__loader__": ObjectDict(get_source=lambda name: self.code),
            "max_threads": set_max_threads,
            "tb_secret": set_tb_secret,
            "tb_var": set_tb_secret,
            "backend_hint": set_backend_hint,
            "cache_ttl": set_cache_ttl,
            "activate": set_activate,
        }
    )

    exec_in(self.compiled, namespace)
    execute = namespace["_tt_execute"]
    # Clear the traceback module's cache of source data now that
    # we've generated a new template (mainly for this module's
    # unittests, where different tests reuse the same name).
    linecache.clearcache()

    try:
        return execute().decode(), template_execution_results
    except SQLTemplateCustomError as e:
        raise e
    except UnboundLocalError as e:
        try:
            message = getattr(e, "msg", str(e)).split("(<string>.generated.py")[0].strip()
            text = getattr(e, "text", message)
            line = None
            try:
                line = re.findall(r"\<string\>:(\d*)", text)
                message = re.sub(r"\<string\>:(\d*)", "", message)
            except TypeError:
                pass

            if line:
                raise SQLTemplateException(f"{message.strip()} line {line[0]}")
            else:
                raise SQLTemplateException(f"{message.strip()}")
        except Exception as e:
            if isinstance(e, SQLTemplateException):
                raise e
            else:
                logging.exception(f"Error on unbound local error: {e}")
                raise ValueError(str(e))
    except TypeError as e:
        error = str(e)
        if "not supported between instances of 'Placeholder' and " in str(e):
            error = f"{str(e)}. If you are using a dynamic parameter, you need to wrap it around a valid Data Type (e.g. Int8(placeholder))"
        raise ValueError(error)
    except Exception as e:
        if "x" in namespace and namespace["x"] and hasattr(namespace["x"], "line") and namespace["x"].line:
            line = namespace["x"].line
            raise ValueError(f"{e}, line {line}")
        raise e


class CodeWriter:
    def __init__(self, file, template):
        self.file = file
        self.current_template = template
        self.apply_counter = 0
        self._indent = 0

    def indent_size(self):
        return self._indent

    def indent(self):
        class Indenter:
            def __enter__(_):
                self._indent += 1
                return self

            def __exit__(_, *args):
                assert self._indent > 0
                self._indent -= 1

        return Indenter()

    def write_line(self, line, line_number, indent=None):
        if indent is None:
            indent = self._indent
        line_comment = "  # %s:%d" % ("<generated>", line_number)
        print("    " * indent + line + line_comment, file=self.file)


def get_var_names(t):
    try:

        def _n(chunks, v):
            for x in chunks:
                line_number = x.line
                if type(x).__name__ == "_ChunkList":
                    _n(x.chunks, v)
                elif type(x).__name__ == "_Expression":
                    c = compile(x.expression, "<string>", "exec", dont_inherit=True)
                    variable_names = [x for x in c.co_names if x not in _namespace and x not in reserved_vars]
                    v += list(map(lambda variable: {"line": line_number, "name": variable}, variable_names))
                elif type(x).__name__ == "_ControlBlock":
                    from io import StringIO

                    buffer = StringIO()
                    writer = CodeWriter(buffer, t)
                    x.generate(writer)
                    c = compile(buffer.getvalue(), "<string>", "exec", dont_inherit=True)
                    variable_names = [x for x in c.co_names if x not in _namespace and x not in reserved_vars]
                    v += list(map(lambda variable: {"line": line_number, "name": variable}, variable_names))
                    _n(x.body.chunks, v)

        var = []
        _n(t.file.body.chunks, var)
        return var
    except SecurityException as e:
        raise SQLTemplateException(e)


def get_var_data(content, node_id=None):
    def node_to_value(x):
        if type(x) in (ast.Bytes, ast.Str):
            return x.s
        elif type(x) == ast.Num:  # noqa: E721
            return x.n
        elif type(x) == ast.NameConstant:  # noqa: E721
            return x.value
        elif type(x) == ast.Name:  # noqa: E721
            return x.id
        elif type(x) == ast.List:  # noqa: E721
            # List can hold different types
            return _get_list_var_data(x)
        elif type(x) == ast.BinOp:  # noqa: E721
            # in this case there could be several variables
            # if that's the case the left one is the main
            r = node_to_value(x.left)
            if not r:
                r = node_to_value(x.right)
            return r
        elif type(x) == ast.Constant:  # noqa: E721
            return x.value
        elif type(x) == ast.UnaryOp and type(x.operand) == ast.Constant:  # noqa: E721
            if type(x.op) == ast.USub:  # noqa: E721
                return x.operand.value * -1
            else:
                return x.operand.value
        else:
            try:
                return x.id
            except Exception:
                # don't let this ruin the parsing
                pass
        return None

    def _get_list_var_data(x):
        if not x.elts:
            return []

        first_elem = x.elts[0]
        if type(first_elem) in (ast.Bytes, ast.Str):
            return [elem.s for elem in x.elts]
        elif type(first_elem) == ast.Num:  # noqa: E721
            return [elem.n for elem in x.elts]
        elif type(first_elem) == ast.NameConstant or type(first_elem) == ast.Constant:  # noqa: E721
            return [elem.value for elem in x.elts]
        elif type(first_elem) == ast.Name:  # noqa: E721
            return [elem.id for elem in x.elts]

        return []

    def _w(parsed):
        vars = {}
        for node in ast.walk(parsed):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                try:
                    func = node.func.id
                    # parse function args
                    args = []
                    for x in node.args:
                        if type(x) == ast.Call:  # noqa: E721
                            vars.update(_w(x))
                        else:
                            args.append(node_to_value(x))

                    kwargs = {}
                    for x in node.keywords:
                        value = node_to_value(x.value)
                        kwargs[x.arg] = value
                        if x.arg == "default":
                            kwargs["default"] = check_default_value(value)
                    if func in VALID_CUSTOM_FUNCTION_NAMES:
                        # Type definition here is set to 'String' because it comes from a
                        # `defined(variable)` expression that does not contain any type hint.
                        # It will be overriden in later definitions or left as is otherwise.
                        # args[0] check is used to avoid adding unnamed parameters found in
                        # templates like: `split_to_array('')`
                        if len(args) and isinstance(args[0], list):
                            raise ValueError(f'"{args[0]}" can not be used as a variable name')
                        if len(args) > 0 and args[0] not in vars and args[0]:
                            vars[args[0]] = {
                                "type": "String",
                                "default": None,
                                "used_in": "function_call",
                            }
                    elif func == "Array":
                        if "default" not in kwargs:
                            default = kwargs.get("default", args[2] if len(args) > 2 and args[2] else None)
                            kwargs["default"] = check_default_value(default)
                        if len(args):
                            if isinstance(args[0], list):
                                raise ValueError(f'"{args[0]}" can not be used as a variable name')
                            vars[args[0]] = {
                                "type": f"Array({args[1]})" if len(args) > 1 else "Array(String)",
                                **kwargs,
                            }
                    elif func in parameter_types:
                        # avoid variable names to be None
                        if len(args) and args[0] is not None:
                            # if this is a cast use the function name to get the type
                            if "default" not in kwargs:
                                default = kwargs.get("default", args[1] if len(args) > 1 else None)
                                kwargs["default"] = check_default_value(default)
                            try:
                                if isinstance(args[0], list):
                                    raise ValueError(f'"{args[0]}" can not be used as a variable name')
                                vars[args[0]] = {"type": func, **kwargs}
                                if "default" in kwargs:
                                    kwargs["default"] = check_default_value(kwargs["default"])
                            except TypeError as e:
                                logging.exception(f"pipe parsing problem {content} (node '{node_id}'):  {e}")
                except ValueError:
                    raise
                except Exception as e:
                    # if we find a problem parsing, let the parsing continue
                    logging.exception(f"pipe parsing problem {content} (node: '{node_id}'):  {e}")
            elif isinstance(node, ast.Name):
                # when parent node is a call it means it's managed by the Call workflow (see above)
                is_cast = (
                    isinstance(node.parent, ast.Call)
                    and isinstance(node.parent.func, ast.Name)
                    and node.parent.func.id in parameter_types
                )
                is_reserved_name = node.id in reserved_vars or node.id in function_list or node.id in _namespace
                if (not isinstance(node.parent, ast.Call) and not is_cast) and not is_reserved_name:
                    vars[node.id] = {"type": "String", "default": None}

        return vars

    def check_default_value(value):
        if isinstance(value, int):
            MAX_SAFE_INTEGER = 9007199254740991
            if value > MAX_SAFE_INTEGER:
                return str(value)
        return value

    def parse_content(content, retries=0):
        try:
            parsed = ast.parse(content)
            return parsed
        except Exception as e:
            if "AST constructor recursion depth mismatch" not in str(e):
                raise e
            retries += 1
            if retries > 3:
                raise e
            return parse_content(content, retries)

    parsed = parse_content(content)

    # calculate parents for each node for later checks
    for node in ast.walk(parsed):
        for child in ast.iter_child_nodes(node):
            child.parent = node
    vars = _w(parsed)

    return [dict(name=k, **v) for k, v in vars.items()]


def get_var_names_and_types(t, node_id=None):
    """
    >>> get_var_names_and_types(Template("SELECT * FROM filter_value WHERE description = {{Float32(with_value, 0.0)}}"))
    [{'name': 'with_value', 'type': 'Float32', 'default': 0.0}]
    >>> get_var_names_and_types(Template("SELECT * FROM filter_value WHERE description = {{Float32(with_value, -0.0)}}"))
    [{'name': 'with_value', 'type': 'Float32', 'default': -0.0}]
    >>> get_var_names_and_types(Template("SELECT * FROM filter_value WHERE description = {{Int32(with_value, 0)}}"))
    [{'name': 'with_value', 'type': 'Int32', 'default': 0}]
    >>> get_var_names_and_types(Template("SELECT * FROM filter_value WHERE description = {{Int32(with_value, -0)}}"))
    [{'name': 'with_value', 'type': 'Int32', 'default': 0}]
    >>> get_var_names_and_types(Template("SELECT * FROM filter_value WHERE description = {{Float32(with_value, -0.1)}}"))
    [{'name': 'with_value', 'type': 'Float32', 'default': -0.1}]
    >>> get_var_names_and_types(Template("SELECT * FROM filter_value WHERE description = {{Float32(with_value, 0.1)}}"))
    [{'name': 'with_value', 'type': 'Float32', 'default': 0.1}]
    >>> get_var_names_and_types(Template("SELECT * FROM filter_value WHERE description = {{String(d, 'test_1')}} AND value = {{Int8(v, 3)}}"))
    [{'name': 'd', 'type': 'String', 'default': 'test_1'}, {'name': 'v', 'type': 'Int8', 'default': 3}]
    >>> get_var_names_and_types(Template("select * from test {% if defined({{UInt64(number_variable)}}) %} where 1 {% end %}"))
    [{'name': 'number_variable', 'type': 'UInt64', 'default': None}]
    >>> get_var_names_and_types(Template("select * from test {% if defined(testing) and defined(testing2) %} where 1 {%end %}"))
    [{'name': 'testing', 'type': 'String', 'default': None, 'used_in': 'function_call'}, {'name': 'testing2', 'type': 'String', 'default': None, 'used_in': 'function_call'}]
    >>> get_var_names_and_types(Template("select * from test {% if defined({{UInt64(number_variable)}}) %} where 1 {% end %}"))
    [{'name': 'number_variable', 'type': 'UInt64', 'default': None}]
    >>> get_var_names_and_types(Template("select {{Array(cod_stock_source_type,'Int16', defined=False)}}"))
    [{'name': 'cod_stock_source_type', 'type': 'Array(Int16)', 'defined': False, 'default': None}]
    >>> get_var_names_and_types(Template("select {{Array(cod_stock_source_type, defined=False)}}"))
    [{'name': 'cod_stock_source_type', 'type': 'Array(String)', 'defined': False, 'default': None}]
    >>> get_var_names_and_types(Template("select {{cod_stock_source_type}}"))
    [{'name': 'cod_stock_source_type', 'type': 'String', 'default': None}]
    >>> get_var_names_and_types(Template("SELECT {{len([1] * 10**7)}}"))
    Traceback (most recent call last):
    ...
    tinybird.tornado_template.SecurityException: Invalid BinOp: Pow()
    >>> get_var_names_and_types(Template("select {{String(cod_stock_source_type, 'test')}}"))
    [{'name': 'cod_stock_source_type', 'type': 'String', 'default': 'test'}]
    >>> get_var_names_and_types(Template("select {{split_to_array(test)}}"))
    [{'name': 'test', 'type': 'String', 'default': None, 'used_in': 'function_call'}]
    >>> get_var_names_and_types(Template("select {{String(test + 'abcd', 'default_value')}}"))
    [{'name': 'test', 'type': 'String', 'default': None}]
    >>> get_var_names_and_types(Template("SELECT * FROM filter_value WHERE description = {{String(d, 'test_1', description='test', required=True)}} AND value = {{Int8(v, 3, format='number', example='1')}}"))
    [{'name': 'd', 'type': 'String', 'description': 'test', 'required': True, 'default': 'test_1'}, {'name': 'v', 'type': 'Int8', 'format': 'number', 'example': '1', 'default': 3}]
    >>> get_var_names_and_types(Template("SELECT * FROM filter_value WHERE description = {{String(d, default='test_1', description='test')}}"))
    [{'name': 'd', 'type': 'String', 'default': 'test_1', 'description': 'test'}]
    >>> get_var_names_and_types(Template("select {{Array(cod_stock_source_type, 'Int16', default='1', defined=False)}}"))
    [{'name': 'cod_stock_source_type', 'type': 'Array(Int16)', 'default': '1', 'defined': False}]
    >>> get_var_names_and_types(Template('select {{symbol(split_to_array(attr, "amount_net")[0] + "_intermediate" )}}'))
    [{'name': 'attr', 'type': 'String', 'default': None, 'used_in': 'function_call'}]
    >>> get_var_names_and_types(Template("SELECT * FROM filter_value WHERE description = {{Float32(with_value, 0.1)}} AND description = {{Float32(zero, 0)}} AND value = {{Float32(no_default)}}"))
    [{'name': 'with_value', 'type': 'Float32', 'default': 0.1}, {'name': 'zero', 'type': 'Float32', 'default': 0}, {'name': 'no_default', 'type': 'Float32', 'default': None}]
    >>> get_var_names_and_types(Template("SELECT * FROM filter_value WHERE description = {{Float32(with_value, -0.1)}} AND description = {{Float32(zero, 0)}} AND value = {{Float32(no_default)}}"))
    [{'name': 'with_value', 'type': 'Float32', 'default': -0.1}, {'name': 'zero', 'type': 'Float32', 'default': 0}, {'name': 'no_default', 'type': 'Float32', 'default': None}]
    >>> get_var_names_and_types(Template('''SELECT * FROM abcd WHERE hotel_id <> 0 {% if defined(date_from) %} AND script_created_at > {{DateTime(date_from, '2020-09-09 10:10:10', description="This is a description", required=True)(date_from, '2020-09-09', description="Filter script alert creation date", required=False)}} {% end %}'''))
    [{'name': 'date_from', 'type': 'DateTime', 'description': 'This is a description', 'required': True, 'default': '2020-09-09 10:10:10'}, {'name': 'date_from', 'type': 'DateTime', 'description': 'This is a description', 'required': True, 'default': '2020-09-09 10:10:10'}]
    >>> get_var_names_and_types(Template("SELECT * FROM filter_value WHERE symbol = {{Int128(symbol_id, 11111, description='Symbol Id', required=True)}} AND user = {{Int256(user_id, 3555, description='User Id')}}"))
    [{'name': 'symbol_id', 'type': 'Int128', 'description': 'Symbol Id', 'required': True, 'default': 11111}, {'name': 'user_id', 'type': 'Int256', 'description': 'User Id', 'default': 3555}]
    >>> get_var_names_and_types(Template("SELECT now() > {{DateTime64(timestamp, '2020-09-09 10:10:10.000')}}"))
    [{'name': 'timestamp', 'type': 'DateTime64', 'default': '2020-09-09 10:10:10.000'}]
    >>> get_var_names_and_types(Template("SELECT * FROM filter_value WHERE symbol = {{Int64(symbol_id, 9223372036854775807)}}"))
    [{'name': 'symbol_id', 'type': 'Int64', 'default': '9223372036854775807'}]
    """
    try:

        def _n(chunks, v):
            for x in chunks:
                if type(x).__name__ == "_ChunkList":
                    _n(x.chunks, v)
                elif type(x).__name__ == "_Expression":
                    var_data = get_var_data(x.expression, node_id=node_id)
                    if var_data:
                        v += var_data
                elif type(x).__name__ == "_ControlBlock":
                    buffer = StringIO()
                    writer = CodeWriter(buffer, t)
                    x.generate(writer)
                    var_data = get_var_data(buffer.getvalue(), node_id=node_id)
                    if var_data:
                        v += var_data
                    _n(x.body.chunks, v)

        var = []
        _n(t.file.body.chunks, var)
        return var
    except SecurityException as e:
        raise SQLTemplateException(e)


@lru_cache(maxsize=256)
def get_var_names_and_types_cached(t: Template):
    return get_var_names_and_types(t)


def wrap_vars(t, escape_arrays: bool = False):
    def _n(chunks, v):
        for x in chunks:
            if type(x).__name__ == "_ChunkList":
                _n(x.chunks, v)
            elif type(x).__name__ == "_Expression":
                x.expression = (
                    "expression_wrapper("
                    + x.expression
                    + ',"""'
                    + x.expression.replace('"', '\\"')
                    + '""",escape_arrays='
                    + str(escape_arrays)
                    + ")"
                )
            elif type(x).__name__ == "_ControlBlock":
                _n(x.body.chunks, v)

    var: List[Any] = []
    _n(t.file.body.chunks, var)
    t.code = t._generate_python(t.loader)
    try:
        t.compiled = compile(
            escape.to_unicode(t.code), "%s.generated.py" % t.name.replace(".", "_"), "exec", dont_inherit=True
        )
    except Exception:
        # formatted_code = _format_code(t.code).rstrip()
        # app_log.error("%s code:\n%s", t.name, formatted_code)
        raise

    return var


def get_used_tables_in_template(sql):
    """
    >>> get_used_tables_in_template("select * from {{table('test')}}")
    ['test']
    >>> get_used_tables_in_template("select * from {%if x %}{{table('test')}}{%else%}{{table('test2')}}{%end%}")
    ['test', 'test2']
    >>> get_used_tables_in_template("select * from {{table('my.test')}}")
    ['my.test']
    >>> get_used_tables_in_template("select * from {{table('my.test')}}, another_table")
    ['my.test']
    >>> get_used_tables_in_template("select * from another_table")
    []
    >>> get_used_tables_in_template("select * from {{table('my.test')}}, {{table('another.one')}}")
    ['my.test', 'another.one']
    """
    try:
        t = Template(sql)

        def _n(chunks, tables):
            for x in chunks:
                if type(x).__name__ == "_Expression":
                    c = compile(x.expression, "<string>", "exec", dont_inherit=True)
                    v = [x.lower() for x in c.co_names if x not in _namespace and x not in reserved_vars]
                    if "table" in v:

                        def _t(*args, **kwargs):
                            return str(args[0])

                        n = {"table": _t, "TABLE": _t}
                        e = "_tt_tmp = %s" % x.expression
                        exec_in(e, n)
                        tables += [n["_tt_tmp"]]
                elif type(x).__name__ == "_ControlBlock":
                    _n(x.body.chunks, tables)

        tables = []
        _n(t.file.body.chunks, tables)
        return tables
    except SecurityException as e:
        raise SQLTemplateException(e)


@lru_cache(maxsize=2**13)
def get_template_and_variables(sql: str, name: Optional[str], escape_arrays: bool = False):
    """
    Generates a Template and does all the processes necessary. As the object and template variables are cached
    it is important to NOT MODIFY THESE OBJECTS.
    Neither render_sql_template() or generate() modify them, so neither should you
    """
    variable_warnings = []

    try:
        t = Template(sql, name)
        template_variables = get_var_names(t)

        for variable in template_variables:
            if variable["name"] in DEFAULT_PARAM_NAMES:
                name = variable["name"]
                line = variable["line"]
                raise ValueError(f'"{name}" can not be used as a variable name, line {line}')
            if variable["name"] in RESERVED_PARAM_NAMES:
                variable_warnings.append(variable["name"])

        wrap_vars(t, escape_arrays=escape_arrays)

        return t, template_variables, variable_warnings
    except SecurityException as e:
        raise SQLTemplateException(e)


def preprocess_variables(variables: dict, template_variables_with_types: List[dict]):
    """
    >>> preprocess_variables({"test": '24'}, [{"name": "test", "type": "Int32", "default": None}])
    {}
    >>> preprocess_variables({"test": "1,2"}, [{"name": "test", "type": "Array(String)", "default": None}])
    {'test': ['1', '2']}
    >>> preprocess_variables({"test": ['1', '2']}, [{"name": "test", "type": "Array(String)", "default": None}])
    {'test': ['1', '2']}
    >>> preprocess_variables({"test": [1,2]}, [{"name": "test", "type": "Array(String)", "default": None}])
    {'test': ['1', '2']}
    >>> preprocess_variables({"test": "1,2,3"}, [{"name": "test", "type": "Array(Int32)", "default": None}])
    {'test': [1, 2, 3]}
    >>> preprocess_variables({"test": "1,2,msg"}, [{"name": "test", "type": "Array(Int32)", "default": None}])
    {}
    """
    processed_variables = {}
    for variable, value in variables.items():
        try:
            template_vars = [t_var for t_var in template_variables_with_types if t_var["name"] == variable] or None
            if template_vars is None or value is None:
                continue

            t_var = template_vars[0]
            var_type = t_var.get("type")
            if var_type is None:
                continue

            # For now, we only preprocess Array types
            match = re.match(r"Array\((\w+)\)", var_type)
            if match is None:
                continue

            array_type = match.group(1)
            array_fn = type_fns.get("Array")
            parsed_exp = array_fn(value, array_type)
            processed_variables[variable] = ast.literal_eval(parsed_exp)
        except Exception:
            continue

    return processed_variables


def format_SQLTemplateException_message(e: SQLTemplateException, vars_and_types: Optional[dict] = None):
    def join_with_different_last_separator(items, separator=", ", last_separator=" and "):
        if not items:
            return ""
        if len(items) == 1:
            return items[0]

        result = separator.join(items[:-1])
        return result + last_separator + items[-1]

    message = str(e)
    var_names = ""

    try:
        if REQUIRED_PARAM_NOT_DEFINED in message and vars_and_types:
            vars_with_default_none = []
            for item in vars_and_types:
                if (
                    item.get("default") is None
                    and item.get("used_in", None) is None
                    and item.get("name") not in vars_with_default_none
                ):
                    vars_with_default_none.append(item["name"])

            var_names = join_with_different_last_separator(vars_with_default_none)
    except Exception:
        pass

    if var_names:
        raise SQLTemplateException(
            f"{REQUIRED_PARAM_NOT_DEFINED}. Check the parameters {join_with_different_last_separator(vars_with_default_none)}. Please provide a value or set a default value in the pipe code.",
            e.documentation,
        )
    else:
        raise e


def render_sql_template(
    sql: str,
    variables: Optional[dict] = None,
    secrets: Optional[List[str]] = None,
    test_mode: bool = False,
    name: Optional[str] = None,
    local_variables: Optional[dict] = None,
) -> Tuple[str, TemplateExecutionResults, list]:
    """
    >>> render_sql_template("select * from table where f = {{Float32(foo)}}", { 'foo': -1 })
    ("select * from table where f = toFloat32('-1.0')", {}, [])
    >>> render_sql_template("{% if defined(open) %}ERROR{% else %}YEAH!{% end %}")
    ('YEAH!', {}, [])
    >>> render_sql_template("{% if defined(close) %}ERROR{% else %}YEAH!{% end %}")
    ('YEAH!', {}, [])
    >>> render_sql_template("{% if defined(input) %}ERROR{% else %}YEAH!{% end %}")
    ('YEAH!', {}, [])
    >>> render_sql_template("{% if defined(print) %}ERROR{% else %}YEAH!{% end %}")
    ('YEAH!', {}, [])
    >>> render_sql_template("select * from table where str = {{foo}}", { 'foo': 'test' })
    ("select * from table where str = 'test'", {}, [])
    >>> render_sql_template("select * from table where f = {{foo}}", { 'foo': 1.0 })
    ('select * from table where f = 1.0', {}, [])
    >>> render_sql_template("select {{Boolean(foo)}} from table", { 'foo': True })
    ('select 1 from table', {}, [])
    >>> render_sql_template("select {{Boolean(foo)}} from table", { 'foo': False })
    ('select 0 from table', {}, [])
    >>> render_sql_template("select * from table where f = {{Float32(foo)}}", { 'foo': 1 })
    ("select * from table where f = toFloat32('1.0')", {}, [])
    >>> render_sql_template("select * from table where f = {{foo}}", { 'foo': "';drop table users;" })
    ("select * from table where f = '\\\\';drop table users;'", {}, [])
    >>> render_sql_template("select * from {{symbol(foo)}}", { 'foo': 'table-name' })
    ('select * from `table-name`', {}, [])
    >>> render_sql_template("select * from {{symbol(foo)}}", { 'foo': '"table-name"' })
    ('select * from `table-name`', {}, [])
    >>> render_sql_template("select * from {{table(foo)}}", { 'foo': '"table-name"' })
    ('select * from table-name', {}, [])
    >>> render_sql_template("select * from {{Int32(foo)}}", { 'foo': 'non_int' })
    Traceback (most recent call last):
    ...
    tinybird.sql_template.SQLTemplateException: Template Syntax Error: Error validating 'non_int' to type Int32
    >>> render_sql_template("select * from table where f = {{Float32(foo)}}", test_mode=True)
    ("select * from table where f = toFloat32('0.0')", {}, [])
    >>> render_sql_template("SELECT * FROM query_log__dev where a = {{test}}", test_mode=True)
    ("SELECT * FROM query_log__dev where a = '__no_value__'", {}, [])
    >>> render_sql_template("SELECT {{test}}", {'token':'testing'})
    Traceback (most recent call last):
    ...
    tinybird.sql_template.SQLTemplateException: Template Syntax Error: expression "test" evaluated to null
    >>> render_sql_template("SELECT {{testisasuperlongthingandwedontwanttoreturnthefullthing}}", {'token':'testing'})
    Traceback (most recent call last):
    ...
    tinybird.sql_template.SQLTemplateException: Template Syntax Error: expression "testisasuperlongthin..." evaluated to null
    >>> render_sql_template("SELECT {{ Array(embedding, 'Float32') }}", {'token':'testing', 'embedding': '1,2,3,4, null'})
    Traceback (most recent call last):
    ...
    tinybird.sql_template.SQLTemplateException: Template Syntax Error: Error validating 1,2,3,4, null[4]( null) to type Float32
    >>> render_sql_template('{% if test %}SELECT 1{% else %} select 2 {% end %}')
    (' select 2 ', {}, [])
    >>> render_sql_template('{% if Int32(test, 1) %}SELECT 1{% else %} select 2 {% end %}')
    ('SELECT 1', {}, [])
    >>> render_sql_template('{% for v in test %}SELECT {{v}} {% end %}',test_mode=True)
    ("SELECT '__no_value__' SELECT '__no_value__' SELECT '__no_value__' ", {}, [])
    >>> render_sql_template("select {{Int32(foo, 1)}}", test_mode=True)
    ("select toInt32('1')", {}, [])
    >>> render_sql_template("SELECT count() c FROM test_table where a > {{Float32(myvar)}} {% if defined(my_condition) %} and c = Int32({{my_condition}}){% end %}", {'myvar': 1.0})
    ("SELECT count() c FROM test_table where a > toFloat32('1.0') ", {}, [])
    >>> render_sql_template("SELECT count() c FROM where {{sql_and(a=a, b=b)}}", {'a': '1', 'b': '2'})
    ("SELECT count() c FROM where a = '1' and b = '2'", {}, [])
    >>> render_sql_template("SELECT count() c FROM where {{sql_and(a=a, b=b)}}", {'b': '2'})
    ("SELECT count() c FROM where b = '2'", {}, [])
    >>> render_sql_template("SELECT count() c FROM where {{sql_and(a=Int(a, defined=False), b=Int(b, defined=False))}}", {'b': '2'})
    ('SELECT count() c FROM where b = 2', {}, [])
    >>> render_sql_template("SELECT count() c FROM where {{sql_and(a__in=Array(a), b=b)}}", {'a': 'a,b,c','b': '2'})
    ("SELECT count() c FROM where a in ['a','b','c'] and b = '2'", {}, [])
    >>> render_sql_template("SELECT count() c FROM where {{sql_and(a__not_in=Array(a), b=b)}}", {'a': 'a,b,c','b': '2'})
    ("SELECT count() c FROM where a not in ['a','b','c'] and b = '2'", {}, [])
    >>> render_sql_template("SELECT c FROM where a > {{Date(start)}}", test_mode=True)
    ("SELECT c FROM where a > '2019-01-01'", {}, [])
    >>> render_sql_template("SELECT c FROM where a > {{DateTime(start)}}", test_mode=True)
    ("SELECT c FROM where a > '2019-01-01 00:00:00'", {}, [])
    >>> render_sql_template("SELECT c FROM where a > {{DateTime(start)}}", {'start': '2018-09-07 23:55:00'})
    ("SELECT c FROM where a > '2018-09-07 23:55:00'", {}, [])
    >>> render_sql_template('SELECT * FROM tracker {% if defined(start) %} {{DateTime(start)}} and {{DateTime(end)}} {% end %}', {'start': '2019-08-01 00:00:00', 'end': '2019-08-02 00:00:00'})
    ("SELECT * FROM tracker  '2019-08-01 00:00:00' and '2019-08-02 00:00:00' ", {}, [])
    >>> render_sql_template('SELECT * from test limit {{Int(limit)}}', test_mode=True)
    ('SELECT * from test limit 0', {}, [])
    >>> render_sql_template('SELECT {{symbol(attr)}} from test', test_mode=True)
    ('SELECT `placeholder` from test', {}, [])
    >>> render_sql_template('SELECT {{Array(foo)}}', {'foo': 'a,b,c,d'})
    ("SELECT ['a','b','c','d']", {}, [])
    >>> render_sql_template("SELECT {{Array(foo, 'Int32')}}", {'foo': '1,2,3,4'})
    ('SELECT [1,2,3,4]', {}, [])
    >>> render_sql_template("SELECT {{Array(foo, 'Int32')}}", test_mode=True)
    ('SELECT [0,0]', {}, [])
    >>> render_sql_template("SELECT {{Array(foo)}}", test_mode=True)
    ("SELECT ['__no_value__0','__no_value__1']", {}, [])
    >>> render_sql_template("{{max_threads(2)}} SELECT 1")
    ('-- max_threads 2\\n SELECT 1', {'max_threads': 2}, [])
    >>> render_sql_template("SELECT {{String(foo)}}", test_mode=True)
    ("SELECT '__no_value__'", {}, [])
    >>> render_sql_template("SELECT {{String(foo, 'test')}}", test_mode=True)
    ("SELECT 'test'", {}, [])
    >>> render_sql_template("SELECT {{String(foo, 'test')}}", {'foo': 'tt'})
    ("SELECT 'tt'", {}, [])
    >>> render_sql_template("SELECT {{String(format, 'test')}}", {'format': 'tt'})
    Traceback (most recent call last):
    ...
    ValueError: "format" can not be used as a variable name, line 1
    >>> render_sql_template("SELECT {{format}}", {'format': 'tt'})
    Traceback (most recent call last):
    ...
    ValueError: "format" can not be used as a variable name, line 1
    >>> render_sql_template("SELECT {{String(q, 'test')}}", {'q': 'tt'})
    Traceback (most recent call last):
    ...
    ValueError: "q" can not be used as a variable name, line 1
    >>> render_sql_template("SELECT {{column(agg)}}", {})
    Traceback (most recent call last):
    ...
    tinybird.sql_template.SQLTemplateException: Template Syntax Error: Missing column() default value, use `column(column_name, 'default_column_name')`
    >>> render_sql_template("SELECT {{column(agg)}}", {'agg': 'foo'})
    ('SELECT `foo`', {}, [])
    >>> render_sql_template("SELECT {{column(agg)}}", {'agg': '"foo"'})
    ('SELECT `foo`', {}, [])
    >>> render_sql_template('{% if not defined(test) %}error("This is an error"){% end %}', {})
    ('error("This is an error")', {}, [])
    >>> render_sql_template('{% if not defined(test) %}custom_error({error: "This is an error"}){% end %}', {})
    ('custom_error({error: "This is an error"})', {}, [])
    >>> render_sql_template("SELECT {{String(foo + 'abcd')}}", test_mode=True)
    ("SELECT '__no_value__'", {}, [])
    >>> render_sql_template("SELECT {{columns(agg)}}", {})
    Traceback (most recent call last):
    ...
    tinybird.sql_template.SQLTemplateException: Template Syntax Error: Missing columns() default value, use `columns(column_names, 'default_column_name')`
    >>> render_sql_template("SELECT {{columns(agg, 'a,b,c')}} FROM table", {})
    ('SELECT `a`,`b`,`c` FROM table', {}, [])
    >>> render_sql_template("SELECT {{columns(agg, 'a,b,c')}} FROM table", {'agg': 'foo'})
    ('SELECT `foo` FROM table', {}, [])
    >>> render_sql_template("SELECT {{columns('a,b,c')}} FROM table", {})
    ('SELECT `a`,`b`,`c` FROM table', {}, [])
    >>> render_sql_template("% {% if whatever(passenger_count) %}{% end %}", test_mode=True)
    Traceback (most recent call last):
    ...
    tinybird.sql_template.SQLTemplateException: Template Syntax Error: 'whatever' is not a valid function, line 1
    >>> render_sql_template("% {% if defined((passenger_count) %}{% end %}", test_mode=True)
    Traceback (most recent call last):
    ...
    SyntaxError: invalid syntax
    >>> render_sql_template("SELECT * FROM dim_fecha_evento where foo like {{sql_unescape(String(pepe), '%')}}", {"pepe": 'raul_el_bueno_is_the_best_%'})
    ("SELECT * FROM dim_fecha_evento where foo like 'raul_el_bueno_is_the_best_%'", {}, [])
    >>> render_sql_template("SELECT * FROM table WHERE field={{String(field_filter)}}", {"field_filter": 'action."test run"'})
    ('SELECT * FROM table WHERE field=\\'action.\\\\"test run\\\\"\\'', {}, [])
    >>> render_sql_template("SELECT {{Int128(foo)}} as x, {{Int128(bar)}} as y", {'foo': -170141183460469231731687303715884105728, 'bar': 170141183460469231731687303715884105727})
    ("SELECT toInt128('-170141183460469231731687303715884105728') as x, toInt128('170141183460469231731687303715884105727') as y", {}, [])
    >>> render_sql_template("SELECT {{Int256(foo)}} as x, {{Int256(bar)}} as y", {'foo': -57896044618658097711785492504343953926634992332820282019728792003956564819968, 'bar': 57896044618658097711785492504343953926634992332820282019728792003956564819967})
    ("SELECT toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968') as x, toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967') as y", {}, [])
    >>> render_sql_template('% SELECT * FROM {% import os %}{{ os.popen("whoami").read() }}')
    Traceback (most recent call last):
    ...
    tinybird.tornado_template.ParseError: import is forbidden at <string>:1
    >>> render_sql_template('% SELECT * FROM {% import os %}{{ os.popen("ls").read() }}')
    Traceback (most recent call last):
    ...
    tinybird.tornado_template.ParseError: import is forbidden at <string>:1
    >>> render_sql_template('% SELECT * FROM {% import os %}{{ os.popen("cat etc/passwd").read() }}')
    Traceback (most recent call last):
    ...
    tinybird.tornado_template.ParseError: import is forbidden at <string>:1
    >>> render_sql_template('% SELECT * FROM {% from os import popen %}{{ popen("cat etc/passwd").read() }}')
    Traceback (most recent call last):
    ...
    tinybird.tornado_template.ParseError: import is forbidden at <string>:1
    >>> render_sql_template('% SELECT {{len([1] * 10**7)}}')
    Traceback (most recent call last):
    ...
    tinybird.sql_template.SQLTemplateException: Template Syntax Error: Invalid BinOp: Pow()
    >>> render_sql_template("% SELECT {{Array(click_selector, 'String', 'pre,pro')}}")
    ("% SELECT ['pre','pro']", {}, [])
    >>> render_sql_template("% SELECT {{Array(click_selector, 'String', 'pre,pro')}}", {'click_selector': 'hi,hello'})
    ("% SELECT ['hi','hello']", {}, [])
    >>> render_sql_template("% SELECT now() > {{DateTime64(variable, '2020-09-09 10:10:10.000')}}", {})
    ("% SELECT now() > '2020-09-09 10:10:10.000'", {}, [])
    >>> render_sql_template("% SELECT {% if defined(x) %} x, 1", {})
    Traceback (most recent call last):
    ...
    tinybird.tornado_template.UnClosedIfError: Missing {% end %} block for if at line 1
    >>> render_sql_template("% SELECT * FROM employees WHERE 0 {% for kv in JSON(payload) %} OR department = {{kv['dp']}} {% end %}")
    ('% SELECT * FROM employees WHERE 0 ', {}, [])
    >>> render_sql_template("% SELECT * FROM employees WHERE 0 {% for kv in JSON(payload, '[{\\"dp\\":\\"Sales\\"}]') %} OR department = {{kv['dp']}} {% end %}")
    ("% SELECT * FROM employees WHERE 0  OR department = 'Sales' ", {}, [])
    >>> render_sql_template("% SELECT * FROM employees WHERE 0 {% for kv in JSON(payload) %} OR department = {{kv['dp']}} {% end %}", { 'payload': '[{"dp":"Design"},{"dp":"Marketing"}]'})
    ("% SELECT * FROM employees WHERE 0  OR department = 'Design'  OR department = 'Marketing' ", {}, [])
    >>> render_sql_template("% {% for kv in JSON(payload) %} department = {{kv['dp']}} {% end %}", test_mode=True)
    Traceback (most recent call last):
    ...
    tinybird.sql_template.SQLTemplateException: Template Syntax Error: Error parsing JSON: '__no_value__' - Expecting value: line 1 column 1 (char 0)
    >>> render_sql_template("% {% for kv in JSON(payload, '') %} department = {{kv['dp']}} {% end %}")
    Traceback (most recent call last):
    ...
    tinybird.sql_template.SQLTemplateException: Template Syntax Error: Error parsing JSON: '' - Expecting value: line 1 column 1 (char 0)
    >>> render_sql_template("% {% if defined(test) %}{% set _groupByCSV = ','.join(test) %} SELECT test as aa, {{Array(test, 'String')}} as test, {{_groupByCSV}} as a {% end %}", {"test": "1,2"})
    ("%  SELECT test as aa, ['1','2'] as test, '1,2' as a ", {}, [])
    >>> render_sql_template("% {% if defined(test) %}{% set _groupByCSV = ','.join(test) %} SELECT test as aa, {{Array(test, 'String')}} as test, {{_groupByCSV}} as a {% end %}", {"test": ["1","2"]})
    ("%  SELECT test as aa, ['1','2'] as test, '1,2' as a ", {}, [])
    >>> render_sql_template("% {% if defined(test) %}{% set _total = sum(test) %} SELECT test as aa, {{Array(test, 'Int32')}} as test, {{_total}} as a {% end %}", {"test": "1,2"})
    ('%  SELECT test as aa, [1,2] as test, 3 as a ', {}, [])
    >>> render_sql_template("% {% if defined(test) %}{% set _groupByCSV = ','.join(test) %} SELECT test as aa, {{Array(test, 'String')}} as test, {{_groupByCSV}} as a {% end %}", {"test": ["1","2"]})
    ("%  SELECT test as aa, ['1','2'] as test, '1,2' as a ", {}, [])
    >>> render_sql_template("% SELECT {% if defined(x) %} x, 1")
    Traceback (most recent call last):
    ...
    tinybird.tornado_template.UnClosedIfError: Missing {% end %} block for if at line 1
    >>> render_sql_template("select * from table where str = {{pipeline}}", { 'pipeline': 'test' })
    ("select * from table where str = 'test'", {}, ['pipeline'])
    >>> render_sql_template("select * from table where str = {{tb_secret('test')}}", secrets = [ 'tb_secret_test' ])
    ('select * from table where str = {test: String}', {}, [])
    >>> render_sql_template("select * from table where str = {{tb_var('test')}}", secrets = [ 'tb_secret_test' ])
    ('select * from table where str = {test: String}', {}, [])
    >>> render_sql_template("select * from table where str = {{tb_secret('test')}}", variables = { 'test': '1234' })
    Traceback (most recent call last):
    ...
    tinybird.sql_template.SQLTemplateException: Template Syntax Error: Cannot access secret 'test'. Check the secret exists in the Workspace and the token has the required scope.
    >>> render_sql_template("select * from table where str = {{tb_secret('test')}}", test_mode=True)
    ('select * from table where str = {test: String}', {}, [])
    >>> render_sql_template("select * from table where str = {{tb_secret('test')}}", secrets = [ 'tb_secret_test' ], test_mode=True)
    ('select * from table where str = {test: String}', {}, [])
    >>> render_sql_template("select * from table where str = {{tb_secret('test')}}", secrets = [ 'tb_secret_test2' ])
    Traceback (most recent call last):
    ...
    tinybird.sql_template.SQLTemplateException: Template Syntax Error: Cannot access secret 'test'. Check the secret exists in the Workspace and the token has the required scope.
    >>> render_sql_template("select * from table where str = {{String(test)}} and category = {{String(category, 'shirts')}} and color = {{ Int32(color)}}", test_mode=False)
    Traceback (most recent call last):
    ...
    tinybird.sql_template.SQLTemplateException: Template Syntax Error: Required parameter is not defined. Check the parameters test and color. Please provide a value or set a default value in the pipe code.
    >>> render_sql_template("select columns(cols, 'salary') from table where str = {{String(test)}}", test_mode=False)
    Traceback (most recent call last):
    ...
    tinybird.sql_template.SQLTemplateException: Template Syntax Error: Required parameter is not defined. Check the parameters test. Please provide a value or set a default value in the pipe code.
    """
    escape_split_to_array = ff_split_to_array_escape.get(False)
    bypass_preprocess_variables = ff_preprocess_parameters_circuit_breaker.get(False)

    t, template_variables, variable_warnings = get_template_and_variables(
        sql, name, escape_arrays=escape_split_to_array
    )
    template_variables_with_types = get_var_names_and_types_cached(t)

    if not bypass_preprocess_variables and variables is not None:
        processed_variables = preprocess_variables(variables, template_variables_with_types)
        variables.update(processed_variables)

    if test_mode:

        def dummy(*args, **kwargs):
            return Comment("error launched")

        v: dict = {x["name"]: Placeholder(x["name"], x["line"]) for x in template_variables}
        is_tb_secret = any([s for s in template_variables if s["name"] == "tb_secret" or s["name"] == "tb_var"])

        if variables:
            v.update(variables)

        if secrets:
            v.update({"tb_secrets": secrets})

        if is_tb_secret:
            v.update({TB_SECRET_IN_TEST_MODE: None})

        v.update(type_fns_check)
        v.update(
            {
                # disable error throws on check
                "error": dummy,
                "custom_error": dummy,
            }
        )

        if local_variables:
            v.update(local_variables)

    else:
        v = {x["name"]: None for x in template_variables}
        if variables:
            v.update(variables)

        if secrets:
            v.update({"tb_secrets": secrets})

        v.update(type_fns)

        if local_variables:
            v.update(local_variables)

    try:
        sql, template_execution_results = generate(t, **v)
        try:
            if TB_SECRET_IN_TEST_MODE in template_execution_results:
                del template_execution_results[TB_SECRET_IN_TEST_MODE]
        except Exception:
            pass
        return sql, template_execution_results, variable_warnings
    except NameError as e:
        raise SQLTemplateException(e, documentation="/cli/advanced-templates.html#defined")
    except SQLTemplateException as e:
        format_SQLTemplateException_message(e, vars_and_types=template_variables_with_types)
        raise
    except Exception as e:
        # errors might vary here, we need to support as much as possible
        # https://gitlab.com/tinybird/analytics/-/issues/943
        if "length" in v and not v["length"]:
            raise SQLTemplateException("length cannot be used as a variable name or as a function inside of a template")
        elif "missing 1 required positional argument" in str(e):
            raise SQLTemplateException(
                "one of the transform type functions is missing an argument",
                documentation="/cli/advanced-templates.html#transform-types-functions",
            )
        elif "not callable" in str(e) or "unhashable type" in str(e):
            raise SQLTemplateException(
                "wrong syntax, you might be using a not valid function inside a control block",
                documentation="/cli/advanced-templates.html",
            )
        raise e


def extract_variables_from_sql(sql: str, params: List[Dict[str, Any]]) -> Dict[str, Any]:
    sql = sql[1:] if sql[0] == "%" else sql
    defaults = {}
    mock_data = {}
    try:
        for param in params:
            mock_data[param["name"]] = "__NO__VALUE__DEFINED__"
        # Initialize a dictionary to track variables
        variable_tracker = {}

        # Wrapper function to capture variable assignments
        def capture_variable(name, value):
            variable_tracker[name] = value
            return value

        # Modify the template by adding capture hooks
        tracked_template_string = sql
        for var_name in mock_data.keys():
            tracked_template_string += f"{{% set __ = capture_variable('{var_name}', {var_name}) %}}"

        # Define the modified template with tracking
        template = Template(tracked_template_string)
        type_fns = get_transform_types()
        template.generate(**mock_data, **type_fns, capture_variable=capture_variable)
        for var_name, value in variable_tracker.items():
            if value != "__NO__VALUE__DEFINED__":
                defaults[var_name] = value
    except Exception as e:
        logging.error(f"Error extracting variables from sql: {e}")
        return {}

    return defaults
