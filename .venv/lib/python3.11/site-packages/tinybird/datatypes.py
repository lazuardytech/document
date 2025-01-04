import ast
import decimal
import re
from decimal import Decimal

datetime64_patterns = [
    r"\d\d\d\d.\d\d.\d\d(T|\s)\d\d:\d\d:\d\d.\d\d\d",
    r"\d\d.\d\d.\d\d\d\d.\d{1,2}:\d{1,2}:\d{1,2}.\d{1,3}",
]

datetime_patterns = [r"\d\d\d\d.\d\d.\d\d(T|\s)\d\d:\d\d:\d\d", r"\d\d.\d\d.\d\d\d\d.\d{1,2}:\d{1,2}:\d{1,2}"]

int_8_max = 2**7
int16_max = 2**15
int32_max = 2**31
int64_max = 2**63
int128_max = 2**127
int256_max = 2**255
uint_8_max = 2**8
uint16_max = 2**16
uint32_max = 2**32
uint64_max = 2**64
uint128_max = 2**128
uint256_max = 2**256
intx_re = r"^[+-]?\d+$"
uintx_re = r"^\d+$"
float32_max = 2**23  # 23 bits is the fractional part of float 32 ieee754
float64_max = 2**52  # 51 bits is the fractional part of float 64 ieee754

datetime64_type_pattern = r"^DateTime64(\([1-9](, ?'.+')?\))?$"
datetime_type_pattern = r"^DateTime(\(('.+')?)?\)?$"

# List from https://github.com/tinybirdco/ClickHousePrivate/blob/153473d9c1c871974688a1d72dcff7a13fc2076c/src/DataTypes/Serializations/SerializationBool.cpp#L216
bool_allowed_values = {
    "true",
    "false",
    "True",
    "False",
    "T",
    "F",
    "Y",
    "N",
    "Yes",
    "No",
    "On",
    "Off",
    "Enable",
    "Disable",
    "Enabled",
    "Disabled",
    "1",
    "0",
}


def is_type_datetime64(type_to_check):
    """
    >>> is_type_datetime64('DateTime64')
    True
    >>> is_type_datetime64('DateTime64(1)')
    True
    >>> is_type_datetime64('DateTime64(3)')
    True
    >>> is_type_datetime64("DateTime64(3,'Madrid')")
    True
    >>> is_type_datetime64("DateTime64(3, 'Madrid/Moscow')")
    True
    >>> is_type_datetime64("DateTime64()")
    False
    >>> is_type_datetime64("datetime64")
    False
    """
    return re.match(datetime64_type_pattern, type_to_check) is not None


def is_type_datetime(type_to_check):
    """
    >>> is_type_datetime('DateTime')
    True
    >>> is_type_datetime('DateTime()')
    True
    >>> is_type_datetime("DateTime('Madrid')")
    True
    >>> is_type_datetime("DateTime(3)")
    False
    >>> is_type_datetime("datetime")
    False
    """
    return re.match(datetime_type_pattern, type_to_check) is not None


def string_test(x):
    return True


def date_test(x):
    return re.match(r"\d\d\d\d-\d\d-\d\d$", x)


def datetime64_test(x):
    return any([re.match(p, x) for p in datetime64_patterns])


def datetime_test(x):
    return any([re.match(p, x) for p in datetime_patterns])


def int_8_test(x):
    return re.match(intx_re, x) and -int_8_max <= int(x) < int_8_max


def int16_test(x):
    return re.match(intx_re, x) and -int16_max <= int(x) < int16_max


def int32_test(x):
    return re.match(intx_re, x) and -int32_max <= int(x) < int32_max


def int64_test(x):
    return re.match(intx_re, x) and -int64_max <= int(x) < int64_max


def int128_test(x):
    return re.match(intx_re, x) and -int128_max <= int(x) < int128_max


def int256_test(x):
    return re.match(intx_re, x) and -int256_max <= int(x) < int256_max


def uint_8_test(x):
    return re.match(uintx_re, x) and 0 <= int(x) < uint_8_max


def uint16_test(x):
    return re.match(uintx_re, x) and 0 <= int(x) < uint16_max


def uint32_test(x):
    return re.match(uintx_re, x) and 0 <= int(x) < uint32_max


def uint64_test(x):
    return re.match(uintx_re, x) and 0 <= int(x) < uint64_max


def uint128_test(x):
    return re.match(intx_re, x) and 0 <= int(x) < uint128_max


def uint256_test(x):
    return re.match(intx_re, x) and 0 <= int(x) < uint256_max


def float_test(x):
    return "_" not in x and type_test(x, float)


def float32_test(x):
    return "_" not in x and type_test(x, float) and -float32_max <= float(x) < float32_max


def float64_test(x):
    return "_" not in x and type_test(x, float) and -float64_max < float(x) < float64_max


def bool_test(x):
    return x in bool_allowed_values


def test_numeric_testers(fn, n):
    """
    >>> test_numeric_testers(int32_test, (2**31)-1)
    True
    >>> test_numeric_testers(int32_test, -(2**31))
    True
    >>> test_numeric_testers(int32_test, -(2**31)-1)
    False
    >>> test_numeric_testers(int32_test, 2**31)
    False

    >>> test_numeric_testers(int64_test, (2**63)-1)
    True
    >>> test_numeric_testers(int64_test, -(2**63))
    True
    >>> test_numeric_testers(int64_test, -(2**63)-1)
    False
    >>> test_numeric_testers(int64_test, 2**63)
    False
    """
    return fn(str(n))


def array_test(_type_test):
    """
    >>> array_test(str)("['blabla']")
    True
    >>> array_test(str)('["blabla"]')
    True
    >>> array_test(str)('["blabla","bloblo"]')
    True
    >>> array_test(str)('["blabla, bloblo"]')
    True
    >>> array_test(str)("[ W ]")
    False
    >>> array_test(int)("[1]")
    True
    >>> array_test(int)('[1]')
    True
    >>> array_test(int)('[1,2]')
    True
    >>> array_test(float)("[1.2]")
    True
    >>> array_test(float)('[1.2]')
    True
    >>> array_test(float)('[1.2,2.1]')
    True
    >>> array_test(float)('["1.2","2.1"]')
    False
    """

    def _test(x):
        if x[0] != "[":
            return False
        try:
            k = ast.literal_eval(x)
        except Exception:
            return False
        if isinstance(k, list):
            return all(isinstance(x, _type_test) for x in k)
        return False

    return _test


numbers_types = (
    "Int8",
    "UInt8",
    "Int16",
    "UInt16",
    "UInt32",
    "Int32",
    "Int64",
    "UInt64",
    "Int128",
    "UInt128",
    "Int256",
    "UInt256",
    "Float32",
    "Float64",
)

# Use guessers for discovering types
# I.e., when you have to take into consideration things like float precision
guessers = {
    "DateTime64": datetime64_test,
    "DateTime": datetime_test,
    "Date": date_test,
    "Int8": int_8_test,
    "UInt8": uint_8_test,
    "Int16": int16_test,
    "UInt16": uint16_test,
    "Int32": int32_test,
    "UInt32": uint32_test,
    "Int64": int64_test,
    "UInt64": uint64_test,
    "Float32": float32_test,
    "Float64": float64_test,
    "Array(Int32)": array_test(int),
    "Array(Float32)": array_test(float),
    "Array(String)": array_test(str),
}

# Use testers validating a value against a type
# I.e., you already know the type and you need to check if a value fits there
testers = {
    "DateTime64": datetime64_test,
    "DateTime": datetime_test,
    "Date": date_test,
    "Int8": int_8_test,
    "UInt8": uint_8_test,
    "Int16": int16_test,
    "UInt16": uint16_test,
    "Int32": int32_test,
    "UInt32": uint32_test,
    "Int64": int64_test,
    "UInt64": uint64_test,
    "Int128": int128_test,
    "UInt128": uint128_test,
    "Int256": int256_test,
    "UInt256": uint256_test,
    "Float32": float_test,
    "Float64": float_test,
    "Bool": bool_test,
    "Array(Int32)": array_test(int),
    "Array(Float32)": array_test(float),
    "Array(String)": array_test(str),
}


# Search for `canBeInsideNullable` under CH code and see which ones are true.
nullable_types = [
    "Date",
    "Date32",
    "DateTime",
    "DateTime32",
    "DateTime64",
    "Decimal",
    "Decimal128",
    "Decimal256",
    "Decimal32",
    "Decimal64",
    "Enum",
    "Enum16",
    "Enum8",
    "FixedString",
    "Float32",
    "Float64",
    "IPv4",
    "IPv6",
    "Int128",
    "Int16",
    "Int256",
    "Int32",
    "Int64",
    "Int8",
    "MultiPolygon",
    "Point",
    "Polygon",
    "Ring",
    "String",
    "UInt128",
    "UInt16",
    "UInt256",
    "UInt32",
    "UInt64",
    "UInt8",
    "UUID",
]


def type_test(i, t):
    try:
        t(i)
        return True
    except Exception:
        return False


def parse_decimal_type(typ):
    """
    >>> parse_decimal_type("decimal")

    >>> parse_decimal_type('Decimal')
    (64, 10, 0)
    >>> parse_decimal_type('Decimal()')

    >>> parse_decimal_type('Decimal(2)')
    (32, 2, 0)
    >>> parse_decimal_type('Decimal(5,2)')
    (32, 5, 2)
    >>> parse_decimal_type('Decimal( 9 , 2 )')
    (32, 9, 2)
    >>> parse_decimal_type('Decimal(10,2)')
    (64, 10, 2)
    >>> parse_decimal_type('Decimal(19,2)')
    (128, 19, 2)
    >>> parse_decimal_type('Decimal(39,2)')
    (256, 39, 2)
    >>> parse_decimal_type('Decimal32(9)')
    (32, 9, 9)
    >>> parse_decimal_type('Decimal32(10)')

    >>> parse_decimal_type('Decimal(10,10)')
    (64, 10, 10)
    >>> parse_decimal_type('Decimal(10,11)')

    >>> parse_decimal_type('Decimal32(5, 2)')

    >>> parse_decimal_type('Decimal64(2)')
    (64, 18, 2)
    >>> parse_decimal_type('Decimal128(2)')
    (128, 38, 2)
    >>> parse_decimal_type('Decimal256(2)')
    (256, 76, 2)
    """
    # Obtained from https://clickhouse.com/docs/en/sql-reference/data-types/decimal
    max_digits_by_bit_width = {32: 9, 64: 18, 128: 38, 256: 76}

    # Check if type is Decimal, Decimal(P), or Decimal(P, S) with whitespace support
    # Regex contains two capturing groups. First capturing P. Second capturing S.
    if m := re.match(r"^Decimal\s*(?:\(\s*(\d+)(?:\s*,\s*(\d+))?\s*\))?$", typ):
        p, s = int(m.group(1) or 10), int(m.group(2) or 0)
        b = min(bit_width for bit_width, max_digits in max_digits_by_bit_width.items() if max_digits >= p)
        if p < s or b is None:
            return None
    # Check if type is Decimal32(S), Decimal64(S), Decimal128(S), or Decimal256(S) with whitespace support.
    # Regex contains 2 capturing groups. First capturing 32, 64, 128 or 256. Second capturing S.
    elif m := re.match(r"^Decimal(32|64|128|256)\s*\(\s*(\d+)\s*\)$", typ):
        b, s = int(m.group(1)), int(m.group(2))
        p = max_digits_by_bit_width[int(b)]
        if p < s:
            return None
    else:
        return None
    return b, p, s


def is_type_decimal(type_to_check):
    """
    >>> is_type_decimal('Decimal')
    True
    >>> is_type_decimal("Decimal(10, 2)")
    True
    >>> is_type_decimal("decimal")
    False
    """
    return parse_decimal_type(type_to_check) is not None


def get_decimal_limits(p, s):
    """
    >>> get_decimal_limits(1, 0)
    (Decimal('-9'), Decimal('9'))
    >>> get_decimal_limits(5, 5)
    (Decimal('-0.99999'), Decimal('0.99999'))
    >>> get_decimal_limits(76, 38)
    (Decimal('-99999999999999999999999999999999999999.99999999999999999999999999999999999999'), Decimal('99999999999999999999999999999999999999.99999999999999999999999999999999999999'))
    """
    with decimal.localcontext(prec=p + 2):
        max_value = Decimal((10**p) - 1) / (10**s)
        min_value = -max_value
    return min_value, max_value
