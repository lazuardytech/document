import logging
from dataclasses import dataclass
from functools import partial
from types import MethodType
from typing import Any, Dict, List, Optional

from sqlfmt import actions, api
from sqlfmt.analyzer import Analyzer, Rule, group
from sqlfmt.comment import Comment
from sqlfmt.dialect import ClickHouse
from sqlfmt.exception import SqlfmtBracketError
from sqlfmt.jinjafmt import JinjaFormatter, JinjaTag
from sqlfmt.line import Line
from sqlfmt.mode import Mode
from sqlfmt.node import Node, get_previous_token
from sqlfmt.node_manager import NodeManager
from sqlfmt.token import Token, TokenType

# This class extends and monkey patches https://github.com/tconbeer/sqlfm
INDENT = " " * 4
# This is the default value in tb fmt --diff, let's use the same
DEFAULT_FMT_LINE_LENGTH = 100


@dataclass
class TBLine(Line):
    @property
    def prefix(self) -> str:
        """
        Returns the whitespace to be printed at the start of this Line for
        proper indentation.

        Tinybird => This is overriden from the base Line because we want SQL inside a template to be indented
        https://github.com/tconbeer/sqlfmt/blob/c11775b92d8a45f0e91d871b81a88a894d620bec/src/sqlfmt/line.py#L92
        """
        prefix = INDENT * (self.depth[0] + self.depth[1])
        return prefix


def from_nodes(
    cls,
    previous_node: Optional[Node],
    nodes: List[Node],
    comments: List[Comment],
) -> "Line":
    """
    Creates and returns a new line from a list of Nodes. Useful for line
    splitting and merging.

    Tinybird => Monkey patched to use `TBLine` and our own indentation logic.
    """
    if nodes:
        line = TBLine(
            previous_node=previous_node,
            nodes=nodes,
            comments=comments,
            formatting_disabled=nodes[0].formatting_disabled or nodes[-1].formatting_disabled,
        )
    else:
        line = TBLine(
            previous_node=previous_node,
            nodes=nodes,
            comments=comments,
            formatting_disabled=previous_node.formatting_disabled if previous_node else False,
        )

    return line


def _format_jinja_node(self, node: Node, max_length: int) -> bool:
    """
    Format a single jinja tag. No-ops for nodes that
    are not jinja. Returns True if the node was blackened
    """
    if node.is_jinja:
        formatter = JinjaFormatter(TBMode())
        tag = JinjaTag.from_string(node.value, node.depth[0] + node.depth[1])
        if formatter.use_black:
            # Monkey patching black to not normalize strings
            old_format_str = formatter.code_formatter.black.format_str

            def format_str(string, mode=None):
                black_mode = formatter.code_formatter.black.Mode(
                    line_length=mode.line_length, string_normalization=False
                )
                return old_format_str(string, mode=black_mode)

            # ugly way to monkeypatch the above function only once
            if "add_formatter_only_once" not in formatter.code_formatter.PY_RESERVED_WORDS:
                formatter.code_formatter.black.format_str = format_str
                formatter.code_formatter.PY_RESERVED_WORDS.append("add_formatter_only_once")

        if tag.code and formatter.use_black:
            tag.code, tag.is_blackened = formatter.code_formatter.format_string(
                tag.code,
                max_length=tag.max_code_length(max_length),
            )

            if "{%" in node.value:
                parts = tag.code.split("\n")
                prefix = INDENT * (node.depth[0] + node.depth[1])
                if len(parts) > 1:
                    tag.code = "\n".join([f'{prefix if i != 0 else ""}{part}' for i, part in enumerate(parts)])

        node.value = str(tag)

        return tag.is_blackened

    else:
        return False


# Some monkey patching
Line.from_nodes = MethodType(from_nodes, Line)
JinjaFormatter._format_jinja_node = MethodType(_format_jinja_node, JinjaFormatter)


class TinybirdNodeManager(NodeManager):
    def __init__(self, case_sensitive_names: bool, lower_keywords: bool) -> None:
        super().__init__(case_sensitive_names)
        self.lower_keywords = lower_keywords

    def create_node(self, token: Token, previous_node: Optional[Node]) -> Node:
        try:
            return super().create_node(token, previous_node)
        except SqlfmtBracketError:
            # we already have tb check and the toolset to check the SQL, no need for this extra validation
            prev_token, extra_whitespace = get_previous_token(previous_node)
            prefix = self.whitespace(token, prev_token, extra_whitespace)
            value = self.standardize_value(token)
            return Node(
                token=token,
                previous_node=previous_node,
                prefix=prefix,
                value=value,
                formatting_disabled=False,
            )

    def raise_on_mismatched_bracket(self, token: Token, last_bracket: Node) -> None:
        # we already have tb check and the toolset to check the SQL, no need for this extra validation
        pass

    def standardize_value(self, token: Token) -> str:
        """
        Tinybird => Patched to not lower keywords
        https://github.com/tconbeer/sqlfmt/blob/c11775b92d8a45f0e91d871b81a88a894d620bec/src/sqlfmt/node_manager.py#L215
        """
        if self.lower_keywords:
            return super().standardize_value(token)

        if token.type in (
            TokenType.UNTERM_KEYWORD,
            TokenType.STATEMENT_START,
            TokenType.STATEMENT_END,
            TokenType.WORD_OPERATOR,
            TokenType.ON,
            TokenType.BOOLEAN_OPERATOR,
            TokenType.SET_OPERATOR,
        ):
            return " ".join(token.token.split())
        else:
            return token.token


class TinybirdDialect(ClickHouse):
    """
    This is an extension of the base rules.

    We might need to override the base `word_operator` and `unterm_keyword` rules with some custom ClickHouse terms.

    For now we are just overriding the end blocks for `if` and `for` to work with Tornado templates.

    https://github.com/tconbeer/sqlfmt/blob/c11775b92d8a45f0e91d871b81a88a894d620bec/src/sqlfmt/dialect.py#L55
    """

    def __init__(self, lower_keywords: bool = False) -> None:
        super().__init__()
        self.lower_keywords = lower_keywords

        override_rules = {
            "main": [
                Rule(
                    name="quoted_name",
                    priority=200,
                    pattern=group(
                        # tripled single quotes (optionally raw/bytes)
                        r"(rb?|b|br)?'''.*?'''",
                        # tripled double quotes
                        r'(rb?|b|br)?""".*?"""',
                        # possibly escaped double quotes
                        r'(rb?|b|br|u&|@)?"([^"\\]*(\\.[^"\\]*|""[^"\\]*)*)"',
                        # possibly escaped single quotes
                        r"(rb?|b|br|u&|x)?'([^'\\]*(\\.[^'\\]*|''[^'\\]*)*)'",
                        r"\$\w*\$[^$]*?\$\w*\$",  # pg dollar-delimited strings
                        # possibly escaped backtick
                        r"`([^`\\]*(\\.[^`\\]*)*)`",
                        r"((?<!\\)\$[\w$]+)",  # Words starting with $ for .incl files
                    ),
                    action=partial(actions.add_node_to_buffer, token_type=TokenType.QUOTED_NAME),
                ),
            ],
            "jinja": [
                Rule(
                    name="jinja_if_block_end",
                    priority=203,
                    pattern=group(r"\{%-?\s*end\s*-?%\}"),
                    action=actions.raise_sqlfmt_bracket_error,
                ),
                Rule(
                    name="jinja_for_block_end",
                    priority=211,
                    pattern=group(r"\{%-?\s*end\s*-?%\}"),
                    action=actions.raise_sqlfmt_bracket_error,
                ),
            ],
        }

        for section in override_rules:
            for rule in override_rules[section]:
                for rr in self.RULES[section]:
                    if rr.name == rule.name:
                        self.RULES[section].remove(rr)
                        self.RULES[section].append(rule)
                        break

    def initialize_analyzer(self, line_length: int) -> Analyzer:
        """
        Creates and returns an analyzer that uses the Dialect's rules for lexing
        Custom NodeManager for Tinybird
        """
        analyzer = Analyzer(
            line_length=line_length,
            rules=self.get_rules(),
            node_manager=TinybirdNodeManager(self.case_sensitive_names, self.lower_keywords),
        )
        return analyzer


@dataclass
class TBMode(Mode):
    lower_keywords: bool = False

    def __post_init__(self) -> None:
        """
        Tinybird => Overriden to use `TinybirdDialect`
        https://github.com/tconbeer/sqlfmt/blob/c11775b92d8a45f0e91d871b81a88a894d620bec/src/sqlfmt/mode.py#L31
        """
        self.dialect = TinybirdDialect(self.lower_keywords)


def _calc_str(self) -> str:
    if self.is_multiline:
        return self.token.token + "\n"
    else:
        marker, comment_text = self._comment_parts()
        if comment_text == "":
            return marker + "\n"
        return marker + " " + comment_text + "\n"


Comment._calc_str = property(_calc_str)


def format_sql_template(sql: str, line_length: Optional[int] = None, lower_keywords: bool = False) -> str:
    try:
        # https://github.com/tconbeer/sqlfmt/blob/c11775b92d8a45f0e91d871b81a88a894d620bec/src/sqlfmt/mode.py#L16-L29
        config: Dict[str, Any] = {
            "line_length": line_length or DEFAULT_FMT_LINE_LENGTH,
            "lower_keywords": lower_keywords,
        }

        mode = TBMode(**config)
        sql = sql.strip()
        return (
            "%\n" + api.format_string(sql[1:], mode=mode).strip()
            if sql[0] == "%"
            else api.format_string(sql, mode=mode).strip()
        )
    except Exception as e:
        logging.warning(f"sqlfmt error: {str(e)}")
        return sql
