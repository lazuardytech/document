from contextvars import ContextVar
from typing import TYPE_CHECKING

# Avoid circular import error
if TYPE_CHECKING:
    from tinybird.user import User

workspace_id: ContextVar[str] = ContextVar("workspace_id")
workspace: ContextVar["User"] = ContextVar("workspace")
table_id: ContextVar[str] = ContextVar("table_id")
hfi_frequency: ContextVar[float] = ContextVar("hfi_frequency")
hfi_frequency_gatherer: ContextVar[float] = ContextVar("hfi_frequency_gatherer")
use_gatherer: ContextVar[bool] = ContextVar("use_gatherer")
allow_gatherer_fallback: ContextVar[bool] = ContextVar("allow_gatherer_fallback")
gatherer_allow_s3_backup_on_user_errors: ContextVar[bool] = ContextVar("gatherer_allow_s3_backup_on_user_errors")
disable_template_security_validation: ContextVar[bool] = ContextVar("disable_template_security_validation")
origin: ContextVar[str] = ContextVar("origin")
request_id: ContextVar[str] = ContextVar("request_id")
engine: ContextVar[str] = ContextVar("engine")
wait_parameter: ContextVar[bool] = ContextVar("wait_parameter")
api_host: ContextVar[str] = ContextVar("api_host")
ff_split_to_array_escape: ContextVar[bool] = ContextVar("ff_split_to_array_escape")
ff_preprocess_parameters_circuit_breaker: ContextVar[bool] = ContextVar("ff_preprocess_parameters_circuit_breaker")
