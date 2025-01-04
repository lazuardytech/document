from typing import Optional, TypedDict


class Region(TypedDict):
    name: str
    provider: str
    api_host: str
    host: str
    default_password: Optional[str]
