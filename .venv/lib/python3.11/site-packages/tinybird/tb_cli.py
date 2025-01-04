import asyncio
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import tinybird.tb_cli_modules.auth
import tinybird.tb_cli_modules.branch
import tinybird.tb_cli_modules.cli
import tinybird.tb_cli_modules.common
import tinybird.tb_cli_modules.connection
import tinybird.tb_cli_modules.datasource
import tinybird.tb_cli_modules.fmt
import tinybird.tb_cli_modules.job
import tinybird.tb_cli_modules.pipe
import tinybird.tb_cli_modules.tag
import tinybird.tb_cli_modules.test
import tinybird.tb_cli_modules.token
import tinybird.tb_cli_modules.workspace
import tinybird.tb_cli_modules.workspace_members

cli = tinybird.tb_cli_modules.cli.cli

if __name__ == "__main__":
    cli()
