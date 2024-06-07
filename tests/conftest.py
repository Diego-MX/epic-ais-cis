from sys import dont_write_bytecode
import pytest
import dependencies as deps


# Revisar FIXTURE(SCOPE, AUTOUSE)
@pytest.fixture(scope="session", autouse=True)
def setup_epicpy_writecode():
    deps.gh_epicpy(deps.EPIC_REF, deps.USER_FILE)
    dont_write_bytecode = True
