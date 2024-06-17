import sys 
import pytest
import dependencies as deps

sys.dont_write_bytecode = True
def pytest_config():
    deps.gh_epicpy(deps.EPIC_REF, deps.USER_FILE)
