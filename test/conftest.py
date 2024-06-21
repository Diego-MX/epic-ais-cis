from sys import dont_write_bytecode
import dependencies as deps
import config as cfg

def pytest_configure():
    dont_write_bytecode = True
    deps.from_reqsfile(cfg.REQS_FILE)
    deps.gh_epicpy(cfg.EPIC_REF, cfg.USER_FILE)

