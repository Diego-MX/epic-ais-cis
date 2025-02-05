# Databricks notebook source
import sys
import pytest

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# Run pytest.
retcode = pytest.main(["./test_dbks.py", "-v", "-p", "no:cacheprovider"])
print(retcode)

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."
