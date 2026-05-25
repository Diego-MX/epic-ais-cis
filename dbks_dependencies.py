"""
DX, Epic Bank
CDMX, 17 octubre '23
"""

# pylint: disable=import-error
# pylint: disable=import-outside-toplevel
# pylint: disable=no-name-in-module
# pylint: disable=unspecified-encoding
# pylint: disable=useless-return

from json import load
from subprocess import check_call
from pkg_resources import working_set

from pyspark.dbutils import DBUtils     # pylint: disable=no-name-in-module
from pyspark.sql import SparkSession

import config

has_yaml = 'yaml' in working_set.by_key

def from_reqsfile(a_file=None)->None:
    """Instalación de paquetes proveninte de pip_reqs.txt"""
    a_file = a_file or config.REQS_FILE
    pip_install('-r', a_file)
    return None

def gh_epicpy(ref=None, tokenfile=None, typing=None, verbose=False)->None:
    """Instalación de tags provenientes de github"""
    if typing:
        v_typing = config.V_TYPING if typing is True else typing
        pip_install('--upgrade', f"typing-extensions=={v_typing}")
    the_keys = {
        'url'  : 'github.com/Bineo2/data-python-tools.git',
        'token': token_from_userfile(tokenfile), 
        'ref'  : ref}
    pip_install("git+https://{token}@{url}@{ref}".format(**the_keys))
    if verbose:
        import epic_py
        dumper = {'Epic Ref': ref, 'Epic Ver': epic_py.__version__}
        print(dumper)
    return None

def token_from_userfile(userfile=config.USER_FILE)->None:
    """Obtención de los secretos de user_file"""
    with open(userfile, 'r',encoding = "utf-8") as file:
        tokener = load(file)

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    return dbutils.secrets.get(tokener['dbks_scope'], tokener['dbks_token'])

def pip_install(*args)->None:
    """Instalación de paquetes con pip"""
    check_call(['pip', 'install', *args])
    return None

# Finite Incantatem
