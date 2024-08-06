
# pylint: disable=unspecified-encoding
# pylint: disable=undefined-variable
import json 
import os
from pathlib import Path
import re
from subprocess import check_call

import config 


def dotenv_manual(env_file='.env'):
    rm_comment = lambda ss: re.sub('#.*$', '', ss)
    with open(env_file, 'r') as e_file: 
        noncommented = map(rm_comment, e_file.readlines())
    secret_reg = r'([A-Z_]*) = \"?([^\s\=]*)\"?'
    the_secrets = {mm.group(1): mm.group(2)
        for ll in noncommented if (mm := re.match(secret_reg, ll))}         
    os.environ.update(the_secrets)
        

def token_from_server(): 
    if 'DATABRICKS_RUNTIME_ENVIRONMENT' in os.environ: 
        # spark = SparkSession.builder.getOrCreate()
        # dbutils = DBUtils(spark)
        with open(config.USER_FILE, 'r') as j_file: 
            tokener = json.load(j_file)
        the_token = dbutils.secrets.get(tokener['dbks_scope'], tokener['dbks_token'])
        os.environ['GH_ACCESS_TOKEN'] = the_token
    elif Path('.env').is_file(): 
        dotenv_manual('.env')
    else: 
        raise EnvironmentError("Cannot set Github token to install from Github.")


def install_reqs(): 
    check_call(['pip', 'install', '--requirement', config.REQS_FILE])


if __name__ == '__main__': 
    if 'GH_ACCESS_TOKEN' not in os.environ: 
        token_from_server()
    install_reqs()