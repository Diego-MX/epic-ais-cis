
import os
from dotenv import load_dotenv 

load_dotenv(override=True)

ENV = os.getenv('ENV_TYPE')
SERVER = os.getenv('SERVER_TYPE')