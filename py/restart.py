import ast
from glob import glob
import os
from traceback import print_exc

from arango import ArangoClient
import pandas as pd

ARANGO_URL = "http://localhost:8529"
ARANGO_CLIENT = ArangoClient(hosts=ARANGO_URL)
SYS_DB = ARANGO_CLIENT.db("_system", username="root", password="")