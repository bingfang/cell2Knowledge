

import ast
from glob import glob
import os
from traceback import print_exc

from arango import ArangoClient
import pandas as pd

ARANGO_URL = "http://localhost:8529"
ARANGO_CLIENT = ArangoClient(hosts=ARANGO_URL)
SYS_DB = ARANGO_CLIENT.db("_system", username="root", password="")

def create_or_get_database(database_name):
    """Create or get an ArangoDB database.

    Parameters
    ----------
    database_name : str
        Name of the database to create or get

    Returns
    -------
    db : arango.database.StandardDatabase
        Database
    """
    # Create database, if needed
    if not SYS_DB.has_database(database_name):
        print(f"Creating ArangoDB database: {database_name}")
        SYS_DB.create_database(database_name)

    # Connect to database
    print(f"Getting ArangoDB database: {database_name}")
    db = ARANGO_CLIENT.db(database_name, username="root", password="")

    return db
  
    
def delete_database(database_name):
    """Delete an ArangoDB database.

    Parameters
    ----------
    database_name : str
        Name of the database to delete

    Returns
    -------
    None
    """
    # Delete database, if needed
    if SYS_DB.has_database(database_name):
        print(f"Deleting ArangoDB database: {database_name}")
        SYS_DB.delete_database(database_name)

try:
    database_name = "nlm-v0.1.0"
    delete_database(database_name)
    db = create_or_get_database(database_name)
except Exception:
    print_exc()
    
    
def create_or_get_graph(db, graph_name):
    """Create or get an ArangoDB database graph.

    Parameters
    ----------
    db : arango.database.StandardDatabase
        Database
    graph_name : str
        Name of the graph to create or get

    Returns
    -------
    graph : arango.graph.Graph
        Database graph
    """
    # Create, or get the graph
    if not db.has_graph(graph_name):
        print(f"Creating database graph: {graph_name}")
        graph = db.create_graph(graph_name)
    else:
        print(f"Getting database graph: {graph_name}")
        graph = db.graph(graph_name)

    return graph
    
    
graph_name = "nlm_cell"
graph = create_or_get_graph(db, graph_name)    
    
    
    
    