import ast
from glob import glob
import os
from traceback import print_exc

from arango import ArangoClient
import pandas as pd

ARANGO_URL = "http://localhost:8529"
ARANGO_CLIENT = ArangoClient(hosts=ARANGO_URL)
SYS_DB = ARANGO_CLIENT.db("_system", username="root", password="")

#DATA_DIR = "../data"

#NCBI_CELL_DIR = f"{DATA_DIR}/ncbi-cell"
#NSFOREST_DIR = f"{DATA_DIR}/nsforest-2024-06-27"


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
    


def delete_graph(db, graph_name):
    """Delete an ArangoDB database graph.

    Parameters
    ----------
    db : arango.database.StandardDatabase
        Database
    graph_name : str
        Name of the graph to delete

    Returns
    -------
    None
    """
    # Delete the graph
    if db.has_graph(graph_name):
        print(f"Deleting database graph: {graph_name}")
        db.delete_graph(graph_name)


def create_or_get_vertex_collection(graph, vertex_name):
    """Create, or get an ArangoDB database graph vertex collection.

    Parameters
    ----------
    graph : arango.graph.Graph
        Graph
    vertex_name : str
        Name of the vertex collection to create or get

    Returns
    -------
    collection : arango.collection.VertexCollection
        Graph vertex collection
    """
    # Create, or get the vertex collection
    if not graph.has_vertex_collection(vertex_name):
        print(f"Creating graph vertex collection: {vertex_name}")
        collection = graph.create_vertex_collection(vertex_name)
    else:
        print(f"Getting graph vertex collection: {vertex_name}")
        collection = graph.vertex_collection(vertex_name)

    return collection


def delete_vertex_collection(graph, vertex_name):
    """Delete an ArangoDB database graph vertex collection.

    Parameters
    ----------
    graph : arango.graph.Graph
        Graph
    vertex_name : str
        Name of the vertex collection to delete

    Returns
    -------
    None
    """
    # Delete the vertex collection
    if graph.has_vertex_collection(vertex_name):
        print(f"Deleting graph vertex collection: {vertex_name}")
        graph.delete_vertex_collection(vertex_name)


def create_or_get_edge_collection(graph, from_vertex_name, to_vertex_name):
    """Create, or get an ArangoDB database edge collection from and
    to the specified vertices.

    Parameters
    ----------
    graph : arango.graph.Graph
        Graph
    from_vertex : str
        Name of the vertex collection from which the edge originates
    to_vertex : str
        Name of the vertex collection to which the edge terminates

    Returns
    -------
    collection : arango.collection.EdgeCollection
        Graph edge collection
    collection_name : str
        Name of the edge collection
    """
    # Create, or get the edge collection
    collection_name = f"{from_vertex_name}_{to_vertex_name}"
    if not graph.has_edge_definition(collection_name):
        print(f"Creating edge definition: {collection_name}")
        collection = graph.create_edge_definition(
            edge_collection=collection_name,
            from_vertex_collections=[f"{from_vertex_name}"],
            to_vertex_collections=[f"{to_vertex_name}"],
        )
    else:
        print(f"Getting edge collection: {collection_name}")
        collection = graph.edge_collection(collection_name)

    return collection


def delete_edge_collection(graph, edge_name):
    """Delete an ArangoDB database graph edge definition and collection.

    Parameters
    ----------
    graph : arango.graph.Graph
        Graph
    edge_name : str
        Name of the edge definition and collection to delete

    Returns
    -------
    None
    """
    # Delete the collection
    if graph.has_edge_definition(edge_name):
        print(f"Deleting graph edge definition and collection: {edge_name}")
        graph.delete_edge_definition(edge_name)

try:
    database_name = "NLM_cell"
    delete_database(database_name)
    db = create_or_get_database(database_name)
except Exception:
    print_exc()  

try:
    graph_name = "nlm_cell"
    delete_graph(db, graph_name)
    graph = create_or_get_graph(db, graph_name)
except Exception:
    print_exc()      
    

cell_type_vertex_name = "Cell_type"
Cell_type = create_or_get_vertex_collection(graph, cell_type_vertex_name)
transcript_vertex_name = "Transcript"
Transcript = create_or_get_vertex_collection(graph, transcript_vertex_name)
disease_vertex_name = "Disease"
Disease = create_or_get_vertex_collection(graph, disease_vertex_name)
organ_vertex_name = "Organ"
Organ = create_or_get_vertex_collection(graph, organ_vertex_name)

#create_or_get_edge_collection(graph, "Transcript", "Cell_type")
Transcript_Cell_type = create_or_get_edge_collection(graph, "Transcript", "Cell_type")
CellType_CellType = create_or_get_edge_collection(graph, "Cell_type", "Cell_type")
Disease_Cell_type= create_or_get_edge_collection(graph, "Disease", "Cell_type")
Disease_Transcript= create_or_get_edge_collection(graph, "Disease", "Transcript")
Organ_Cell_type = create_or_get_edge_collection(graph, "Organ", "Cell_type")

Organ.insert({"_key": "Organ_12345", "name": "Lung"})




# Read SSS file
with open("../../../sss.csv", "r") as f:
    data_in= f.read().strip().split("\n")
    transcript_dic={}
    cell_dic={}
    transcriptCell_list=[]
    organ_Cell_list=[]
    for row in data_in[:]:

        field=row.split(",")

        if "transcript" in field[1] and "cell" in field[4]:
            transcript_name=field[0]
            cell_name=field[3]
            idx = field[6]
            
            transcript_key = transcript_name.replace(" ", "-") + "_" + idx
            cell_key = cell_name.replace(" ", "-")  + "_" + idx

        

            if transcript_name not in transcript_dic:
                transcript_dic[transcript_name]= transcript_key
                d1= {"_key":transcript_key, "name":transcript_name}
                Transcript.insert(d1) 
            else:
                #print("This transcript is already in db: ", transcript_name)
                transcript_key = transcript_dic[transcript_name]
                
                
            if cell_name not in cell_dic:
                cell_dic[cell_name]= cell_key
                d2= {"_key":cell_key,"name":cell_name}
                Cell_type.insert(d2)
            else:
                #print("This cell type is already in db: ", cell_name)
                cell_key = cell_dic[cell_name]

            transcriptCell_edge_key = transcript_key + "_" +cell_key
            OrganCell_edge_key = cell_key + "_Organ_12345"
            if transcriptCell_edge_key not in transcriptCell_list:
                transcriptCell_list.append(transcriptCell_edge_key)
                transcript_key = transcript_dic[transcript_name]
                cell_key = cell_dic[cell_name]
                d3 = {"_key": transcriptCell_edge_key, "_from": f"Cell_type/{cell_key}","_to": f"Transcript/{transcript_key}","name": "PART_OF"}
                Transcript_Cell_type.insert(d3)
            else:
                print("This edge is already in db:", transcriptCell_edge_key)
            
            if OrganCell_edge_key not in organ_Cell_list:
                organ_Cell_list.append(OrganCell_edge_key)
                cell_key = cell_dic[cell_name]          
                d4 = {"_key": OrganCell_edge_key, "_from": "Organ/Organ_12345","_to": f"Cell_type/{cell_key}","name": "PART_OF"}
                Organ_Cell_type.insert(d4)

            else:
                print("This organ cell type edge is already in db:", OrganCell_edge_key)
            
print(len(cell_dic))
  
 














