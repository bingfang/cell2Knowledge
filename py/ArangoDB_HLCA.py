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
    database_name = "HLCA"
    delete_database(database_name)
    db = create_or_get_database(database_name)
except Exception:
    print_exc()  

try:
    graph_name = "hlca"
    delete_graph(db, graph_name)
    graph = create_or_get_graph(db, graph_name)
except Exception:
    print_exc()      
    

cell_set_vertex_name = "Cell_set"
Cell_set = create_or_get_vertex_collection(graph, cell_set_vertex_name)
marker_vertex_name = "Biomarker_combination"
Biomarker_combination = create_or_get_vertex_collection(graph, marker_vertex_name)
cell_type_vertex_name = "Cell_type"
Cell_type = create_or_get_vertex_collection(graph, cell_type_vertex_name)
anatomic_structure_vertex_name = "Anatomic_structure"
Anatomic_structure = create_or_get_vertex_collection(graph, anatomic_structure_vertex_name)

disease_vertex_name = "Disease"
Disease = create_or_get_vertex_collection(graph, disease_vertex_name)


#create_or_get_edge_collection(graph, "Transcript", "Cell_type")
biomarker_cellSet = create_or_get_edge_collection(graph, "Biomarker_combination", "Cell_set")
cellSet_cellType = create_or_get_edge_collection(graph, "Cell_set", "Cell_type")
Disease_Cell_type= create_or_get_edge_collection(graph, "Disease", "Cell_type")
Disease_Transcript= create_or_get_edge_collection(graph, "Disease", "Transcript")
cellType_AnatomicStructure= create_or_get_edge_collection(graph, "Cell_type", "Anatomic_structure")

Anatomic_structure.insert({"_key": "Organ_12345", "name": "Lung"})




# Read SSS file
with open("../data/HLCA_CellRef matching_ver3.txt", "r") as f:
    data_in= f.read().strip().split("\n")
    Biomarker_combination_dic={}
    Cell_set_dic={}
    Cell_type_dic={}
    Anatomic_structure_dic={}
    cellSet_cellType_list=[]
    biomarker_cellSet_list=[]
    cellType_AnatomicStructure_list=[]
    for row in data_in[2:64]:

        field=row.split("\t")

        if "CL" in field[40] and "skos" in field[38]:
            Cell_set_name=field[5]
            Cell_type_name=field[39]
            Biomarker_combination_name=field[30]
            idx = field[0]
            
            Cell_set_key = "Cell_set_" + idx
            print(Cell_set_key)
            Cell_type_key = "Cell_type_" + idx
            print(Cell_type_key)
            Biomarker_combination_key = "Biomarker_combination_" + idx


            
            print(Biomarker_combination_key)

            if Cell_set_name not in Cell_set_dic:
                Cell_set_dic[Cell_set_name]= Cell_set_key
                d1= {"_key":Cell_set_key, "name":Cell_set_name}
                Cell_set.insert(d1) 
            else:
                #print("This Cell_set is already in db: ", Cell_set_name)
                Cell_set_key = Cell_set_dic[Cell_set_name]
                
            
                
            if Cell_type_name not in Cell_type_dic:
                Cell_type_dic[Cell_type_name]= Cell_type_key
                d2= {"_key":Cell_type_key, "name":Cell_type_name}
                Cell_type.insert(d2) 
            else:
                #print("This Cell_type is already in db: ", Cell_type_name)
                Cell_type_key = Cell_type_dic[Cell_type_name]    
                
                
                
            if Biomarker_combination_name not in Biomarker_combination_dic:
                Biomarker_combination_dic[Biomarker_combination_name]= Biomarker_combination_key
                d3= {"_key":Biomarker_combination_key,"name":Biomarker_combination_name}
                Biomarker_combination.insert(d3)
            else:
                #print("This cell type is already in db: ", cell_name)
                Biomarker_combination_key = Biomarker_combination_dic[Biomarker_combination_name]
                

            biomarker_cellSet_edge_key = Biomarker_combination_key + "_" + Cell_set_key
            cellType_AnatomicStructure_edge_key = Cell_type_key + "_Organ_12345"  
            cellSet_cellType_edge_key =Cell_set_key + "_" + Cell_type_key
            
         
            if biomarker_cellSet_edge_key not in biomarker_cellSet_list:
                biomarker_cellSet_list.append(biomarker_cellSet_edge_key)
                Biomarker_combination_key = Biomarker_combination_dic[Biomarker_combination_name]
                Cell_set_key = Cell_set_dic[Cell_set_name]
                d4 = {"_key": biomarker_cellSet_edge_key, "_from":f"Biomarker_combination/{Biomarker_combination_key}","_to":f"Cell_set/{Cell_set_key}" ,"name": "IS_MARKER_FOR"}
                biomarker_cellSet.insert(d4)
            else:
                print("This edge is already in db:", biomarker_cellSet_edge_key)
            

            
            if cellType_AnatomicStructure_edge_key not in cellType_AnatomicStructure_list:
                cellType_AnatomicStructure_list.append(cellType_AnatomicStructure_edge_key)
                Cell_type_key = Cell_type_dic[Cell_type_name]          
                d5 = {"_key": cellType_AnatomicStructure_edge_key, "_from":f"Cell_type/{Cell_type_key}" ,"_to":"Anatomic_structure/Organ_12345","name": "PART_OF"}
                cellType_AnatomicStructure.insert(d5)
            else:
                print("This cellType_AnatomicStructure edge is already in db:", cellType_AnatomicStructure_edge_key)
                
               
                
            if cellSet_cellType_edge_key not in cellSet_cellType_list:
                cellSet_cellType_list.append(cellSet_cellType_edge_key)
                Cell_type_key = Cell_type_dic[Cell_type_name]
                Cell_set_key = Cell_set_dic[Cell_set_name]         
                d6 = {"_key": cellSet_cellType_edge_key, "_from": f"Cell_set/{Cell_set_key}","_to": f"Cell_type/{Cell_type_key}","name": "IS_INSTANCE_OF"}
                cellSet_cellType.insert(d6)
            else:
                print("This cellType_AnatomicStructure edge is already in db:", cellType_AnatomicStructure_edge_key)
            
  
  
 














