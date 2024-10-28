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
    database_name = "HLCA_cellRef"
    delete_database(database_name)
    db = create_or_get_database(database_name)
except Exception:
    print_exc()  

try:
    graph_name = "hlca_cellref"
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
publication_vertex_name = "Publication"
Publication = create_or_get_vertex_collection(graph, publication_vertex_name)
gene_vertex_name = "Gene"
Gene = create_or_get_vertex_collection(graph, gene_vertex_name)


cell_set_dataset_vertex_name = "Cell_set_dataset"
Cell_set_dataset = create_or_get_vertex_collection(graph, cell_set_dataset_vertex_name)
disease_vertex_name = "Disease"
Disease = create_or_get_vertex_collection(graph, disease_vertex_name)


#create_or_get_edge_collection(graph, "Transcript", "Cell_type")
biomarker_cellSet = create_or_get_edge_collection(graph, "Biomarker_combination", "Cell_set")
cellSet_cellType = create_or_get_edge_collection(graph, "Cell_set", "Cell_type")
cellType_AnatomicStructure= create_or_get_edge_collection(graph, "Cell_type", "Anatomic_structure")
publication_cellSet = create_or_get_edge_collection(graph, "Publication", "Cell_set")
gene_biomarker = create_or_get_edge_collection(graph, "Gene", "Biomarker_combination")
gene_cellSet = create_or_get_edge_collection(graph, "Biomarker_combination", "Gene")


Disease_Cell_type= create_or_get_edge_collection(graph, "Disease", "Cell_type")
Disease_Transcript= create_or_get_edge_collection(graph, "Disease", "Transcript")



Anatomic_structure.insert({"_key": "Organ_12345", "name": "Lung"})
Publication.insert({"_key": "HLCA_2023_Sikkema", "name": "HLCA_2023_Sikkema_doi.org/10.1038/s41591-023-02327-2"})
Publication.insert({"_key": "cellRef_2023_Guo", "name": "cellRef_2023_Guo_doi.org/10.1038/s41467-023-40173-5"})

# Read SSS file
with open("../data/HLCA_CellRef_matching_ver3_import.txt", "r") as f:
    data_in= f.read().strip().split("\n")
    
    #initiate dictionary for each vertex collection
    Biomarker_combination_dic={}
    Cell_set_dic={}
    Cell_type_dic={}
    Anatomic_structure_dic={}
    Gene_dic={}
    Publication_dic={}
    
    #initiate list for each edge collection
    publication_cellSet_list=[]
    gene_biomarker_list=[]
    cellSet_cellType_list=[]
    gene_cellSet_list=[]
    biomarker_cellSet_list=[]
    cellType_AnatomicStructure_list=[]
    for row in data_in[2:64]:

        field=row.split("\t")

        if "CL" in field[40] and "skos" in field[38]:
            # find name for vertex
            Cell_set_hlca_name=field[5] + "_HLCA"
            Cell_set_cellref_name=field[6] + "_cellRef"
            Cell_type_name=field[39]
            Biomarker_combination_hlca_name=field[30]
            Biomarker_combination_cellref_name=field[31]
            idx_hlca = field[0]
            idx_cellref = field[1]
            
            # generate key for vertex
            Cell_set_hlca_key = "Cell_set_hlca_" + idx_hlca
            Cell_set_cellref_key = "Cell_set_cellref_" + idx_cellref
            #print(Cell_set_cellref_key)
            Cell_type_key = "Cell_type_" + idx_hlca
            #print(Cell_type_key)
            Biomarker_combination_hlca_key = "Biomarker_combination_hlca_" + idx_hlca
            Biomarker_combination_cellref_key = "Biomarker_combination_cellref_" + idx_cellref


            # import cell set data
            if Cell_set_hlca_name not in Cell_set_dic:
                Cell_set_dic[Cell_set_hlca_name]= Cell_set_hlca_key
                d1= {"_key":Cell_set_hlca_key, "name":Cell_set_hlca_name}
                Cell_set.insert(d1)
            else:
                #print("This Cell_set is already in db: ", Cell_set_name)
                Cell_set_hlca_key = Cell_set_dic[Cell_set_hlca_name] 
            if Cell_set_cellref_name not in Cell_set_dic:
                Cell_set_dic[Cell_set_cellref_name]= Cell_set_cellref_key
                d2= {"_key":Cell_set_cellref_key, "name":Cell_set_cellref_name}
                Cell_set.insert(d2)
            else:
                #print("This Cell_set is already in db: ", Cell_set_name)
                Cell_set_cellref_key = Cell_set_dic[Cell_set_cellref_name]
                
            
            # import cell type data    
            if Cell_type_name not in Cell_type_dic and Cell_type_name !="unknown":
                Cell_type_dic[Cell_type_name]= Cell_type_key
                d3= {"_key":Cell_type_key, "name":Cell_type_name}
                Cell_type.insert(d3) 
            else:
                print("This Cell_type is unknown: ", Cell_type_name)
                 
             
             # import Biomarker_combination data                 
            if Biomarker_combination_hlca_name not in Biomarker_combination_dic:
                Biomarker_combination_dic[Biomarker_combination_hlca_name]= Biomarker_combination_hlca_key
                d4= {"_key":Biomarker_combination_hlca_key,"name":Biomarker_combination_hlca_name}
                Biomarker_combination.insert(d4)   
            else:
                #print("This cell type is already in db: ", cell_name)
                Biomarker_combination_hlca_key = Biomarker_combination_dic[Biomarker_combination_hlca_name]
            if Biomarker_combination_cellref_name not in Biomarker_combination_dic:
                Biomarker_combination_dic[Biomarker_combination_cellref_name]= Biomarker_combination_cellref_key
                d5= {"_key":Biomarker_combination_cellref_key,"name":Biomarker_combination_cellref_name}
                Biomarker_combination.insert(d5)   
            else:
                #print("This cell type is already in db: ", cell_name)
                Biomarker_combination_cellref_key = Biomarker_combination_dic[Biomarker_combination_cellref_name]
                

            
         
            # generate edge keys 
            biomarker_cellSet_hlca_edge_key = Biomarker_combination_hlca_key + "_" + Cell_set_hlca_key
            biomarker_cellSet_cellref_edge_key = Biomarker_combination_cellref_key + "_" + Cell_set_cellref_key
            

            publication_cellSet_hlca_edge_key = "HLCA_2023_Sikkema_" + Cell_set_hlca_key
            publication_cellSet_cellref_edge_key = "cellRef_2023_Guo_" + Cell_set_cellref_key
            cellType_AnatomicStructure_edge_key = Cell_type_key + "_Organ_12345"  
            cellSet_cellType_hlca_edge_key =Cell_set_hlca_key + "_" + Cell_type_key
            cellSet_cellType_cellref_edge_key =Cell_set_cellref_key + "_" + Cell_type_key
         
         
            # import cellType_AnatomicStructure edge data
            if cellType_AnatomicStructure_edge_key not in cellType_AnatomicStructure_list and Cell_type_name !="unknown":
                cellType_AnatomicStructure_list.append(cellType_AnatomicStructure_edge_key)
                Cell_type_key = Cell_type_dic[Cell_type_name]          
                d6 = {"_key": cellType_AnatomicStructure_edge_key, "_from":f"Cell_type/{Cell_type_key}" ,"_to":"Anatomic_structure/Organ_12345","name": "PART_OF"}
                cellType_AnatomicStructure.insert(d6)
            else:
                print("This cellType_AnatomicStructure edge is not exist: ", cellType_AnatomicStructure_edge_key)
            
           
            # import publication_cellSet edge data
            if publication_cellSet_hlca_edge_key not in publication_cellSet_list:
                publication_cellSet_list.append(publication_cellSet_hlca_edge_key)
                Cell_set_key = Cell_set_dic[Cell_set_hlca_name]          
                d7 = {"_key": publication_cellSet_hlca_edge_key, "_from":f"Cell_set/{Cell_set_key}" ,"_to":"Publication/HLCA_2023_Sikkema","name": "SOURCE"}
                publication_cellSet.insert(d7)
            else:
                print("This cellset_publication is already in db:", publication_cellSet_hlca_edge_key)
            if publication_cellSet_cellref_edge_key not in publication_cellSet_list:
                publication_cellSet_list.append(publication_cellSet_cellref_edge_key)
                Cell_set_key = Cell_set_dic[Cell_set_cellref_name]          
                d7 = {"_key": publication_cellSet_cellref_edge_key, "_from":f"Cell_set/{Cell_set_key}" ,"_to":"Publication/cellRef_2023_Guo","name": "SOURCE"}
                publication_cellSet.insert(d7)
            else:
                print("This cellset_publication is already in db:", publication_cellSet_cellref_edge_key)


            # import cell_type_cellSet edge data
            if cellSet_cellType_hlca_edge_key not in cellSet_cellType_list and Cell_type_name !="unknown":
                cellSet_cellType_list.append(cellSet_cellType_hlca_edge_key)
                Cell_type_key = Cell_type_dic[Cell_type_name]
                Cell_set_key = Cell_set_dic[Cell_set_hlca_name]         
                d8 = {"_key": cellSet_cellType_hlca_edge_key, "_from": f"Cell_set/{Cell_set_key}","_to": f"Cell_type/{Cell_type_key}","name": "IS_INSTANCE_OF"}
                cellSet_cellType.insert(d8)
            else:
                print("This cellType_cellSet is already in db:", cellSet_cellType_hlca_edge_key)

            if cellSet_cellType_cellref_edge_key not in cellSet_cellType_list and Cell_type_name !="unknown":
                cellSet_cellType_list.append(cellSet_cellType_cellref_edge_key)
                Cell_type_key = Cell_type_dic[Cell_type_name]
                Cell_set_key = Cell_set_dic[Cell_set_cellref_name]         
                d9 = {"_key": cellSet_cellType_cellref_edge_key, "_from": f"Cell_set/{Cell_set_key}","_to": f"Cell_type/{Cell_type_key}","name": "IS_INSTANCE_OF"}
                cellSet_cellType.insert(d9)
            else:
                print("This cellType_cellSet is already in db:", cellSet_cellType_cellref_edge_key)           
            
        

            # biomarker_cellSet edge data
            if biomarker_cellSet_hlca_edge_key not in biomarker_cellSet_list:
                biomarker_cellSet_list.append(biomarker_cellSet_hlca_edge_key)
                Biomarker_combination_key = Biomarker_combination_dic[Biomarker_combination_hlca_name]
                Cell_set_key = Cell_set_dic[Cell_set_hlca_name]
                d10 = {"_key": biomarker_cellSet_hlca_edge_key, "_from":f"Biomarker_combination/{Biomarker_combination_key}","_to":f"Cell_set/{Cell_set_key}" ,"name": "IS_MARKER_FOR"}
                biomarker_cellSet.insert(d10)
            else:
                print("This edge is already in db:", biomarker_cellSet_hlca_edge_key)
            if biomarker_cellSet_cellref_edge_key not in biomarker_cellSet_list:
                biomarker_cellSet_list.append(biomarker_cellSet_cellref_edge_key)
                Biomarker_combination_key = Biomarker_combination_dic[Biomarker_combination_cellref_name]               
                Cell_set_key = Cell_set_dic[Cell_set_cellref_name]
                d11 = {"_key": biomarker_cellSet_cellref_edge_key, "_from":f"Biomarker_combination/{Biomarker_combination_key}","_to":f"Cell_set/{Cell_set_key}" ,"name": "IS_MARKER_FOR"}
                biomarker_cellSet.insert(d11)
            else:
                print("This edge is already in db:", biomarker_cellSet_hlca_edge_key)

            
            
               
                

  

 














