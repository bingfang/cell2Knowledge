import ast
from glob import glob
import os
from traceback import print_exc

from arango import ArangoClient
import pandas as pd

import ArangoDB as adb

ARANGO_URL = "http://localhost:8529"
ARANGO_CLIENT = ArangoClient(hosts=ARANGO_URL)
SYS_DB = ARANGO_CLIENT.db("_system", username="root", password="")

#DATA_DIR = "../data"


try:
    database_name = "NLM_KN1"
    adb.delete_database(database_name)
    db = adb.create_or_get_database(database_name)
except Exception:
    print_exc()  

try:
    graph_name = "nlm-kn1"
    adb.delete_graph(db, graph_name)
    graph = adb.create_or_get_graph(db, graph_name)
except Exception:
    print_exc()      
    
# Read Schema file
df = pd.read_excel('../data/Cell_Phenotype_KG_Schema_v2_test.xlsm')
print(df.columns)


collection={}
node={}     
for index, row in df.iterrows():

    # creat collection from subject entities, load subject node
    collection_name_subject=row["Subject Node"]
    node_name_subject=row["Subject"]
    node_key_subject=row["Subject Node"]+str(row["Idx"])  
    
    if collection_name_subject not in collection:
        collection[collection_name_subject]=adb.create_or_get_vertex_collection(graph, collection_name_subject)
    else:
        print("this collection is already in graph")     
    if node_name_subject not in node:
        d1= {"_key":node_key_subject, "name":node_name_subject}
        (collection[collection_name_subject]).insert(d1)
        node[node_name_subject]=node_key_subject
    else:
        print("this node is already in graph")     
        
    # creat collection from object entities, load object node
    collection_name_object=row["Object Node"]
    node_name_object=row["Object"]
    node_key_object=row["Object Node"]+str(row["Idx"])  
    if collection_name_object not in collection:
        collection[collection_name_object]=adb.create_or_get_vertex_collection(graph, collection_name_object)
    else:
        print("this collection is already in graph") 
    if node_name_object not in node:
        d2= {"_key":node_key_object, "name":node_name_object}
        (collection[collection_name_object]).insert(d2)
        node[node_name_object]=node_key_object
    else:
        print("this node is already in graph")  
        
    
    #  creat edge collection load edge node    
    collection_name =  collection_name_subject +"-" + collection_name_object
    if collection_name not in collection:      
        collect = graph.create_edge_definition(edge_collection=collection_name,from_vertex_collections=[collection_name_subject],to_vertex_collections=[collection_name_object])
        collection[collection_name] = collect
    edge_key=row["Predicate"]+str(row["Idx"]) 
    if edge_key not in node:
        d3 = {"_key": edge_key, "_from":(collection_name_subject +"/" + node[node_name_subject]),"_to":(collection_name_object +"/"+node[node_name_object]), "name":row["Predicate Relation"]}
    (collection[collection_name]).insert(d3)
    
