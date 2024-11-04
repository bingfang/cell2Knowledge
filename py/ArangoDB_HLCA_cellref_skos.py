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
    database_name = "HLCA_cellRef"
    adb.delete_database(database_name)
    db =adb.create_or_get_database(database_name)
except Exception:
    print_exc()  

try:
    graph_name = "hlca_cellref"
    adb.delete_graph(db, graph_name)
    graph = adb.create_or_get_graph(db, graph_name)
except Exception:
    print_exc()      
    


Cell_set = adb.create_or_get_vertex_collection(graph,  "Cell_set")

Biomarker_combination = adb.create_or_get_vertex_collection(graph, "Biomarker_combination")

Cell_type = adb.create_or_get_vertex_collection(graph, "Cell_type")

Anatomic_structure = adb.create_or_get_vertex_collection(graph, "Anatomic_structure")

Publication = adb.create_or_get_vertex_collection(graph, "Publication")

Gene = adb.create_or_get_vertex_collection(graph, "Gene")



biomarker_cellSet,x = adb.create_or_get_edge_collection(graph, "Biomarker_combination", "Cell_set")
cellSet_cellType,d = adb.create_or_get_edge_collection(graph, "Cell_set", "Cell_type")
cellType_AnatomicStructure,y= adb.create_or_get_edge_collection(graph, "Cell_type", "Anatomic_structure")
publication_cellSet,z = adb.create_or_get_edge_collection(graph, "Publication", "Cell_set")
gene_biomarker,a = adb.create_or_get_edge_collection(graph, "Gene", "Biomarker_combination")
gene_cellSet,b = adb.create_or_get_edge_collection(graph, "Biomarker_combination", "Gene")
cellType_cellType,c = adb.create_or_get_edge_collection(graph, "Cell_type", "Cell_type")


Anatomic_structure.insert({"_key": "Organ_12345", "name": "Lung"})
Publication.insert({"_key": "HLCA_2023_Sikkema", "name": "HLCA_2023_Sikkema_doi.org/10.1038/s41591-023-02327-2"})
Publication.insert({"_key": "cellRef_2023_Guo", "name": "cellRef_2023_Guo_doi.org/10.1038/s41467-023-40173-5"})

# Read SSS file
df = pd.read_excel('../data/HLCA_CellRef_matching_ver3_import1.xlsm')
print(df.columns)


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
for index, row in df.iterrows():
	# find name for vertex
	Cell_type_name = row['CL manual match (object)']
	Cell_set_hlca_name="HLCA_" + row["HLCA_ClusterName"]  
	Cell_set_cellref_name="cellref_" + str(row["CellRef_ClusterName"]) 
	Biomarker_combination_hlca_name=row['HLCA_NSForestMarkers']
	Biomarker_combination_cellref_name=row['CellRef_NSForestMarkers']
	
	# generate key for vertex
	idx_hlca = row['HLCA hierarchical cluster order']
	idx_cellref = row['CellRef order']
	Cell_set_hlca_key = "Cell_set_hlca_" + str(idx_hlca)
	Cell_set_cellref_key = "Cell_set_cellref_" + str(idx_cellref)
	Cell_type_key = "Cell_type_" + str(idx_hlca)
	Biomarker_combination_hlca_key = "Biomarker_combination_hlca_" + str(idx_hlca)
	Biomarker_combination_cellref_key = "Biomarker_combination_cellref_" + str(idx_cellref)


	# import cell set data
	if Cell_set_hlca_name not in Cell_set_dic and Cell_set_hlca_name != "no marker":
		Cell_set_dic[Cell_set_hlca_name]= Cell_set_hlca_key
		d1= {"_key":Cell_set_hlca_key, "name":Cell_set_hlca_name}
		Cell_set.insert(d1)

	if Cell_set_cellref_name not in Cell_set_dic and Cell_set_cellref_name != "no marker":
		Cell_set_dic[Cell_set_cellref_name]= Cell_set_cellref_key
		d2= {"_key":Cell_set_cellref_key, "name":Cell_set_cellref_name}
		Cell_set.insert(d2)



	# import cell type data    
	if Cell_type_name not in Cell_type_dic:
		Cell_type_dic[Cell_type_name]= Cell_type_key
		d3= {"_key":Cell_type_key, "name":Cell_type_name}
		Cell_type.insert(d3) 




	# import Biomarker_combination data                 
	if Biomarker_combination_hlca_name not in Biomarker_combination_dic and Biomarker_combination_hlca_name != "no marker":
		Biomarker_combination_dic[Biomarker_combination_hlca_name]= Biomarker_combination_hlca_key
		d4= {"_key":Biomarker_combination_hlca_key,"name":Biomarker_combination_hlca_name}
		Biomarker_combination.insert(d4)   


	if Biomarker_combination_cellref_name not in Biomarker_combination_dic and Biomarker_combination_cellref_name != "no marker":
		Biomarker_combination_dic[Biomarker_combination_cellref_name]= Biomarker_combination_cellref_key
		d5= {"_key":Biomarker_combination_cellref_key,"name":Biomarker_combination_cellref_name}
		print(d5)
		Biomarker_combination.insert(d5)   





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
		d6 = {"_key": cellType_AnatomicStructure_edge_key, "_from":"Cell_type/"+Cell_type_dic[Cell_type_name] ,"_to":"Anatomic_structure/Organ_12345","name": "PART_OF"}
		cellType_AnatomicStructure.insert(d6)
	else:
		print("This cellType_AnatomicStructure edge is not exist: ", cellType_AnatomicStructure_edge_key)

"""
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

"""



  

 














