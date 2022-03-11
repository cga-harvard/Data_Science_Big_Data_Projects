
import os
import pandas as pd
import osmnx as ox
import datetime
import numpy as np


# ox.config(use_cache=True, log_console=False)
# @jit(nopython=True) # Set "nopython" mode for best performance, equivalent to @njit
def get_unique_locations(state_data):
    
    # get all unique place locations in the state data, save it into a dataframe called places_dataframe
    places_set = set()
    for k,v in state_data.iterrows():
        x,y = v["orig_lon"],v["orig_lat"]
        x_,y_ = v["dest_lon"],v["dest_lat"]
        places_set.add((x,y ))
        places_set.add((x_,y_ ))        
    temp = list(places_set)
    places_dataframe = pd.DataFrame(np.array(temp),index=temp)
    return places_dataframe
    # places_dataframe should like follows, each row is a location in the state data set
    #                                      0   1   
    # (-51.071587, 0.079208098)   -51.071587  0.079208    
    # (-51.047249, 0.067405201)   -51.047249  0.067405    
    # (-51.179249, -0.0546593)    -51.179249  -0.054659   
    # (-51.051659, 0.0588351) -51.051659  0.058835    
    # (-51.07864, 0.0070496998)   -51.078640  0.007050    

def create_graph(state_name):
    # create graph
    print("Creating graph...")
    t1 = datetime.datetime.now()
    ox.config(use_cache=True, log_console=False)
# create a graph from  osm, may take half an hour
    G = ox.graph_from_place('%s, Brazil'%state_name, network_type='drive')
    t2 = datetime.datetime.now()
    print("Create graph use:",t2-t1)
    G = ox.speed.add_edge_speeds(G)
    G = ox.speed.add_edge_travel_times(G)
    
    return G

def find_nearest_nodes_in_graph(state_data,G):
    
    t1 = datetime.datetime.now()
    places_dataframe = get_unique_locations(state_data)
    # find all nearset nodes( in the graph) of the all the places (in state data set)
    print("Find nearest_nodes...")
    nearests = ox.distance.nearest_nodes(G, places_dataframe[0].values,places_dataframe[1].values)
    places_dataframe["nearest"] = nearests

    
# prepare the all the nearset nodes of each OD, save it into  a array caleed ods
    nearest_nodes_of_ODs = []
    for k,v in state_data.iterrows():
        # print(v["orig_lat"],v["orig_lon"])
        # origs = ox.distance.nearest_nodes(G, v["orig_lat"], v["orig_lon"])
        # dests = ox.distance.nearest_nodes(G, v["dest_lat"], v["dest_lon"])

        x,y = v["orig_lon"],v["orig_lat"]
        o = places_dataframe.loc[[(x,y)],"nearest"].values[0]
        x_,y_ = v["dest_lon"],v["dest_lat"]
        d = places_dataframe.loc[[(x_,y_)],"nearest"].values[0]
        nearest_nodes_of_ODs.append([o,d])
    nearest_nodes_of_ODs = np.array(nearest_nodes_of_ODs)    

    t2 = datetime.datetime.now()
    print("Find nearest_nodes use",t2-t1)    
    return nearest_nodes_of_ODs


def get_shortest_path(state_data,state_name):

    G = create_graph(state_name)
    nearest_nodes_of_ODs = find_nearest_nodes_in_graph(state_data,G)

    t3 =  datetime.datetime.now()
    # use osmox to find  all the shortest paths between all the ODs in the state data set, 
    # use multi-processing with the cpus parameters
    print("Finding the shortest path...")
    routes = ox.shortest_path(G, nearest_nodes_of_ODs[:,0], nearest_nodes_of_ODs[:,1], weight="travel_time", cpus=16)
    t4 =  datetime.datetime.now()
    print("Done use:",t4-t3)
    # caclulate all the total distances in each shortest paths, save these distance into state 
    # data set and be read to save it
    total_distance_list = []
    for route in routes:
        edge_lengths = ox.utils_graph.get_route_edge_attributes(G, route, "length")
        total_distance = sum(edge_lengths)
        total_distance_list.append(total_distance)

#     
    state_data["shortest drive distance"] = total_distance_list

    return state_data




def main():

    out_path =  "./data/data/"
    state_cvs_path = "./data/data/state_cvs/"
    need_process = set(os.listdir(state_cvs_path)) - set(os.listdir(out_path+"state_csv_with_distance"))
    print(need_process)
    # 12 have error in this machine
    # 15 have many errors
    # start = 25
    for i,file_name in enumerate(os.listdir(state_cvs_path)):
        # if i < start:
        #     continue
        if file_name not in need_process:
            continue
        if ".ipynb_checkpoints" in file_name:
            continue    
        t0 =  datetime.datetime.now()
    #     read data
        state_name = file_name.split(".")[0] 

        # if state_name != "Minas Gerais":
        #     continue

        # read data in one state
        state_data = pd.read_csv(state_cvs_path+file_name,index_col=0)
        state_data = state_data[['cep', 'cnes', 'tot', 'orig_lat', 'orig_lon', 'dest_lat', 'dest_lon']]
        # state_data = state_data.sample(100,random_state=3)
        print(t0,"State processing...",state_name,state_data.shape[0],"*"*30,i)


        # state_data = get_shortest_path(state_data,state_name)
    # ------------------------------------------------------------------
        G = create_graph(state_name)
        nearest_nodes_of_ODs = find_nearest_nodes_in_graph(state_data,G)

        t3 =  datetime.datetime.now()
        # use osmox to find  all the shortest paths between all the ODs in the state data set, 
        # use multi-processing with the cpus parameters
        print("Finding the shortest path...")
        routes = ox.shortest_path(G, nearest_nodes_of_ODs[:,0], nearest_nodes_of_ODs[:,1], weight="travel_time", cpus=2)
        t4 =  datetime.datetime.now()
        print("Done use:",t4-t3)
        # caclulate all the total distances in each shortest paths, save these distance into state 
        # data set and be read to save it
        total_distance_list = []
        for route in routes:
            try:
                edge_lengths = ox.utils_graph.get_route_edge_attributes(G, route, "length")
                total_distance = sum(edge_lengths)
            except Exception as e:
                print(e)
                total_distance = None

            total_distance_list.append(total_distance)

        state_data["shortest drive distance"] = total_distance_list
    # --------------------------------------------------------------------------





        print(datetime.datetime.now(),"writing result...")
        state_data.to_csv(out_path + "state_csv_with_distance/%s"%file_name)
        t10 =  datetime.datetime.now()

        print(state_name,"done use",t10-t0)
    
if __name__ == '__main__':
   
    main()


