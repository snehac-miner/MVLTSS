__author__ = 'snehachalla'

import pandas as pd
import numpy as np
import random
from DataParsers import DataParsers
from Kuldorff import Kuldorff
from SubsetAggregation_FF import SubsetAggregation
import sys

class Runner(object):
    def caller(self):

            moving_average_window = 2  # to calculate baselines
            win_size = 1   # to calculate
            alpha = 0.1
            #num_of_restarts = 50
            #num_of_iterations_per_restart = 50
            p_0 = 0.5
            #distribution_type = "POISSON"
            #type_of_test = "POSITIVE"

           #self.def_fast_fast_SA(stream_ids,location_ids,counts , baselines,num_of_restarts , num_of_iterations_per_restart,p_0, distribution_type, type_of_test )

            df = pd.DataFrame.from_csv(path = "/Users/snehachalla/Desktop/poisson_mini.csv" , header =0, sep = ",")
            arr = df.as_matrix()
            t=pd.melt(df, id_vars= [ 'time', 'location' ], value_vars= ['ds_1' , 'ds_2'], var_name= 'streams', value_name='counts', col_level=None)

            stream_ids= pd.Series(t['streams'].values.ravel()).unique()
            location_temp = pd.Series(t['location'].values.ravel()).unique()
            time_ids  = pd.Series(t['time'].values.ravel()).unique()
            location_ids = []
            time_ids = list(time_ids)
            stream_ids = list(stream_ids)

            for i in range(0 , len(location_temp)):
                if location_temp[i] != '$' :
                    location_ids.append(location_temp[i])

            counts = np.empty((len(time_ids) ,len(location_ids),len(stream_ids)))

            #print "Length of locations"

            """
            distribution_type =[]
            type_of_test =[]
            for i in range(0,len(stream_ids)):
                type= raw_input("Enter the distribution type of Data Stream "+ str(i ))
                test= raw_input("Input the type of test on datastream "+ str(i))
                distribution_type.append(type)
                type_of_test.append(test)
            """
            for i in range(0 ,len(arr)):
                for j in location_ids:
                    for k in time_ids :
                        for m in range(0,len(stream_ids)):
                            if arr[i][1] == j and arr[i][0] == k :
                                counts[time_ids.index(k)][location_ids.index(j)][m]=arr[i][m+2]

            print ("Loaded data into the counts array")
            #distribution_type = ["POISSON" , "GAUSSIAN"]
            #type_of_test =["POSITIVE" , "NEGATIVE"]
            boo = DataParsers()
            baselines = boo.generate_moving_average_baselines(counts,moving_average_window, -1  )

            print ("Loaded baselines of the data")

            print baselines.shape
            print "baseline 1 time"
            print counts[len(time_ids)-1][len(location_ids)-1][len(stream_ids)-1]
            print baselines[len(time_ids)-1][len(location_ids)-1][len(stream_ids)-1]
            # Set q_max
            q_max =2

            print("Please enter the choice number of the MVLTSS method")

            choice = input("Enter 1 to for Fast Kuldorff, Enter 2 for Fast_Fast Subset Aggregation, Enter 3 for Fast Naive Subset Aggregation")

            if(choice == 1 ):
                print("Starting Fast Kuldorff")
                number_of_restarts = input("Enter Number of Restarts:")
                kul_instance = Kuldorff()
            # anomalous subset detection starts from the day of win_size
                for i in range(win_size-1,len(time_ids) ):
                    for j in range(0, len(location_ids)):
                        for k in range(0 , len(stream_ids)):
                             start_win = max(0,i-win_size)
                             counts[i][j][k]= counts[start_win:i+1,j,k].mean()
                    cur_counts = np.empty((len(location_ids), len(stream_ids)))
                    cur_baselines = np.empty((len(location_ids), len(stream_ids)))
                    #print counts
                    cur_counts = counts[i]
                    cur_baselines = baselines[i]
                    current_subset,selected_streams,f_record_sum_now=kul_instance.fast_kulldorff(cur_counts,cur_baselines,location_ids,stream_ids,p_0 ,q_max , number_of_restarts,distribution_type,type_of_test  )
                    sys.stdout = open("/Users/snehachalla/Desktop/FK_new.txt", "a")
                    print current_subset,selected_streams,f_record_sum_now

            if(choice ==2):
                print("Starting Fast Fast Subset Aggregation")
                type_of_distribution = raw_input("Enter the type of distribution of the data")
                test_type= raw_input("Enter the type of test on the data streams")
            # anomalous subset detection starts from the day of win_size
                for i in range(win_size-1,len(time_ids) ):
                    for j in range(0, len(location_ids)):
                        for k in range(0 , len(stream_ids)):
                             start_win = max(0,i-win_size)
                             counts[i][j][k]= counts[start_win:i+1,j,k].mean()
                             print counts
                             cur_counts = np.empty((len(location_ids) , len(stream_ids)))
                             cur_baselines = np.empty((len(location_ids) , len(stream_ids)))
                    cur_counts = counts[i]
                    cur_baselines = baselines[i]
                    SA = SubsetAggregation()
                    print "Printing current counts and baselines"
                    print cur_counts , cur_baselines
                    most_anamalous_location_ids,most_anamalous_stream_ids,most_anamalous_score =SA.fast_fast_SA(stream_ids, location_ids,cur_counts,cur_baselines,10,10,0.5,type_of_distribution, test_type)
                    sys.stdout = open("/Users/snehachalla/Desktop/SA_FF_new.txt", "w")
                    print most_anamalous_location_ids,most_anamalous_stream_ids,most_anamalous_score

            if(choice == 3):
                print("Starting Fast Naive Subset Aggregation")
                type_of_distribution = raw_input("Enter the type of distribution of the data")
                test_type= raw_input("Enter the type of test on the data streams")
            # anomalous subset detection starts from the day of win_size
                for i in range(win_size-1,len(time_ids) ):
                    for j in range(0, len(location_ids)):
                        for k in range(0 , len(stream_ids)):
                             start_win = max(0,i-win_size)
                             counts[i][j][k]= counts[start_win:i+1,j,k].mean()
                             cur_counts = np.empty((len(location_ids) , len(stream_ids)))
                             cur_baselines = np.empty((len(location_ids) , len(stream_ids)))
                    cur_counts = counts[i]
                    cur_baselines = baselines[i]
                    SA = SubsetAggregation()
                    most_anamalous_location_ids,most_anamalous_stream_ids,most_anamalous_score =SA.fast_naive_subset_aggregation(cur_counts,cur_baselines,stream_ids,location_ids,type_of_distribution,test_type  )
                    sys.stdout = open("/Users/snehachalla/Desktop/SA_FN_new.txt", "w")
                    print most_anamalous_location_ids,most_anamalous_stream_ids,most_anamalous_score


run_1 = Runner()
run_1.caller()

