__author__ = 'snehachalla'

"""
This class contains the implementation for Fast optimization over subsets of locations and Fast optimization
over subsets of data streams (FF)
"""
import itertools
from Priority import Priority
from Scoring_Functions_New import ScoringFunctions
import numpy as np
import pandas as pd
import math as m
import random
import sys

class SubsetAggregation(object):

    """
    def generate_random_stream_ids(number_of_data_streams, p_0):
        p_m = 0
        indices=[]
        for i in range(0,number_of_data_streams):
            p_m = random.random()
            if(p_m > p_0):
                indices.append(i)
        return indices
    """

    def fast_fast_SA(self, stream_ids , location_ids, counts , baselines, num_of_restarts,num_iterations_per_restart, p_0, distribution_type, type_of_test):
        print  sys._getframe().f_code.co_name
        restart_count =1
        while(restart_count < num_of_restarts):
           print "RESTART NUMBER" + str(restart_count)
           p_m=0
           random_select_stream_indices =[]
           for i in range(0,len(stream_ids)):
               p_m = random.random()
               if(p_m > p_0):
                   random_select_stream_indices.append(i)
           most_anamalous_score = float("inf")
           score,cur_stream_ids,cur_location_ids = self.fast_fast_iterate_over_step1_step2(random_select_stream_indices, stream_ids, location_ids,counts, baselines,num_iterations_per_restart,distribution_type, type_of_test)
           if(score > most_anamalous_score):
               most_anamalous_score = score
               most_anamalous_stream_ids = cur_stream_ids
               most_anamalous_location_ids = cur_location_ids
               restart_count = restart_count + 1
        return most_anamalous_location_ids,most_anamalous_stream_ids,most_anamalous_score


    def fast_fast_iterate_over_step1_step2(self,random_select_stream_indices, stream_ids, location_ids, counts, baselines,num_iterations_per_restart, distribution_type, type_of_test):
        print  sys._getframe().f_code.co_name
        current_restart_most_anamalous_subset_score = float("inf")
        current_restart_most_anamalous_stream_subset = random_select_stream_indices
        current_restart_most_anamalous_location_subset = location_ids
        iteration_count = 1
        while iteration_count < num_iterations_per_restart:
            cur_anamalous_records, score = self.find_most_anamalous_records_given_streams(current_restart_most_anamalous_stream_subset,stream_ids, location_ids, counts,baselines,distribution_type, type_of_test)
            if (score < current_restart_most_anamalous_subset_score) :
                return current_restart_most_anamalous_stream_subset,current_restart_most_anamalous_location_subset,current_restart_most_anamalous_subset_score
            else :
                current_restart_most_anamalous_location_subset = cur_anamalous_records
                current_restart_most_anamalous_subset_score = score
            cur_anamalous_streams, score = self.find_most_anamalous_streams_given_records(current_restart_most_anamalous_location_subset,counts, baselines,stream_ids,location_ids,distribution_type, type_of_test)

            if(score < current_restart_most_anamalous_subset_score):
                return current_restart_most_anamalous_stream_subset,current_restart_most_anamalous_location_subset,current_restart_most_anamalous_subset_score
            else:
                current_restart_most_anamalous_subset_score = score
                current_restart_most_anamalous_stream_subset = cur_anamalous_streams

            iteration_count = iteration_count + 1
        print  sys._getframe().f_code.co_name
        return current_restart_most_anamalous_stream_subset,current_restart_most_anamalous_location_subset,current_restart_most_anamalous_subset_score

    def find_most_anamalous_records_given_streams(self,cur_anamalous_streams,stream_ids, location_ids, counts, baselines,distribution_type, type_of_test):
        print  sys._getframe().f_code.co_name
        aggregate_counts, aggregate_baselines = self.aggregate_counts_baselines_across_streams(cur_anamalous_streams, stream_ids,location_ids,counts,baselines)
        pr = Priority()
        priorities_locations = pr.calculate_priority(aggregate_counts,aggregate_baselines,distribution_type, type_of_test)

        priorities_locations_df = pd.DataFrame(priorities_locations)
        location_ids_df = pd.DataFrame(location_ids)
        aggregate_counts_df = pd.DataFrame(aggregate_counts)
        aggregate_baselines_df = pd.DataFrame(aggregate_baselines)
        frame = [location_ids_df,priorities_locations_df,aggregate_counts_df,aggregate_baselines_df]
        final_frame = pd.concat(frame , axis =1 )
        #print final_frame
        final_frame.columns = ['locations' , 'priorities' , 'counts', 'baselines']
        final_frame= final_frame.sort(['priorities'], ascending=[False])
        print final_frame
        max_f_score = None
        max_score_index = None
        counts= np.array(final_frame['counts'])
        baselines = np.array(final_frame['baselines'])
        locations = np.array(final_frame['locations'])
        priorities = np.array(final_frame['priorities'])
        print counts,baselines,locations,priorities
      # This for loop scans over all the subsets containing top-j priority records and compute their score F(S)

        for i in range(1,len(final_frame)):
            sfn=ScoringFunctions()
            f_score_i = sfn.f_score_statistic_subset_aggregation(counts[0:i], baselines[0:i], distribution_type, type_of_test)
            print "Fscore per records subset for a set of streams"
            print f_score_i
            if max_f_score == None:
                max_f_score =  f_score_i
                max_score_index = i
            elif max_f_score < f_score_i :
                 max_f_score =  f_score_i
                 max_score_index = i
            else:
                 max_f_score = max_f_score
                 max_score_index = max_score_index

        most_anamalous_subset = []
        for i in range(0, max_score_index):
                print final_frame['locations'][i]
                most_anamalous_subset.append(final_frame['locations'][i])
        print "anamalous records"
        print most_anamalous_subset
        return most_anamalous_subset,max_f_score


    def find_most_anamalous_streams_given_records(self, cur_anamalous_locations ,counts,baselines,stream_ids,location_ids,distribution_type, type_of_test):
        print  sys._getframe().f_code.co_name
        stream_counts , stream_baselines = self.aggregate_counts_baselines_over_the_records(cur_anamalous_locations,stream_ids,location_ids,counts,baselines)
        pr=Priority()
        priorities_streams = pr.calculate_priority(stream_counts,stream_baselines,distribution_type, type_of_test)
        priorities_streams_df = pd.DataFrame(priorities_streams)
        stream_ids_df = pd.DataFrame(stream_ids)
        stream_counts_df = pd.DataFrame(stream_counts)
        stream_baselines_df = pd.DataFrame(stream_baselines)
        frame = [stream_ids_df,priorities_streams_df,stream_counts_df,stream_baselines_df]
        final_frame = pd.concat(frame , axis =1 )
        final_frame.columns = ['stream_ids' , 'priorities' , 'stream_counts','stream_baselines']
        final_frame= final_frame.sort(['priorities'], ascending=[False])
        counts= np.array(final_frame['stream_counts'])
        baselines = np.array(final_frame['stream_baselines'])
        streams = np.array(final_frame['stream_ids'])
        priorities = np.array(final_frame['priorities'])
        max_f_score = None
        max_score_index = None

        # This for loop scans over all the subsets containing top-j priority records and compute their score F(S)
        for i in range(0,len(final_frame)):
            sfn=ScoringFunctions()
            f_score_i = sfn.f_score_statistic(counts[0:i], baselines[0:i], distribution_type, type_of_test)
            if max_f_score == None:
                max_f_score =  f_score_i
                max_score_index = i
            elif max_f_score < f_score_i :
                 max_f_score =  f_score_i
                 max_score_index = i
            else:
                 max_f_score = max_f_score
                 max_score_index = max_score_index

        most_anamalous_subset = []
        for i in range(0, max_score_index):
                most_anamalous_subset.append(final_frame['stream_ids'][i])
        print "Most anamalous streams"
        print most_anamalous_subset
        return most_anamalous_subset,max_f_score


    def aggregate_counts_baselines_across_streams(self,cur_anamalous_streams, stream_ids,location_ids,counts,baselines):
        print  sys._getframe().f_code.co_name
        aggregate_counts = np.empty(len(location_ids))
        aggregate_baselines = np.empty(len(location_ids))

        for i in range(0, len(location_ids)):
            for j in cur_anamalous_streams :
                aggregate_counts[i] = aggregate_counts[i] + counts[i][j]
                aggregate_baselines[i] = aggregate_baselines[i] + baselines[i][j]
        return aggregate_counts , aggregate_baselines


    def aggregate_counts_baselines_over_the_records(self,cur_anamalous_locations,stream_ids,location_ids,counts,baselines):
        print  sys._getframe().f_code.co_name
        stream_counts = np.empty(len(stream_ids))
        stream_baselines = np.empty(len(stream_ids))
        cur_anamalous_locations_indices = []

        for k in range(0, len(cur_anamalous_locations)):
            cur_anamalous_locations_indices.append(location_ids.index(cur_anamalous_locations[k]))

        for i in range(0, len(stream_ids)):
            for j in cur_anamalous_locations_indices:
                stream_counts[i] = stream_counts[i] + counts[j][i]
                stream_baselines[i] = stream_baselines[i] + baselines[j][i]

        return stream_counts,stream_baselines


    def fast_naive_subset_aggregation(self,counts, baselines, stream_ids, location_ids,distribution_type,type_of_test):
        print  sys._getframe().f_code.co_name
        number_of_datastream_subsets = 2** len(stream_ids)  # Store number of elements in the power set
        priority = np.empty(len(location_ids))  #Initialize priority 1-D array to store priorities of each of the records for the subset of data streams being considered
        counts_temp = np.empty(len(location_ids)) # This holds the counts values for each of the records in the current subset being considered
        baselines_temp = np.empty(len(location_ids))# This holds the baseline values for each of the records in the current subset being considered

        most_anamalous_subset_records =[] # This list stores the
        most_anamalous_score_across_streams = None
        most_anamalous_subset_stream_ids =[]

        #Looping over all the (2**N-1) available subsets of data streams starting with subsets of size 1
        for L in range(1, len(stream_ids)+1):
            for subset in itertools.combinations(stream_ids, L):  #works in IPython 2.6 and above.
                 current_stream_ids = []  #This list Stores the stream ids of the current subset
                 current_stream_ids = subset
                 indexes = []  # This is to hold the indexes of the stream ids in the current subset
                 # Looping over the streams present in the current subset
                 print "Current stream subset"
                 print subset
                 for d_m in current_stream_ids :
                     indexes.append(d_m.index(d_m))
                 #Find most anamalous subset of records for the current set of streams
                 cur_anamalous_rec_subset,max_f_score= self.find_most_anamalous_records_given_streams(indexes,stream_ids, location_ids, counts, baselines,distribution_type, type_of_test)
            if(most_anamalous_score_across_streams == None) or (most_anamalous_score_across_streams < max_f_score) :
                    most_anamalous_score_across_streams = max_f_score
                    most_anamalous_subset_records = cur_anamalous_rec_subset
                    most_anamalous_subset_stream_ids = subset
        return most_anamalous_subset_records,most_anamalous_subset_stream_ids,most_anamalous_score_across_streams






















