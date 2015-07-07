__author__ = 'snehachalla'

"""
This Class currently implements the Fast Kuldorff's method .

Reference: http://www.cs.cmu.edu/~neill/papers/StatMed2013.pdf

"""

"""

FK, FN , FF

"""
import random
import math
print random.random()
import numpy as np
from Scoring_Functions_New import ScoringFunctions

class Kuldorff(object):

     def fast_kulldorff(self, counts , baselines,location_ids,stream_ids,p_0 ,q_max , number_of_restarts,distribution_type,type_of_test  ):
            """
            :param number_of_streams: count representing number of data streams in the dataset.
            :param P_0: cutoff value only. All the streams with P_m > P_0 have Q_m between [1,Qmax] . All other streams have Q_m = 1
            :param stream_ids: A list containing all the stream ids . Minimum length of 1 and maximum size of number_of_streams
            :param Q_max : Max value of Q_m possible for the selected streams.
            :param counts: counts is 2 d numpy array containing counts for each of the location and stream ids
            :param baselines : baselines is a 2-D numpy array containing baselines for each of the locations and stream ids.
            :param number_of_restarts: Number of random restarts to be performed
            :param distribution_type list containing the distribution types of the streams
            :param type_of_test list containing type of test to be performed for each of the streams
            :return:
            """

            restart_count = 1
            number_of_streams = len(stream_ids)
            sfn= ScoringFunctions()
            while  restart_count < number_of_restarts:
                p_m = []
                q_m = []  #Q_m holds the values of the risk function for each of the datastreams.
                for i in range(0,number_of_streams):
                    p_m.append(random.random())

                #Now P_m holds the list of probabilities for each of the streams.
                # The following block for the selected streams
                selected_streams = []
                for i in range(0, number_of_streams):
                    if p_m[i] > p_0 :
                        selected_streams.append(stream_ids[i])
                        q_m.append(random.uniform(1, q_max))
                    else:
                        q_m.append(1)

                num_records = len(counts)
                f_record = np.empty(num_records)
                current_subset = []
                for i in range(0, num_records):
                    for j in range(0, number_of_streams):
                        if q_m[j] >0 :
                            #print counts[i][j]
                            #print baselines[i][j]
                            f_record[i] = f_record[i]+ sfn.score_statistic_per_rec_Kuldorff(counts,baselines,i,j,distribution_type[j],type_of_test[j])
                    if f_record[i]>0:
                        current_subset.append( location_ids[i])

                f_record_total = sum(f_record)

                counts_m=np.empty(number_of_streams)
                baselines_m =np.empty(number_of_streams)
                for i in range(0,number_of_streams):
                    for j in range(0, len(current_subset)):
                         k= location_ids.index(current_subset[j])
                         counts_m[i] = counts_m[i]+ counts[k][i]
                         baselines_m[i] = baselines_m[i]+ baselines[k][i]
                    q_m[i]= counts_m[i]/baselines_m[i]


                for i in range(0, num_records):
                    for j in range(0, number_of_streams):
                         if q_m[j] >0 :
                             f_record[i] = f_record[i]+ sfn.score_statistic_per_rec_Kuldorff(counts,baselines,i,j,distribution_type[j],type_of_test[j])

                f_record_sum_now = sum(f_record)
                f_record_sum_now = round(f_record_sum_now,2)
                if f_record_sum_now> f_record_total:
                        restart_count += 1
                else:
                        restart_count = number_of_restarts


            return current_subset,selected_streams,f_record_sum_now














































