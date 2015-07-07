__author__ = 'snehachalla'

import pandas as pd
import numpy
import math

class ScoringFunctions(object):
     # for C>B

    def f_score_statistic_subset_aggregation(self,counts , baselines , distribution_type,type_of_test):
            """

            :param counts: counts is a series object containing final_frame['counts']
            :param baselines: baselines is a series object containing final_frame['baselines']
            :param type_of_test:  Indicates if it is a one-sided positive test(C>B) or a one-sided negative test(C<B)
            :return:
            """
            sum_of_counts = 0
            sum_of_baselines = 0
            for i in range(0,len(counts)):
                sum_of_counts = sum_of_counts + counts[i]
                sum_of_baselines = sum_of_baselines + baselines[i]

            if (distribution_type == "POISSON" and type_of_test == "POSITIVE") :

                if sum_of_counts > sum_of_baselines and ((sum_of_counts * sum_of_baselines)>0):
                         return sum_of_counts* math.log(sum_of_counts/sum_of_baselines) + (sum_of_baselines-sum_of_counts)
                else:
                         return 0

            if (distribution_type == "POISSON" and type_of_test == "NEGATIVE") :
                if (sum_of_counts <  sum_of_baselines) and ((sum_of_counts * sum_of_baselines)>0) :
                        return sum_of_counts* math.log(sum_of_baselines/sum_of_counts) + (sum_of_counts-sum_of_baselines)
                else:
                    return 0

    def score_statistic_per_rec_Kuldorff(self,counts,baselines,i,j,distribution_type,type_of_test ):

            """

            :param counts:
            :param baselines:
            :param i:
            :param j:
            :param distribution_type:
            :param type_of_test:
            :return:
            """


            if (distribution_type == "POISSON" and type_of_test == "POSITIVE") :

                if counts[i][j] > baselines[i][j] and ((counts[i][j] * baselines[i][j])>0):
                         return counts[i][j]* math.log(counts[i][j]/baselines[i][j]) + (baselines[i][j]-counts[i][j])
                else:
                         return 0

            if counts[i][j] > baselines[i][j] and ((counts[i][j] * baselines[i][j])>0):
                if (counts[i][j] <  baselines[i][j]) and ((counts[i][j] * baselines[i][j])>0) :
                        return counts[i][j]* math.log(baselines[i][j]/counts[i][j]) + (counts[i][j]-baselines[i][j])
                else:
                        return 0
            if (distribution_type == "GAUSSIAN" and type_of_test == "POSITIVE") :

                if counts[i][j] > baselines[i][j] :
                         return (counts[i][j] - baselines[i][j])**2 / 2*baselines[i][j]
                else:
                         return 0

            if (distribution_type == "GAUSSIAN" and type_of_test == "NEGATIVE") :
                if counts[i][j] > baselines[i][j] :
                         return (counts[i][j] - baselines[i][j])**2 / 2*counts[i][j]
                else:
                         return 0
































