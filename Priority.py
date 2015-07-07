__author__ = 'snehachalla'

import numpy as np

class Priority(object):

    def calculate_priority(self,aggregate_counts,aggregate_baselines,distribution_type, type_of_test):

        list_distribution_types = ["GAUSSIAN" , "gaussian" , "POISSON" ,"poisson" , "P" , "G"]
        list_type_of_test = ["POSITIVE" , "NEGATIVE","positive" , "negative" , "pos" , "neg"]

        if distribution_type is None:
            raise ValueError("Data distribution type unspecified")
        elif distribution_type not in list_distribution_types:
             raise ValueError("Not a valid Data distribution type ")

        if type_of_test is None:
            raise ValueError("Type of test is unspecified")
        elif type_of_test not in list_type_of_test:
             raise ValueError("Not a valid test type ")

        if len(aggregate_counts)==0 :
            raise ValueError("Aggregate counts passed to the priority function is empty")
        if len(aggregate_baselines)==0  :
            raise ValueError("Aggregate Baselines passed to the priority function is empty")


        priority = np.empty(len(aggregate_counts))

        if (distribution_type == "POISSON" and type_of_test == "POSITIVE"):

            for i in range(0,len(aggregate_counts)):
                priority[i] = aggregate_counts[i] / aggregate_baselines[i]
            return priority
        if (distribution_type == "POISSON" and type_of_test == "NEGATIVE"):

            for i in range(0,len(aggregate_baselines)):
                priority[i] = aggregate_baselines[i] / aggregate_counts[i]
            return priority
        if (distribution_type == "GUASSIAN" and type_of_test == "POSITIVE"):

            for i in range(0,len(aggregate_counts)):
                priority[i] = aggregate_counts[i] / aggregate_baselines[i]
            return priority

        if (distribution_type == "GUASSIAN" and type_of_test == "NEGATIVE"):

            for i in range(0,len(aggregate_counts)):
                priority[i] = aggregate_counts[i] / aggregate_baselines[i]
            return priority



  













