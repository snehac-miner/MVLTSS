__author__ = 'snehachalla'

import numpy as np


class DataParsers(object):

    def prepare_data_counts(self,counts, Wmax):

        if not isinstance(counts, np.ndarray):
            raise TypeError("counts should be an numpy.ndarray, but found %s" % type(counts))
            if not counts.ndim == 3:
             raise ValueError("counts should be a 3D numpy array, instead found %d dimensions" % counts.ndim)

        num_time_steps, num_locations, num_streams = counts.shape

        for time in xrange(1, num_time_steps):
            for location in xrange(num_locations):
                for stream in xrange(num_streams):
                    start_win = max(0, time - Wmax)
                    counts[time][location][stream] = counts[start_win:time, location, stream].mean()

        return counts


    def generate_moving_average_baselines(self,counts, window_size , baseline_at_t_zero =-1 ):

         if not isinstance(counts, np.ndarray):
            raise TypeError("counts should be an numpy.ndarray, but found %s" % type(counts))
            if not counts.ndim == 3:
               raise ValueError("counts should be a 3D numpy array, instead found %d dimensions" % counts.ndim)


         num_time_steps, num_locations, num_streams = counts.shape
         print "counts shape"
         print counts.shape

         if window_size is None:
            window_size = num_time_steps
         elif not isinstance(window_size, int):
        # validate user provided window size
            raise TypeError("Window_size should be an int, but found %s" % type(window_size))
         elif not window_size > 0:
        # validate user provided window size
            raise ValueError("Window size must be greater than 0")

         # create an empty array
         baselines = np.empty(counts.shape, dtype=float)

            # fill baseline at t=0
         for location in xrange(num_locations):
            for stream in xrange(num_streams):
                baselines[0][location][stream] = baseline_at_t_zero

         for time in xrange(1, num_time_steps):
            for location in xrange(num_locations):
                for stream in xrange(num_streams):
                    start_win = max(0, time - window_size)
                    baselines[time][location][stream] = counts[start_win:time+1, location, stream].mean()
         print "baseline shape"
         print baselines.shape
         return baselines





