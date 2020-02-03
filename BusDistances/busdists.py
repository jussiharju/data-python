
from mrjob.job import MRJob
from math import radians, cos, sin, asin, sqrt

def haversine(p1, p2):  # p1, p2 coordinate points of form (latitude, longitude).
    # Convert decimal degrees to radians (also ensures values are floats).
    pr1 = (radians(float(p1[0])), radians(float(p1[1])))
    pr2 = (radians(float(p2[0])), radians(float(p2[1])))

    # Haversine formula
    dlat = pr2[0] - pr1[0]   # Difference of latitudes.
    dlon = pr2[1] - pr1[1]   # Difference of longitudes.
    a = sin(dlat/2)**2 + cos(pr1[0]) * cos(pr2[0]) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers.
    return c * r

class BusDistances(MRJob):
    
#     def mapper_init(self):
#         for i in range(24):
#             yield ("{:02d}".format(i), 0)
    
    def mapper(self, _, line):
        import os
        file_name = os.getenv('mapreduce_map_input_file')
        key = file_name[-6:-4]
        stats = line.split(",")
        try:
            bus = stats[9]
            lon = float(stats[4])
            lat = float(stats[5])
            time = stats[0]
            yield key, (bus, time, (lat,lon))
        except:
            pass
       
            
    def combiner(self, key, value):
        buses = {}
        for v in value:
            if v[0] not in buses.keys():
                buses[v[0]] = [(v[1], v[2])]
            else:
                buses[v[0]].append((v[1], v[2]))
        yield key, buses
    
    def reducer(self, key, value):
        for buses in value:
            dist = 0
            for bus in buses.values():
                for i in range(len(bus)-1):
                    dist += haversine(bus[i][1],bus[i+1][1])
        yield key, dist
        

if __name__ == '__main__':
    BusDistances.run()
