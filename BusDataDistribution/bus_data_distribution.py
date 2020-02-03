from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class BusDataDistribution(MRJob):
    
    OUTPUT_PROTOCOL = RawValueProtocol 
    
    def mapper_init(self):
        for i in range(24):
            yield (i, 0)
    
    def mapper(self, _, line):
        lines = line.split(";")[1].split(":")
        try:
            yield (int(lines[0]), 1)
        except:
            pass
        
    
    def reducer(self, hour, counts_iter):
        yield (hour, "{};{}".format(hour, sum(counts_iter)))

if __name__ == '__main__':
    BusDataDistribution.run()
