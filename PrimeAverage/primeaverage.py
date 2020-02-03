from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class PrimeAverage(MRJob):
    
    OUTPUT_PROTOCOL = RawValueProtocol 

    def mapper(self, _, line):
        lines = line.split()
        try:
            for line in lines:
                yield (1, int(line))
        except:
            pass
        
    def combiner(self, key, counts_iter):
        yield (key, sum(counts_iter))
    
    def reducer(self, key, counts_iter):
        yield (key, str(sum(counts_iter)/1000000))

if __name__ == '__main__':
    PrimeAverage.run()
