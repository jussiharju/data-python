
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class LengthCounts(MRJob):
    
    OUTPUT_PROTOCOL = RawValueProtocol
    
    def mapper(self, _, line):
        import os
        # files are in the folder, thus the "\\"-split
        file_name = os.getenv('mapreduce_map_input_file').split("\\")[1]
        words = line.lower().split()
        for word in words:
            yield ((file_name,str(len(word))), 1)
            
    def combiner(self, key, counts_iter):
        yield (key[0], (key[1], sum(counts_iter)))
    
    
    def reducer(self, key, value):
        d = {"file": key}
        for v in value:
            if v[0] not in d.keys():
                d[v[0]] = v[1]
            else:
                d[v[0]] += v[1]
        yield (key, str(d))

if __name__ == '__main__':
    LengthCounts.run()
