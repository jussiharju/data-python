from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class BookRatings(MRJob):
    
    OUTPUT_PROTOCOL = RawValueProtocol 
    
    def mapper(self, _, line):
        import os
        file_name = os.getenv('mapreduce_map_input_file')
        if "Ratings" in file_name:
            try:
                user_id = line.split(";")[0]
                user_id = user_id[1:len(user_id)-1]
                rating = line.split(";")[2]
                rating = int(rating[1:len(rating)-1])
                yield "r", (user_id, rating)
            except ValueError:
                pass
        elif "Users" in file_name:
            try:
                user_id = line.split(";")[0]
                user_id = user_id[1:len(user_id)-1]
                country = line.split(";")[1].split(",")
                country = country[len(country)-1]
                country = country[1:len(country)-1]
                yield "c", (user_id, country)
            except IndexError:
                pass
        
    def combiner(self, key, value):
        if key == "r":
            for v in value:
                yield None, v
        elif key == "c":
            users = {}
            for v in value:
                users[v[0]] = v[1]
            yield None, users
    
    def reducer(self, key, value):
        ratings = []
        users = {}
        for v in value:
            if type(v) is dict:
                users.update(v)
            else:
                ratings.append(v)
        countries = {}
        for i in range(len(ratings)):
            try:
                if users[ratings[i][0]] in countries:
                    countries[users[ratings[i][0]]].append(ratings[i][1])
                else:
                    countries[users[ratings[i][0]]] = [ratings[i][1]]
            except KeyError:
                pass
            
        for c in countries:
            yield None, "{};{};{}".format(c, len(countries[c]), round(sum(countries[c])/len(countries[c]),1))

if __name__ == '__main__':
    BookRatings.run()
