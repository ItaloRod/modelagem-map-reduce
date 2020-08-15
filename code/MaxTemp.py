from mrjob.job import MRJob

class MaxTemp(MRJob):
    def mapper(self, _, line):
        tokens = line.split(',')

        metric = tokens[2]
        if metric == 'TMAX' or metric == 'TMIN':
            yield tokens[0], float(tokens[3])
        
    def reducer(self, key, values):
        items = list(values)

        output = 'Min : {0}, Max: {1}'.format(min(items), max(items))

        yield key, output

if __name__ == "__main__":
    MaxTemp.run()