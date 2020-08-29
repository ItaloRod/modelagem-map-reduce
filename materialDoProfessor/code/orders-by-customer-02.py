from mrjob.job import MRJob

class OrdersByCustomer(MRJob):
    def mapper(self, _, line):
        user, item, value = line.split(',')
        
        yield int(user), float(value)

    def reducer(self, key, values):
        items = list(values)

        yield key, (min(items), max(items), sum(items)/len(items))

if __name__ == "__main__":
    OrdersByCustomer.run()