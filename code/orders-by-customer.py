from mrjob.job import MRJob

class OrdersByCustomer(MRJob):
    def mapper(self, _, line):
        user, _, value = line.split(',')
        yield int(user), float(value)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == "__main__":
    OrdersByCustomer.run()