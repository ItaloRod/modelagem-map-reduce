from mrjob.job import MRJob
from mrjob.step import MRStep
'''
    python SalesQuantity.py sales.csv > outputSalesQuantity.txt
'''

class SalesQuantity(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.adder,
            ),
            MRStep(
                reducer=self.reducer
            )
        ]

    def mapper(self, _, line):
        _, product, _, _, _, _, _, country, _, _, _, _ = line.split(',')
        yield (country, product), 1

    def adder(self, key, values):
        items = list(values)
        country = key[0]
        product = key[1]
        yield country, (product, sum(items))

    def reducer(self, key, values):
        items = list(values)
        yield  key, items

if __name__ == "__main__":
    SalesQuantity.run()