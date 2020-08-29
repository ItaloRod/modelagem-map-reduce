'''
    realiza a contagem baseada em combinações. Essas combinações podem ser feitas de duas ou três combinações baseado no dataset.
    python mba.py mba.csv

'''

from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
class MBA(MRJob):

    def get_combinations(self, items):
        result = []
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                a = items[i]
                b = items[j]
                result.append((a,b))
        return result

    def mapper(self, _, line):
        items = line.split(',')
        items.sort()
        # combs = self.get_combinations(items) # Forma do método acima
        combs = combinations(items, 2) # Forma utilizando o intertools combinations
        for combination in combs:
            yield combination, 1

    def combiner(self, key, values):
        yield key, sum(values)
    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == "__main__":
    MBA.run()