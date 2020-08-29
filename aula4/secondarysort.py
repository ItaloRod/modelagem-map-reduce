'''
    ordenação secundaria. Ao inves de gastar processamento solicitando a ordenação, passamos uma chave secundária com  o elemento que desejo ordenar.

'''

from mrjob.job import MRJob
from mrjob.step import MRStep

class MovingAverage(MRJob):
    length_window = 3
    def step(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.sort
            ),
            MRStep(
                reducer=self.reducer
            )
        ]

    def mapper(self, _, line):
        stock, timestamp, value = line.split(',')
        yield (stock, timestamp), float(value)

    def sort(self, key, values):
        items = list(values)
        yield key[0], (items[0], key[1])

    def reducer(self, key, values):
        items = list(values)
        index = 0
        total = 0.0
        moving = 0.0

        for timestamp, value in items:
            total += value
            if index >= self.length_window:
                total = total - items[index - self.length_window][1]
            divisor = min((index + 1), self.length_window)
            moving = total / divisor

            index+=1
            yield key, (timestamp, moving)

if __name__ == "__main__":
    MovingAverage.run()