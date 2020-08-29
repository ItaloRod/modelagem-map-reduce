'''
Utilizada sempre quando há  recortes temporais
ele sempre é tratado da seguinte forma (k,t,v) - Key, time e value
para poder fazer a média móvel, preciso saber da janela de elementos que farão parte da média.

O tamanho da janela depende do negócio e ela informa quantos valores vão fazer parte da média. Se a janela for de 3, a média será de 3 números por 3.

    python movingAverage.py moving-average.csv

'''

from mrjob.job import MRJob
from mrjob.step import MRStep

class MovingAverage(MRJob):
    length_window = 3
    def step(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            )
        ]

    def mapper(self, _, line):
        stock, timestamp, value = line.split(',')
        yield stock, (timestamp, float(value))

    def reducer(self, key, values):
        items = list(values)
        items.sort()
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