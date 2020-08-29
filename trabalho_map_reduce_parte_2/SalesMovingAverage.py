from mrjob.job import MRJob
from mrjob.step import MRStep

class SalesMovingAverage(MRJob):
    length_window = 3
    def mapper(self, _, line):
        datetime, product, price, _, _, _, _, country, _, _, _, _ = line.split(',')
        date,_ = datetime.split()
        yield (country, product), (date, float(price))

    def reducer(self, key, values):
        items = list(values)
        items.sort()
        index = 0
        total = 0.0
        moving = 0.0

        for date, value in items:
            total += value
            if index >= self.length_window:
                total = total - items[index - self.length_window][1]
            divisor = min((index + 1), self.length_window)
            moving = total / divisor

            index+=1
            yield key, (date, moving)

if __name__ == "__main__":
    SalesMovingAverage.run()