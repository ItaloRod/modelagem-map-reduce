from mrjob.job import MRJob
from mrjob.step import MRStep

class Recomender(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper1,
                reducer=self.reducer1
            ),
            MRStep(
                mapper=self.mapper2,
                reducer=self.reducer2
            )
        ]

    def mapper1(self, _, line):
        user, item = line.split(',')
        yield user, item

    def reducer1(self, key, values):
        items = list(values)
        yield key, items

    def mapper2(self, key, items):
        for item in items:
            maps={}
            for i in items:
                if item != i:
                    if i not in maps:
                        maps[i] = 0
                    maps[i] = 1
            yield item, maps

    def reducer2(self, key, values):
        items = list(values)
        final = {}
        for m in items:
            for k, v in m.items():
                if k not in final:
                    final[k] = 0
                final[k] = final[k] + int(v)
        yield key, final

if __name__ == "__main__":
    Recomender.run()