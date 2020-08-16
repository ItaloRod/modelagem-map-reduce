'''
    python moviesjob.py movies.txt > output.txt
'''
from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviesPopularity(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            ),
            MRStep(
                reducer=self.sort
            )
        ]

    def mapper(self, _, line):
        _, movie_id, rating, _ = line.split()
        yield int(movie_id), int(rating)

    def reducer(self, key, values):
        items = list(values)
        avg = sum(items)/len(items)
        yield None, (avg, key)

    def sort(self, _, movies):
        for avg, key in sorted(movies):
            yield key, avg

if __name__ == "__main__":
    MoviesPopularity.run()