from mrjob.job import MRJob
from mrjob.step import MRStep
'''
 python marveljob.py Marvel-graph.txt > output.txt
'''
class MarvelFriends(MRJob):
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
        marvel_friendList = line.split()
        hero = int(marvel_friendList[0])
        friends = marvel_friendList[1:]
        yield hero, len(friends)

    def reducer(self, key, values):
        yield None, (sum(values), key)

    def sort(self, _, friends):
        for friends_count, key in sorted(friends, reverse=True):
            yield key, friends_count

if __name__ == "__main__":
    MarvelFriends.run()