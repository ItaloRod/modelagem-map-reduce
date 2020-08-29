'''
    content-based recommendation
'''
from mrjob.job import MRJob
from mrjob.step import MRStep
from math import *

class CBR (MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper1,
                reducer=self.reducer1
            ),
            MRStep(
                mapper=self.mapper2,
                reducer=self.reducer2
            ),
            MRStep(
                mapper=self.mapper3,
                reducer=self.reducer3
            ),
            MRStep(
                mapper=self.mapper4,
                reducer=self.reducer4
            )
        ]

    def mapper1(self, _, line):
        user, movie, rating, timestamp = line.split()
        yield movie, (user, int(rating))

    def reducer1(self, movie, values):
        items = list(values)
        number_of_raters = len(items)

        for item in items:
            user, rating = item
            yield user, (movie, rating, number_of_raters)

    def mapper2(self, user, value):
        (movie, rating, number_of_raters) = value
        yield user, (movie, rating, number_of_raters)
        # yield user, value

    def reducer2(self, user, values):
        items = list(values)

        combinations = self.get_combinations(items)
        for comb in combinations:
            reducer_key = comb[0][0], comb[1][0]

            rating1 =  comb[0][1]
            rating2 =  comb[1][1]

            number_raters1 = comb[0][2]
            number_raters2 = comb[1][2]

            rating_product = rating1 * rating2

            rating1_square = rating1**2
            rating2_square = rating2**2

            reducer_value = (rating1, number_raters1, rating2, number_raters2, rating_product, rating1_square, rating2_square)

            yield reducer_key, reducer_value

    def mapper3(self, key, value):
        movie1, movie2 = key
        yield (movie1, movie2), value

    def reducer3(self, key, values):
        items = list(values)

        rating_sum1 = 0
        rating_sum2 = 0
        rating_norm_sq1 = 0
        rating_norm_sq2 = 0

        for item in items:
            rating_sum1 += item[0]
            rating_sum2 += item[2]
            rating_norm_sq1 += item[5]
            rating_norm_sq2 += item[6]

        features1 = [rating_sum1, rating_norm_sq1]
        features2 = [rating_sum2, rating_norm_sq2]

        similarity = self.calculate_euclidian(features1, features2)
        yield key, similarity

    def mapper4(self, key, value):
        movie1, movie2 = key
        yield movie1, (movie2, value)

    def reducer4(self, key, values):
        items = list(values)
        items.sort(key=lambda x : x[1], reverse=True)
        yield key, items

    def calculate_euclidian(self, features1, features2):
        result = sqrt(sum(pow(a-b, 2) for a, b in zip(features1, features2)))

        return 1 / (1 + result)

    def get_combinations(self,items):
        result = []
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                m1 = items[i]
                m2 = items[j]

                result.append((m1,m2))
        return result

if __name__ == "__main__":
    CBR.run()