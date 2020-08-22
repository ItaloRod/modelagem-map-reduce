'''
    topN mostra os 10 maiores de uma lista ordenada
    python topn.py cats.txt > output.txt
'''

from mrjob.job import MRJob
from mrjob.step import MRStep

class TopN(MRJob):
    # atibutos da classe
    top = []
    N = 10

    def steps (self):
        # Para que o reducer_init faça efeito, ele deve estar no mesmo step que o mapper e o reducer isoladamente.
        return [
            MRStep(
                mapper=self.mapper,
                reducer_init=self.reducer_init,
            ),
            MRStep(
                reducer=self.reducer
            )
        ]
    def mapper(self, _, line):
        weight, id, name = line.split(',')
        self.top.append((float(weight), line)) # Guarda uma tupla contendo o parâmetro quantidade que o TopN irá validar, com ele guarda-se a linha também.
        if len(self.top) > self.N: # Caso o tamanho da lista seja muito grande, ele remove o primeiro que ordenado é o menor valor.
            self.top.sort()
            self.top.pop(0)
    def reducer_init(self):
        for item in self.top: # Agrupa todos os itens da lista para poder realizar o reducer. é um job pré-reducer
            yield None, item

    def reducer(self, _, values):
        # Reduz a lista para mostrar os valores corretos
        items = list(values)
        items.sort(reverse=True)

        for key, value in items:
            yield key, value

if __name__ == "__main__":
    TopN.run()

