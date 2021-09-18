# Faz a contagem de palavras e traz a que mais aparece.

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# regex para considerar apenas palavras
REGEX_ONLY_WORDS = re.compile(r"[\w']+")


class MRPalavraMaisUsada(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words, combiner=self.combiner_count_words, reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]


    def mapper_get_words(self, _, line):
        for word in REGEX_ONLY_WORDS.findall(line):
            yield (word.lower(), 1)

    def combiner_count_words(self, word, counts):
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word)

    def reducer_find_max_word(self, _, word_count_pairs):
        yield max(word_count_pairs)


if __name__ == '__main__':
    MRPalavraMaisUsada.run()