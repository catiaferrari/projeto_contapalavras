# Faz a contagem de palavras ordenando de forma decrescente de ocorrências.

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# regex para considerar apenas palavras
REGEX_ONLY_WORDS = re.compile(r"[\w']+")


class MRContaPalavras(MRJob):
  
    def steps(self):
        return [
            MRStep(mapper = self.mapper_get_words, combiner=self.combiner_count_words, reducer = self.reducer_count_words),
            MRStep(reducer = self.reducer_output_words)
        ]


    def mapper_get_words(self, _, line):
        for word in REGEX_ONLY_WORDS.findall(line):
            yield (word.lower(), 1)

    def combiner_count_words(self, word, counts):
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word) 

    def reducer_output_words(self, _, word_counts):
        for count, word in sorted(word_counts, reverse=True):
            yield ('%04d'%int(count), word)


if __name__ == '__main__':
    MRContaPalavras.run()