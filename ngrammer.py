from mrcc import CCJob
from bs4 import BeautifulSoup as BS
import nltk
from collections import Counter
from itertools import chain
from html2text import html2text
import re
from string import translate,maketrans,punctuation
from itertools import chain
from nltk import PunktSentenceTokenizer
from textblob import TextBlob
from string import translate,maketrans,punctuation
from itertools import chain


###helper funcitons
tokenizer = WhitespaceTokenizer()
tknr = PunktSentenceTokenizer()

def delimsplit(string):
	fields = re.split('([,\.;])',string)
	values = fields[::2]
	delimiters = fields[1::2]+['']
	return ' '.join(v+d for v,d in zip(values,delimiters))

def splitonnth(string):  
	occur = 2  # on which occourence you want to split
	indices = [x.start() for x in re.finditer("_", string)]
	part1 = string[0:indices[occur-1]]
	part2 = string[indices[occur-1]+1:]
	return part1+ ' '+ part2

def wordSplitter(word):
#checks if all the letters are capitalized; likely an abbreviation
	if word.upper()==word:
		return word
	elif re.search("[,\.;]",word) is not None:
		return delimsplit(word)
	#elif len(re.split(r'[A-Z]',word))>2:
		#n = 2
		#groups = re.split(r'([A-Z])',word)
		#'_'.join(groups[:n]), '_'.join(groups[n:])
	else:
		return ' '.join(re.sub(r"([A-Z])",r" \1",word).split())

def textFixer(raw_text):
	html_conv = BS(raw_text).get_text()
	words = html_conv.split()
	return ' '.join(filter(None,[wordSplitter(x) for x in words]))

def log(msg):
	print("{} {}".format(str(datetime.datetime.now()), msg))

def removeNonAscii(s): 
	return "".join(filter(lambda x: ord(x)<128, s))
	
punctuation = punctuation.replace('-','')
#makes a C translation dictionary converting punctuations to white spaces
Trans = maketrans(punctuation, ' '*len(punctuation))

def ngrammer(l,n):
	temp = [" ".join(l[i:i+n]) for i in xrange(0,len(l)) if len(l[i:i+n])==n]
	#temp.extend([i+"s" if i[-1]!='s' and i.split()[-1] not in Stopwords else i[0:-1] for i in temp])
	yield temp

def text_transformer(text):
	#nouns list from text blob
	#TextBlobnouns=[x.encode('utf-8') for x in multi_blobber(text)]
	text = removeNonAscii(text.lower())
	sentences = tknr.tokenize(text)
	# removes everything bad, and splits into words
	cleaned_words = [list(translate(sentence,Trans).split()) for sentence in sentences]
	#splits sentences into 2-5 word ngrams
	two_five_ngrams = [ngrammer(sent,num) for num in [2,3,4,5] for sent in cleaned_words]
	#removes stopwords from singlewords
	#good_singles = list(set(chain(*cleaned_words)).difference(Stopwords))
	#ngrams = list(chain(*[list(chain(*list(x))) for x in two_five_ngrams]))
	#ngrams=[" ".join(ngram) for ngram in list(chain(*[list(chain(*list(x))) for x in two_five_ngrams]))] +good_singles
	#correctly pluralizes and singularizes the words in the text, spss->spsss->sps, i'm on with the
	#created noise on things like this, because the incorrect words will not be found in raw ngrammed object text
	#pls=[pluralize(i) for i in ngrams]
	#sings=[singularize(i) for i in ngrams]
	#keep as is for tfidf
	#treated_ngrams=pls+sings+ngrams
	#skills_extracted = list(set(skills).intersection(treated_ngrams))
	#tfidf representation of wiki and Linkedin
	#tfidf_ready=[x for x in treated_ngrams if x in skills_extracted]
	#tfidf_for both
	#ensemble=TextBlobnouns+skills_extracted
	#ensemble=TextBlobnouns+tfidf_ready
	#ensemble=",".join(ensemble)
	yield two_five_ngrams
	#return ensemble+','

class WordCount(CCJob):
  def process_record(self, record):
    if record['Content-Type'] != 'text/plain':
      return
    data = record.payload.read()
    #for word in data.split():
    #  yield word, 1
    #for word, count in Counter(data.split()).iteritems():
      #yield word, 1
    return text_transformer(data)
    self.increment_counter('commoncrawl', 'processed_pages', 1)

if __name__ == '__main__':
  WordCount.run()
