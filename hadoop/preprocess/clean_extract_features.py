import langid
from nltk.stem import WordNetLemmatizer, LancasterStemmer
from nltk import pos_tag, word_tokenize
import string
from pyspark.sql.types import *
import re
from nltk.corpus import stopwords
from pyspark.sql import HiveContext
from pyspark.sql.functions import udf
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

hsC = HiveContext(sc)

# Use langid module to classify the language to make sure we are applying the correct cleanup actions
# https://github.com/saffsd/langid.py
def check_lang(data_str):
    predict_lang = langid.classify(data_str)
    if predict_lang[1] >= .9:
        language = predict_lang[0]
    else:
        language = 'NA'
    return language

hsC.registerFunction("check_lang", lambda x: check_lang(x), StringType())

# Stop words usually refer to the most common words in a language, there is no single universal list of stop words used
# by all natural language processing tools.
# Reduces Dimensionality
# removes stop words of a single Tweets (cleaned_str/row/document)
def remove_stops(data_str):
    # expects a string
    stops = set(stopwords.words("english"))
    list_pos = 0
    cleaned_str = ''
    text = data_str.split()
    for word in text:
        if word not in stops:
            # rebuild cleaned_str
            if list_pos == 0:
                cleaned_str = word
            else:
                cleaned_str = cleaned_str + ' ' + word
            list_pos += 1
    return cleaned_str

hsC.registerFunction("remove_stops", lambda x: remove_stops(x), StringType())


# catch-all to remove other 'words' that I felt didn't add a lot of value
# Reduces Dimensionality, gets rid of a lot of unique urls
def remove_features(data_str):
    # compile regex
    url_re = re.compile('https?://(www.)?\w+\.\w+(/\w+)*/?')
    punc_re = re.compile('[%s]' % re.escape(string.punctuation))
    num_re = re.compile('(\\d+)')
    mention_re = re.compile('@(\w+)')
    alpha_num_re = re.compile("^[a-z0-9_.]+$")
    # convert to lowercase
    data_str = data_str.lower()
    # remove hyperlinks
    data_str = url_re.sub(' ', data_str)
    # remove @mentions
    data_str = mention_re.sub(' ', data_str)
    # remove puncuation
    data_str = punc_re.sub(' ', data_str)
    # remove numeric 'words'
    data_str = num_re.sub(' ', data_str)
    # remove non a-z 0-9 characters and words shorter than 3 characters
    list_pos = 0
    cleaned_str = ''
    for word in data_str.split():
        if list_pos == 0:
            if alpha_num_re.match(word) and len(word) > 2:
                cleaned_str = word
            else:
                cleaned_str = ' '
        else:
            if alpha_num_re.match(word) and len(word) > 2:
                cleaned_str = cleaned_str + ' ' + word
            else:
                cleaned_str = cleaned_str + ' '
        list_pos += 1
    return cleaned_str

hsC.registerFunction("remove_features", lambda x: remove_features(x), StringType())

def tag_and_remove(data_str):
    cleaned_str = ''
    # noun tags
    nn_tags = ['NN', 'NNP', 'NNP', 'NNPS', 'NNS']
    # adjectives
    jj_tags = ['JJ', 'JJR', 'JJS']
    # verbs
    vb_tags = ['VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ']
    nltk_tags = nn_tags + jj_tags + vb_tags
    
    # break string into 'words'
    text = data_str.split()
    
    # tag the text and keep only those with the right tags
    tagged_text = pos_tag(text)
    for tagged_word in tagged_text:
        if tagged_word[1] in nltk_tags:
            cleaned_str += ' ' + tagged_word[0]
            
    return cleaned_str

hsC.registerFunction("tag_and_remove", lambda x: tag_and_remove(x), StringType())

# Tweets are going to use different forms of a word, such as organize, organizes, and
# organizing. Additionally, there are families of derivationally related words with similar meanings, such as democracy,
# democratic, and democratization. In many situations, it seems as if it would be useful for a search for one of these
# words to return documents that contain another word in the set.
# Reduces Dimensionality and boosts numerical measures like TFIDF

# http://nlp.stanford.edu/IR-book/html/htmledition/stemming-and-lemmatization-1.html
# lemmatization of a single Tweets (cleaned_str/row/document)
def lemm_verbs(data_str):
    # expects a string
    list_pos = 0
    cleaned_str = ''
    lmtzr = WordNetLemmatizer()
    text = data_str.split()
    for word in text:
        lemma = lmtzr.lemmatize(word, 'v')
        if list_pos == 0:
            cleaned_str = lemma
        else:
            cleaned_str = cleaned_str + ' ' + lemma
        list_pos += 1
    return cleaned_str

hsC.registerFunction("lemm_verbs", lambda x: lemm_verbs(x), StringType())


# stemming of a single Tweets (cleaned_str/row/document)
def stem_str(data_str):
    # expects a string
    list_pos = 0
    cleaned_str = ''
    lancaster_stemmer = LancasterStemmer()
    text = data_str.split()
    for word in text:
        stemmed_words = lancaster_stemmer.stem(word)
        if list_pos == 0:
            cleaned_str = stemmed_words
        else:
            cleaned_str = cleaned_str + ' '+stemmed_words
        list_pos += 1
    return cleaned_str

hsC.registerFunction("stem_str", lambda x: stem_str(x), StringType())



# import data from Hive
# training_df = hsC.sql("select text, class from tchug.training")
# test_df = hsC.sql("select text from tchug.search")

# Load a text file and convert each line to a Row.
lines = sc.textFile("/Users/dreyco676/nlp_spark/data/classified_tweets.txt")
parts = lines.map(lambda l: l.split("\t"))
training = parts.map(lambda p: (p[0], p[1].strip()))

# The schema is encoded in a string.
schemaString = "tweet_text classification"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD.
schemaTraining = hsC.createDataFrame(training, schema)

# Infer the schema, and register the DataFrame as a table.
schemaTraining = hsC.createDataFrame(training)
schemaTraining.registerTempTable("training")




# classify language and filter down to english only
lang_df = df.selectExpr("tweet", "class", "check_lang(tweetText) as lang")
en_df = lang_df.filter(lang_df.lang == 'en')

# remove non helpful text
rm_features_df = en_df.selectExpr("class", "reduce_features(tweetText) as Tweet_Text")


noWS = rm_features_df.map(lambda cleaned_str: str(cleaned_str[0])+'\t'+cleaned_str[1].replace('\t', '').replace('\n', ' ').replace('\r', ' '))
noWS.saveAsTextFile('/data/oghma/training/search/clean/')