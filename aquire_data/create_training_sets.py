import pandas as pd

# filter down to tweets containing keyword and save as csv
def create_training(term_list, filename, corpus_df):
    # empty dataframe for filtered tweets to go into
    combo_df = pd.DataFrame()

    # ensure no mismatched case
    term_list = [element.lower() for element in term_list]

    # search for each term in list
    for keyword in term_list:
        # ensure no mismatched case
        keyword = keyword.lower()
        df = corpus_df[corpus_df['tweet'].str.contains(keyword)]
        combo_df = pd.concat([combo_df, df], ignore_index=True)

    # remove any duplicates
    final_df = combo_df.drop_duplicates()

    # save out to text file
    final_df.to_csv(filename, header=False, index=False)


if __name__ == '__main__':
    # read in csv file
    corpus_df = pd.read_csv('tweets.txt', header=None, names=['tweet'])
    # ensure no mismatched case
    lower_series = corpus_df['tweet'].str.lower()
    # remove urls otherwise you will end up with duplicates differing by reposted urls
    no_url_series = lower_series.str.replace('https?://(www.)?\w+\.\w+(/\w+)*/?', '')
    cleaner_df = no_url_series.to_frame()

    # remove na rows
    corpus_df = cleaner_df.dropna()

    # words should be a good proxy for a high TFIDF (unique to the type of the document but high freq within)
    # Use Amazon Mechanical Turks if you have budget to have two people go through and verify

    # python
    python_terms = ['python', 'pandas', 'django', 'anaconda', 'pypi', 'beautifulsoup', 'numpy', 'scipy', 'matplotlib',
                    'pygame', 'sqlalchemy', 'nltk', 'pyspark', 'pep8']
    #create_training(python_terms, 'training/python_training.txt', corpus_df)


    # hadoop/big data
    hadoop_terms = ['hadoop', 'big data', 'bigdata', 'cloudera', 'impala', 'hive', 'mapr', 'hortonworks', 'mapreduce',
                    'map reduce']
    #create_training(hadoop_terms, 'training/hadoop_training.txt', corpus_df)


    # data science, this is much more nebulous and hotly debated, let's keep it simple
    datasci_terms = ['datascience', 'data science', 'data scientist', 'data scientist']
    #create_training(datasci_terms, 'training/datasci_training.txt', corpus_df)

    # create all other class
    # not_in_terms = 'python|pandas|django|anaconda|pypi|beautifulsoup|numpy|scipy|matplotlib|pygame|sqlalchemy|nltk|' \
    #                'pyspark|pep8|hadoop|big data|bigdata|cloudera|impala|hive|mapr|hortonworks|mapreduce|map reduce|' \
    #                'datascience|data science|data scientist|data scientist'
    # all_other = corpus_df[~corpus_df['Tweet_Text'].str.contains(not_in_terms)]
    # ao_no_dups = all_other.dropna()
    # ao_no_dups.to_csv('training/allother_training.txt', header=False, index=False)

    # what do we say to optomization?
    # Not today!

    # lets create the basic table of tweets to use for all training
    python_df = pd.read_csv('training/python_training.txt', header=None, names=['Tweet_Text'])
    python_df['text_class'] = 'python'

    hadoop_df = pd.read_csv('training/hadoop_training.txt', header=None, names=['Tweet_Text'])
    hadoop_df['text_class'] = 'hadoop'

    datasci_df = pd.read_csv('training/datasci_training.txt', header=None, names=['Tweet_Text'])
    datasci_df['text_class'] = 'datasci'

    ao_df = pd.read_csv('training/allother_training.txt', header=None, names=['Tweet_Text'])
    ao_df['text_class'] = 'all_other'

    combo_training_df = pd.concat([python_df, hadoop_df, datasci_df, ao_df], ignore_index=True)

    # WARNING: Tab only works since we took all of them out in our hive script!
    combo_training_df.to_csv('training/combo_training.txt', sep='\t', header=False, index=False)


