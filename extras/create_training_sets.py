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

    # return dataframe
    return final_df


if __name__ == '__main__':
    # read in csv file
    corpus_df = pd.read_csv('training_ids.txt', sep='\t', header=None, names=['tweet', 'tweetid'])
    # ensure no mismatched case
    corpus_df['tweet'].str.lower()
    # remove urls otherwise you will end up with duplicates differing by reposted urls
    corpus_df['tweet'].str.replace('https?://(www.)?\w+\.\w+(/\w+)*/?', '')

    # remove na rows
    cleaner_df = corpus_df.dropna()
    print(cleaner_df.head(50))

    # words should be a good proxy for a high TFIDF (unique to the type of the document but high freq within)
    # Use Amazon Mechanical Turks if you have budget to have two people go through and verify

    # python
    python_terms = ['python', 'pandas', 'django', 'anaconda', 'pypi', 'beautifulsoup', 'numpy', 'scipy', 'matplotlib',
                    'pygame', 'sqlalchemy', 'nltk', 'pyspark', 'pep8']
    python_df = create_training(python_terms, 'python_training.txt', cleaner_df)
    python_df['text_class'] = 'python'

    # hadoop/big data
    hadoop_terms = ['hadoop', 'big data', 'bigdata', 'cloudera', 'impala', 'hive', 'mapr', 'hortonworks', 'mapreduce',
                    'map reduce']
    hadoop_df = create_training(hadoop_terms, 'hadoop_training.txt', cleaner_df)
    hadoop_df['text_class'] = 'hadoop'

    # data science, this is much more nebulous and hotly debated, let's keep it simple
    datasci_terms = ['datascience', 'data science', 'data scientist', 'data scientist']
    datasci_df = create_training(datasci_terms, 'datasci_training.txt', cleaner_df)
    datasci_df['text_class'] = 'datasci'

    # create all other class
    not_in_terms = 'python|pandas|django|anaconda|pypi|beautifulsoup|numpy|scipy|matplotlib|pygame|sqlalchemy|nltk|' \
                   'pyspark|pep8|hadoop|big data|bigdata|cloudera|impala|hive|mapr|hortonworks|mapreduce|map reduce|' \
                   'datascience|data science|data scientist|data scientist'
    all_other = corpus_df[~cleaner_df['tweet'].str.contains(not_in_terms)]
    ao_df = all_other.dropna()
    ao_df['text_class'] = 'all_other'

    combo_training_df = pd.concat([python_df, hadoop_df, datasci_df, ao_df], ignore_index=True)

    # WARNING: Tab only works since we took all of them out in our hive script!
    combo_training_df.to_csv('training.txt', sep='\t', header=False, index=False)


