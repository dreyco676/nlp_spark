from time import time
import pandas as pd
from aquire_data import twitter as tw


# pull my twitter friends timelines and put into txt files
def update_timeline(id_df):
    for index, row in id_df.iterrows():

        # get parameters to hand off to twitter
        handle = row['User_Id']
        file_path = 'timelines/' + str(handle) + '_' + str(time()) + '.txt'

        # get twitter data
        twitter = tw.TwitterClient()
        twitter.get_timeline(handle, file_path)
    return

if __name__ == '__main__':
    id_df = pd.read_csv('dreyco676_followers_2015-12-23.txt')
    print(id_df)
    update_timeline(id_df)

