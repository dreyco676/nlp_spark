from aquire_data import twitter as tw
import datetime


# get followers for a user and same them in a UTF-8 Formatted CSV file.
twitter = tw.TwitterClient()

handle = 'dreyco676'
result = twitter.get_followers(handle)
now = datetime.datetime.now().strftime("%Y-%m-%d")
filename = handle + '_followers_' + now + '.txt'
result.to_csv(filename, index=False, mode='a+', encoding='utf-8')