import twitter as tw

twitter = tw.TwitterClient()

# get parameters to hand off to twitter
handle = 'dreyco676'
result = twitter.get_followers(handle)
filename = handle + 'followers.txt'
result.to_csv(filename, index=False, mode='a+', encoding='utf-8')