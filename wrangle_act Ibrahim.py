#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import requests
import zipfile
import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()
import seaborn as sns
plt.style.use('bmh')
from PIL import Image
from io import BytesIO
from wordcloud import WordCloud, STOPWORDS
import requests
import numpy as np
get_ipython().run_line_magic('matplotlib', 'inline')
import json
import time
import math
import re
import tweepy


# In[2]:


tw_arc = pd.read_csv('twitter-archive-enhanced.csv')


# In[3]:


tw_arc.head()


# In[4]:


url = 'https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv'
response = requests.get(url)

# save to .tsv file
with open('image_predictions.tsv', 'wb') as file:
    file.write(response.content)
img_df = pd.read_csv('image_predictions.tsv', sep='\t')


# In[5]:


image_pred = pd.read_csv('image_predictions.tsv',sep='\t')
image_pred.head()


# In[6]:


consumer_key = 'YOUR CONSUMER KEY'
consumer_secret = 'YOUR CONSUMER SECRET'
access_token = 'YOUR ACCESS TOKEN'
access_secret = 'YOUR ACCESS SECRET'


# In[7]:


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)


# In[8]:


api = tweepy.API(auth_handler=auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


# In[ ]:


start = time.time() # start timer

with open('getstatus_error.txt', 'w') as errfile: 
    valid_ids = 0
    err_ids = 0
    tweet_ids = tw_arc.tweet_id
    with open('tweet_json.txt', 'w', encoding='utf-8') as outfile:
        for i, tweet_id in tweet_ids.iteritems():
            try:
                print("%s# %s" % (str(i+1), tweet_id))
                # Get tweet data using Twitter API
                tweet = api.get_status(tweet_id, tweet_mode='extended')
                json_content = tweet._json
                
                # Write each tweet's JSON data to its own line in a file
                json.dump(json_content, outfile)
                outfile.write('\n')
                valid_ids += 1
            except tweepy.TweepError as e:
                err_ids += 1
                err_str = []
                err_str.append(str(tweet_id))
                err_str.append(': ')
                err_str.append(e.response.json()['errors'][0]['message'])
                err_str.append('\n')
                errfile.write(''.join(err_str))
                print(''.join(err_str))
                continue
        print("%s %s" % ('Valid tweets:', valid_ids))
        print("%s %s" % ('Error tweets:', err_ids))
        
end = time.time() # end timer
print((end - start)/(1000*60))


# In[ ]:


df_list = []
with open('tweet_json.txt', 'r') as json_file:
    for line in json_file:
        status = json.loads(line)
        
        # Append to list of dictionaries
        df_list.append({'tweet_id': status['id'],
                        'retweet_count': status['retweet_count'],
                        'favorite_count': status['favorite_count'],
                        'display_text_range': status['display_text_range']
                       })

# Create a DataFrame with tweet ID, retweet count, favorite count and display_text_range
status_df = pd.DataFrame(df_list, columns = ['tweet_id', 'retweet_count', 'favorite_count', 'display_text_range'])


# In[ ]:


image_pred


# In[ ]:


tw_arc.sample(6)


# In[ ]:


tw_arc.info()


# In[ ]:


# Check if there are any records in arc_df which are retweets
len(tw_arc[tw_arc.retweeted_status_id.isnull() == False])


# In[ ]:


# Check if there are any records in arc_df whose corresponding record with same tweet_id is missing in img_df table
len(tw_arc[~tw_arc.tweet_id.isin(img_df.tweet_id)])


# In[ ]:


# Sort by rating_denominator values
tw_arc.rating_denominator.value_counts().sort_index()


# In[ ]:


twitter_archive.name.str.istitle().value_counts()


# In[ ]:


# Sort by rating_numerator values
tw_arc.rating_numerator.value_counts().sort_index()


# In[ ]:


tw_arc.name.value_counts().sort_index(ascending=False)


# In[ ]:


# Number of records whose both doggo and floofer columns are not None
len(tw_arc[(tw_arc.doggo != 'None') & (tw_arc.floofer != 'None')])


# In[ ]:


# Number of records whose both doggo and pupper columns are not None
len(tw_arc[(tw_arc.doggo != 'None') & (tw_arc.pupper != 'None')])


# In[ ]:


# Number of records whose both doggo and pupper columns are not None
len(tw_arc[(tw_arc.doggo != 'None') & (tw_arc.puppo != 'None')])


# In[ ]:


# Number of records whose both floofer and pupper columns are not None
len(tw_arc[(tw_arc.floofer != 'None') & (tw_arc.pupper != 'None')])


# In[ ]:


img_df.sample(7)


# In[ ]:


img_df.info()


# In[ ]:


status_df.info()


# In[ ]:


twitter_archive.expanded_urls.sample(5)


# In[ ]:


len(twitter_archive.text[twitter_archive.text.str.match('.*only rate dogs')])


# In[ ]:


image_pred.info()


# In[ ]:


image_pred.describe()


# In[ ]:


image_pred.sample(5)


# In[ ]:


image_pred.p1.value_counts()


# In[ ]:


# Take a copy of arc_df on which the cleaning tasks will be performed
archive_clean = tw_arc.copy()


# In[ ]:


archive_clean = archive_clean[archive_clean.retweeted_status_id.isnull()]


# In[ ]:


archive_clean = archive_clean[archive_clean.tweet_id.isin(img_df.tweet_id)]


# In[ ]:


len(archive_clean[~archive_clean.tweet_id.isin(img_df.tweet_id)])


# In[ ]:


archive_clean.info()


# In[ ]:


archive_clean.drop(['retweeted_status_id', 'retweeted_status_user_id', 'retweeted_status_timestamp'], axis=1, inplace=True)


# In[ ]:


archive_clean.info()


# In[ ]:


archive_clean.in_reply_to_status_id = archive_clean.in_reply_to_status_id.fillna(0)
archive_clean.in_reply_to_user_id = archive_clean.in_reply_to_user_id.fillna(0)

archive_clean.in_reply_to_status_id = archive_clean.in_reply_to_status_id.astype(np.int64)
archive_clean.in_reply_to_user_id = archive_clean.in_reply_to_user_id.astype(np.int64)

archive_clean.timestamp = pd.to_datetime(archive_clean.timestamp)


# In[ ]:


archive_clean.info()


# In[ ]:


archive_clean.source = archive_clean.source.str.replace(r'<(?:a\b[^>]*>|/a>)', '')


# In[ ]:


archive_clean.source = archive_clean.source.astype('category')


# In[ ]:


archive_clean.source.value_counts()


# In[ ]:


# Before extraction: untruncated text of first 3 records
print(archive_clean.iloc[0].text)
print(archive_clean.iloc[1].text)
print(archive_clean.iloc[2].text)


# In[ ]:


twitter_archive_clean.dog_stage.value_counts()


# In[ ]:


#archive_clean to extract text using range values
archive_clean = pd.merge(archive_clean, status_df[['tweet_id', 'display_text_range']], on='tweet_id')


# In[ ]:


# using display_text_range of archive_clean, extract displayable text
for i, row in archive_clean.iterrows():
    text_range = row.display_text_range
    display_text = row.text[text_range[0]:text_range[1]]
    archive_clean.set_value(i, 'text', display_text)


# In[ ]:


# drop display_text_range column
archive_clean.drop('display_text_range', axis=1, inplace=True)


# In[107]:


# regex to match fractions
pattern = "\s*(\d+([.]\d+)?([/]\d+))"

# function which will match the above pattern and return an array of fractions, if any
def tokens(x):
  return [m.group(1) for m in re.finditer(pattern, x)]


# In[108]:


# iterate through all those records whose rating_denominator is not 10
for i, row in archive_clean[archive_clean.rating_denominator != 10].iterrows():
    d = row.rating_denominator
    
    # if rating_denominator is greater than 10 and divisible by 10
    if d > 10 and d%10 == 0:
        # assign divisor as the quotient
        divisor = d/10
        n = row.rating_numerator
        
        # if rating_numerator is greater than 10 and divisible by the divisor
        if n%divisor == 0:
            # reassign rating_denominator as 10
            archive_clean.set_value(i, 'rating_denominator', 10)
            # reassign rating_numerator as the quotient of rating_numerator by divisor
            archive_clean.set_value(i, 'rating_numerator', int(n/divisor))
    
    # for all those records whose rating_denominator is either less than 10 or not divisible by 10
    else:
        # extract all fractions(ratings) from text using tokens function
        ratings = tokens(row.text)
        # iterate through all the fractions
        for rating in ratings:
            # if denominator of any such fraction is equal to 10
            if rating.split('/')[1] == '10':
                # reassign rating_denominator as 10
                archive_clean.set_value(i, 'rating_denominator', 10)
                # reassign rating_numerator as the numerator value of this fraction
                archive_clean.set_value(i, 'rating_numerator', int(round(float(rating.split('/')[0]))))
                break


# In[109]:


archive_clean.rating_denominator.value_counts()


# In[110]:


archive_clean[(archive_clean.rating_numerator <= 10) | (archive_clean.rating_numerator > 14)].rating_numerator.value_counts().sort_index()


# In[111]:


# for rows whose rating numerator is either less than or equal to 10 OR
# greater than 10 but has a very high value (consider greater than 14)
for i, row in archive_clean[(archive_clean.rating_numerator <= 10) | (archive_clean.rating_numerator > 14)].iterrows():
    ratings = tokens(row.text)
    for rating in ratings:        
        if rating.split('/')[1] == '10':
            n = int(round(float(rating.split('/')[0])))
            if (row.rating_numerator == 10 and n > 10) or (row.rating_numerator != 10 and n >= 10):
                archive_clean.set_value(i, 'rating_numerator', n)
                break


# In[112]:


archive_clean[(archive_clean.rating_numerator <= 10) | (archive_clean.rating_numerator > 14)].rating_numerator.value_counts().sort_index()


# In[113]:


archive_clean['name'][archive_clean['name'].str.match('[a-z]+')] = 'None'


# In[114]:


archive_clean.name[archive_clean.name == 'None'].value_counts()


# In[115]:


# Sort ascending by name to check if there are more names starting with a lowercase alphabet
archive_clean.name.value_counts().sort_index(ascending=False)


# In[116]:


print(len(archive_clean[(archive_clean.doggo != 'None') & (archive_clean.floofer != 'None')]))
print(len(archive_clean[(archive_clean.doggo != 'None') & (archive_clean.puppo != 'None')]))
print(len(archive_clean[(archive_clean.doggo != 'None') & (archive_clean.pupper != 'None')]))


# In[118]:


for i, row in archive_clean[((archive_clean.doggo != 'None') & (archive_clean.floofer != 'None'))
                   | ((archive_clean.doggo != 'None') & (archive_clean.puppo != 'None'))].iterrows():
    print('%s %s\n'%(row.tweet_id, row.text))


# In[67]:


twitter_archive_clean = pd.merge(twitter_archive_clean, tweet_json_clean , how = 'left' , on = 'tweet_id')

# use the merge function to merge `twitter_archive_clean` and `image_pred_clean` on tweet_id column (inner join) 
# and make master dataset
master_dataset = pd.merge(twitter_archive_clean, image_pred_clean , how = 'inner' , on = 'tweet_id')


# In[68]:


master_dataset.info()


# In[69]:


master_dataset.jpg_url.isnull().sum()


# In[70]:


master_dataset.source.unique()


# In[71]:


def fix_source(i):
    'i is an html string from the source column in twitter_archive_clean dataset'
    #find the first closed  tag >
    x= i.find('>') + 1
    # find the first open tag after the previous <
    y =i[x:].find('<')
    # extract the text in between
    return i[x:][:y]


# In[72]:


master_dataset.source= master_dataset.source.apply(lambda x: fix_source(x))


# In[73]:


master_dataset.source.value_counts()


# In[ ]:




