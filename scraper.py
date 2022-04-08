# original script for comment scraping from https://github.com/JosephLai241/URS, adapted for specific needs

import praw
import os
from pmaw import PushshiftAPI
import json
import pandas as pd
import datetime as dt
import subprocess
import time
import re
from urllib.parse import urlparse

start_time = time.time()

# [INPUT NEEDED] -------------------------------
# replace these variables according to scraping needs:

# load Reddit authentication for PRAW/PMAW

reddit = praw.Reddit(client_id='', 
                     client_secret='',
                     username="",
                     password="",
                     user_agent='')

# building the Pushshift API request for data collection
keywords = 'Ukraine|ukrainian|ukrainians|Kiev|Kyiv|Zelensky|Украина|украина|Украинцы|Киев|Киев|Зеленский'
subreddits = 'Worldnews'#,News,PoliticalDiscussion,GeoPolitics,InTheNews,USA,USANews,Europe 
before = int(dt.datetime(2022,2,23,0,0).timestamp())
after = int(dt.datetime(2021,2,23,0,0).timestamp())
limit=0
workers=40

# -----------------------------------------------

# function that formats text for readability 

def clean_text(text):
    text = text.strip()
    text = re.sub('\n+', ' ', text)
    text = re.sub('&amp;', '&', text)
    text = re.sub('&lt;', '<', text)
    text = re.sub('&gt;', '>', text)
    text = re.sub('&#x200B;', '', text)
    text = re.sub('&nbsp;', ' ', text)
    text = re.sub('&gt;', '>', text)
    text = re.sub('&lt;', '<', text)
    return text

api = PushshiftAPI(praw=reddit)

# -----------------------------------------------
# start submission scraping

first_pass = True
last_utc=before
data = []
passes=1
while True:
    submissions=api.search_submissions(q={keywords},subreddit=subreddits,after=after, before=last_utc,memsafe=True,num_workers=workers)
    print("Adding submission data for pass #" + str(passes))
    posts=[post for post in submissions]
    data.extend(posts)
    if len(posts) == 0:
        break # stop collecting data once there's nothing left to collect
    passes+=1
    last_utc = data[-1]['created_utc']

print("successful data collection! "+str(len(data)) +" comments collected\n")

# -----------------------------------------------
# clean/format data - data should already be updated since PMAW is used

print("cleaning and formatting data...\n")
record = 0
domain = ""
for d in data:

    d.update({'post keywords': keywords}) # for reference in csv
    d.update({'date': dt.datetime.fromtimestamp(d['created_utc'])})
    d.update({'title': clean_text(d.get("title","N/A"))})
    d.update({'selftext': clean_text(d.get("selftext","N/A"))})
    domain = d['url']
    d.update({'url' : urlparse(str(domain)).netloc})
    d.update({'full_link' : "reddit.com"+str(d.get('permalink'))})
    record+=1

# -----------------------------------------------
# final formatting and exporting scraped posts to csv

column_order = ['id', 'subreddit', 'url', 'post keywords', 'date', 'score', 'num_comments', 'author', 'title','full_link']
df = pd.DataFrame.from_records(data, columns=column_order).drop_duplicates()
df = df.sort_values(['score'], ascending=False) # sort by updated scores in csv
df.to_excel('./data/scraped_posts/'+ str(subreddits)+'_submissions_pmaw.xlsx', engine='xlsxwriter', index=False, header=True, columns=list(df.axes[1]), encoding='utf-8-sig')

runtime = '{:.0f}'.format(time.time() - start_time)
print(f"--- DONE! runtime: {runtime} seconds ---")
print("see data > scraped_posts for exported csv \n")

# -----------------------------------------------
# start comment scraping

start_time = time.time()
before = int(dt.datetime(2022,2,23,0,0).timestamp())
after = int(dt.datetime(2021,2,23,0,0).timestamp())
posts_to_scrape_for_comments = pd.read_csv('./data/scraped_posts/'+ str(subreddits)+'_submissions_pmaw.csv', sep='|')
print(len(posts_to_scrape_for_comments))
posts_to_scrape_for_comments=posts_to_scrape_for_comments[19:]
posts_to_scrape_for_comments=posts_to_scrape_for_comments[posts_to_scrape_for_comments['num_comments']>0]
print(len(posts_to_scrape_for_comments))
posts_to_scrape_for_comments=posts_to_scrape_for_comments['full_link']
passes=1
for link in posts_to_scrape_for_comments:
    url = f'https://{link}'
    print("collecting Reddit comment data...")
    cmd=format('python C:\\Desktop\\Ukraine\\URS\\urs\\Urs.py -c '+str(url)+' 0 --raw')
    subprocess.check_output(['start', 'cmd', '/c', cmd], shell=True)
    print("submission #"+str(passes)+" complete, scraping following submission")
    passes+=1
print("successful data collection!\n")

# -----------------------------------------------
# convert json in python dictionary

data=[]
path_to_json = 'C:\\Users\\Natale\\Desktop\\scrapes\\'
for folder, sub_folders, files in os.walk(path_to_json):
    for name in files:
        if name.endswith(".json"):
            filename = os.path.join(folder, name)
            file = json.load(open(filename))
            data.extend(file['data']['comments'])
        else:
            continue
print('Conversion from JSON to python dictionary completed. Total comments: ' + str(len(data)))

# -----------------------------------------------
# clean and format commenta data

record=1
for d in data:
    d.update({'body': clean_text(d.get("body","N/A"))})
    print("comment in row " +str(record) + " updated")
    record+=1

# -----------------------------------------------
# final formatting and exporting scraped comments to csv

df1 = pd.DataFrame.from_records(data)
df1=df1.drop('body_html', axis=1)
df1.to_excel('./data/scraped_comments/'+ str(subreddits)+'_comments_pmaw.xlsx', engine='xlsxwriter', index=False, header=True, columns=list(df1.axes[1]), encoding='utf-8-sig')

runtime = '{:.0f}'.format(time.time() - start_time)
print(f"--- DONE! runtime: {runtime} seconds ---")
print("see data > scraped_comments for exported csv \n")





