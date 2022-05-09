import nltk
import pandas
import re
import nltk
from nltk.corpus import stopwords
from nltk import sentiment as se
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import requests
stopwords = requests.get("https://gist.githubusercontent.com/rg089/35e00abf8941d72d419224cfd5b5925d/raw/12d899b70156fd0041fa9778d657330b024b959c/stopwords.txt").content
stopword_list = set(stopwords.decode().splitlines()) 

print('Loading excel file...')

subreddits = 'worldnews'#,News,PoliticalDiscussion,GeoPolitics,InTheNews,USA,USANews,Europe,russia
df=pandas.read_excel('__________')#insert path to scraped content

print('File read...')

def lower_string(string:str):
    return str(string).lower()
df['body']=df['body'].apply(lower_string)
print("All lower case: complete")
def remove_stopwords(sentence:str):
    sentence = re.sub(r"\'n't", " not", sentence)
    sentence = re.sub(r"\'t", " not", sentence)
    sentence = re.sub(r"\'re", " are", sentence)
    sentence = re.sub(r"\'s", " is", sentence)
    sentence = re.sub(r"\'d", " would", sentence)
    sentence = re.sub(r"\'ll", " will", sentence)
    sentence = re.sub(r"\'ve", " have", sentence)
    sentence = re.sub(r"\'m", " am", sentence)
    sentence = re.sub(r"(?!(?<=[a-z])'[a-z])[^\w\s]", '', sentence)
    sentence=re.sub(r'http\S+', '', sentence)
    word_tokens = nltk.word_tokenize(sentence, language='english', preserve_line=True)  
    clean_tokens = [w for w in word_tokens if not w in stopword_list]  
    clean_tokens = [w for w in clean_tokens if not w =="'s"]
    clean_tokens = [w for w in clean_tokens if not w =="n't"]
    clean_tokens=' '.join(clean_tokens)
    clean_tokens = nltk.word_tokenize(clean_tokens, language='english', preserve_line=True) 
    return clean_tokens

def clean(sentence:str):
    sentence = re.sub(r"\'n't", " not", sentence)
    sentence = re.sub(r"\'t", " not", sentence)
    sentence = re.sub(r"\'re", " are", sentence)
    sentence = re.sub(r"\'s", " is", sentence)
    sentence = re.sub(r"\'d", " would", sentence)
    sentence = re.sub(r"\'ll", " will", sentence)
    sentence = re.sub(r"\'ve", " have", sentence)
    sentence = re.sub(r"\'m", " am", sentence)
    sentence = re.sub(r"(?!(?<=[a-z])'[a-z])[^\w\s]", '', sentence)
    sentence=re.sub(r'http\S+', '', sentence)
    return sentence

print('Tokenizing...')

def tokenize(text:str):
    return nltk.word_tokenize(text, language='english', preserve_line=True)

df['tokenized_body'] = df['body'].apply(remove_stopwords)

print("Calculating frequency distribution...")

fdist = nltk.FreqDist(sum(df['tokenized_body'],[]))
print("Frequency distribution calculated")
wc = WordCloud(width=2400, height=1200, max_words=200).generate_from_frequencies(fdist)
plt.figure(figsize=(100, 100))
plt.imshow(wc, interpolation='bilinear')
plt.axis('off')
plt.show()

df['tokenized_body'] = df['body'].apply(remove_stopwords)
df['body_clean']=df['body'].apply(clean)

df['body_clean_tokenized']=df['body_clean'].apply(tokenize)
words = sum(df['body_clean_tokenized'],[])
print(type(words))
words1= sum(df['tokenized_body'],[])
#finder2 = nltk.collocations.BigramCollocationFinder.from_words(words1)
#finder3 = nltk.collocations.TrigramCollocationFinder.from_words(words1)
#finder4 = nltk.collocations.QuadgramCollocationFinder.from_words(words1)

#text=nltk.Text(words)
#text.concordance("ukraine", lines=25)

#finder2.ngram_fd.tabulate(5)
#finder3.ngram_fd.tabulate(5)
#finder4.ngram_fd.tabulate(5)
sia=se.SentimentIntensityAnalyzer()
def polarity_scores(text:str):
    return sia.polarity_scores(text=text)['compound']

df['polarity_score']=df['body_clean'].apply(polarity_scores)
df.to_excel('______.xlsx', engine='xlsxwriter', index=False, header=True, encoding='utf-8-sig') #add path for export
