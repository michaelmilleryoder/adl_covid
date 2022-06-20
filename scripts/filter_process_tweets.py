""" 
Load, filter, process COVID Twitter data
By keywords and media attachments
"""


import re
import pdb
from multiprocessing import Pool
from collections import Counter
import os
import gzip
import json
import shutil

from tqdm import tqdm
import pandas as pd


def remove_mentions(text, user_mentions):
    """ Remove mentions from a text """
    new_text = text
    usernames = [mention['screen_name'] for mention in user_mentions]
    for username in usernames:
        new_text = re.sub(username, 'USER', new_text, flags=re.IGNORECASE)
    return new_text


def process_text(text, user_mentions):
    new_text = remove_mentions(text, user_mentions)
    return ' '.join(new_text.split(' ')[:-1]) # remove URL


def process_tweets(data):
    """ Process a dataframe of tweets """
    selected_cols = ['created_at', 'id_str', 'text', 'in_reply_to_status_id_str', 'geo', 'coordinates', 'place', 
                 'is_quote_status', 'quote_count', 'reply_count', 'retweet_count', 'favorite_count', 'possibly_sensitive',
                 'search_match',
                 'user.id_str', 'user.location', 'user.geo_enabled',
                 'entities.hashtags', 'entities.urls', 'entities.user_mentions', 'entities.symbols', 'entities.media', 'extended_entities.media',
                 'retweeted_status.id_str', 'retweeted_status.user.location', 'retweeted_status.geo', 'retweeted_status.coordinates', 'retweeted_status.place', 
                 'retweeted_status.quote_count', 'retweeted_status.reply_count', 'retweeted_status.retweet_count',
                 'quoted_status.id_str', 'quoted_status.user.location', 'quoted_status.geo', 'quoted_status.coordinates', 'quoted_status.place', 
                 'quoted_status.quote_count', 'quoted_status.reply_count', 'quoted_status.retweet_count',
                ]
    # Check if columns are present
    selected_cols = [col for col in selected_cols if col in data.columns]
    data = data[selected_cols].copy()
    data['url'] = [el[-1] for el in data.text.str.split(' ')]
    data['processed_text'] = [process_text(text, mentions) for text, mentions in zip(data['text'], data['entities.user_mentions'])]
    data = data[sorted(data.columns)]
    return data.drop(columns='text')


class TweetFilter():

    def __init__(self, overwrite=False):
        self.terms_path = '../resources/antisemitic_terms.txt'
        self.basepath = '/storage3/coronavirus/'
        self.paths = None
        self.n_selected = 0
        self.overwrite = overwrite

    def load_resources(self):
        with open(self.terms_path) as f:
            search_terms = f.read().splitlines()
            self.pats = [re.compile(r'\b{}\b'.format(re.escape(term.lower()))) for term in search_terms]

    def select_tweet(self, tweet):
        """ See if a tweet is worth keeping (matches enough criteria) """
        select = False
        
        # Basic cleaning
        if len(tweet) == 1 and 'limit' in tweet:
            return select
        
        # Language is English
        if tweet['lang'] != 'en':
            return select
        
        # Has media
        if 'media' not in tweet['entities']:
            return select
        
        # Contains possibly antisemitic terms
        if 'extended_tweet' in tweet:
            text = tweet['extended_tweet']['full_text'].lower()
        else:
            text = tweet['text'].lower()
        for p in self.pats:
            m = re.search(p, text)
            # if any([re.search(p, tweet['extended_tweet']['full_text'].lower()) for p in pats]):
            if m is not None:
                select = m.group()
                # tqdm.write('one selected')
                
        return select

    def load_process_tweets(self):
        """ Load tweets that have been filtered and saved already.
            Use if you want to do additional processing
        """
        filtered_dirpath = os.path.join('../output', 'tweets_json')
        out_dirpath = os.path.join('../output', 'processed_tweets_csv')
        if not os.path.exists(out_dirpath):
            os.mkdir(out_dirpath)
        for fname in tqdm(sorted(os.listdir(filtered_dirpath)), ncols=80):
            fpath = os.path.join(filtered_dirpath, fname)
            with open(fpath) as f:
                data = pd.json_normalize([json.loads(line) for line in f.read().splitlines()])
            if not len(data) == 0:
                processed = process_tweets(data)
        
            # Save out
            outpath = os.path.join(out_dirpath, f'{os.path.splitext(fname)[0]}.csv')
            processed.to_csv(outpath)

    def process_dump(self, fpath):
        """ Process a jsonlines dumped Twitter file """
        selected = []
        fname = os.path.basename(fpath)
        outpath = os.path.join('../output', 'tweets_json', f'{fname.split(".")[0]}.jsonl')
        csv_outpath = os.path.join('../output', 'tweets_json', f'{fname.split(".")[0]}.csv')
        if os.path.exists(outpath): # already processed
            return

        #tqdm.write(fname)
        with gzip.open(fpath, 'rb') as f:
            #for i, line in tqdm(enumerate(f), total=974483):
            # for i, line in tqdm(enumerate(f), total=974483, bar_format='selected: {postfix} | Elapsed: {elapsed} | {rate_fmt}', postfix=n_selected):
            for line in f:
                if len(line) == 1:
                    continue
                try:
                    tweet = json.loads(line)
                except json.decoder.JSONDecodeError:
                    tqdm.write('json decode error')
                    continue
                except UnicodeDecodeError:
                    tqdm.write('unicode decode error')
                    continue
                match = self.select_tweet(tweet)
                if match:
                    tweet['search_match'] = match
                    selected.append(tweet)
                # if i > 100:

        # Save out selected
        with open(outpath, 'w') as f:
            f.write('\n'.join([json.dumps(tweet) for tweet in selected]))
        with open(csv_outpath, 'w') as f:
            df = pd.json_normalize(selected)
            processed = process_tweets(df)
            processed.to_csv(csv_outpath)

    def run(self):
        n_cores = 10

        csv_dirpath = os.path.join('../output', 'tweets_csv')
        json_dirpath = os.path.join('../output', 'tweets_json')
        if self.overwrite:
            if os.path.exists(csv_dirpath):
                shutil.rmtree(csv_dirpath)
            os.mkdir(csv_dirpath)
        if not os.path.exists(json_dirpath):
            os.mkdir(json_dirpath)

        self.load_resources()

        # Load COVID Twitter data (Carley lab)
        # Older data
        print("Filtering older data...")
        dirname = 'json_keyword_stream'
        paths = [os.path.join(self.basepath, dirname, fname) for fname in sorted(os.listdir(os.path.join(self.basepath, dirname)))]
        with Pool(n_cores) as p:
            list(tqdm(p.imap(self.process_dump, paths), ncols=80, total=len(paths)))
        #list(map(self.process_dump, paths)) # debugging

        # Newer data
        print("Filtering newer data...")
        dirname = 'json_keyword_stream_mike'
        paths = [os.path.join(self.basepath, dirname, fname) for fname in sorted(os.listdir(os.path.join(self.basepath, dirname))) if fname.endswith('json.gz')]
        with Pool(n_cores) as p:
            list(tqdm(p.imap(self.process_dump, paths), ncols=80, total=len(paths)))
        #list(tqdm(map(self.process_dump, paths), ncols=80, total=len(paths))) # debugging

        #self.load_process_tweets()
    
        #print(Counter([select['search_match'] for select in selected]).most_common())


if __name__ == '__main__':
    tweet_filter = TweetFilter(overwrite=True)
    tweet_filter.run()
