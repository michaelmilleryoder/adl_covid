""" 
Load, filter COVID Twitter data
By keywords and media attachments
"""


import re
import pdb
from multiprocessing import Pool
from collections import Counter
import os
import gzip
import json

from tqdm import tqdm
import pandas as pd


class TweetFilter():

    def __init__(self):
        self.terms_path = '../resources/antisemitic_terms.txt'
        self.basepath = '/storage3/coronavirus/'
        self.paths = None
        self.n_selected = 0

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

    def process_dump(self, fpath):
        """ Process a jsonlines dumped Twitter file """
        selected = []
        fname = os.path.basename(fpath)
        outpath = os.path.join('../output', 'tweets_json', f'{fname.split(".")[0]}.jsonl')
        csv_outpath = os.path.join('../output', 'tweets_csv', f'{fname.split(".")[0]}.csv')
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
                    self.n_selected += 1
                # if i > 100:

        # Save out selected
        with open(outpath, 'w') as f:
            f.write('\n'.join([json.dumps(tweet) for tweet in selected]))
        with open(csv_outpath, 'w') as f:
            pd.json_normalize(selected).to_csv(csv_outpath)

    def run(self):
        self.load_resources()

        # Load COVID Twitter data (Carley lab)
        # Older data
        #dirname = 'json_keyword_stream'
        #paths = [os.path.join(self.basepath, dirname, fname) for fname in sorted(os.listdir(os.path.join(self.basepath, dirname)))]
        #with Pool(15) as p:
        #    list(tqdm(p.imap(self.process_dump, paths), ncols=80, total=len(paths)))

        # Newer data
        dirname = 'json_keyword_stream_mike'
        paths = [os.path.join(self.basepath, dirname, fname) for fname in sorted(os.listdir(os.path.join(self.basepath, dirname))) if fname.endswith('json.gz')]
        with Pool(15) as p:
            list(tqdm(p.imap(self.process_dump, paths), ncols=80, total=len(paths)))
        #list(tqdm(map(self.process_dump, paths), ncols=80, total=len(paths)))
    
        print(self.n_selected)
        #print(Counter([select['search_match'] for select in selected]).most_common())


if __name__ == '__main__':
    tweet_filter = TweetFilter()
    tweet_filter.run()
