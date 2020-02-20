import requests
import urllib.parse
import json
import bz2
import argparse
import datetime
import os.path
import multiprocessing
import pandas

import pytz
tz = pytz.timezone('Canada/Eastern')

baseurl = 'https://hacker-news.firebaseio.com/v0/'

pool_size = 86
batch_size = 1024

def main():
    parser = argparse.ArgumentParser(description='Download HN')
    parser.add_argument('input', type=str, help='comments data file')
    parser.add_argument('output', type=str, help='output file name')
    args = parser.parse_args()
    printWithDate(f"Scanning {args.input}")
    df = pandas.read_csv(args.input)
    users = list(df['by'].unique())
    num_users = len(users)
    printWithDate(f"Starting scrape of {len(users)}")
    with bz2.open(args.output, 'at') as f, multiprocessing.Pool(pool_size) as pool:
        batch = []
        while len(users) > 0:
            batch.append(users.pop())
            if len(batch) >= batch_size:
                num_null, num_timeout = fetch_batch(batch, f, pool)
                batch = []
                printWithDate(f"ID {num_users - len(users)} done, {1 - len(users) / num_users * 100:.4f}% done", end = '\r')
        num_null, num_timeout = fetch_batch(batch, f, pool)
    printWithDate("Done scrape")

def fetch_batch(id_lst, output, pool):
    r = pool.map(getItem, id_lst)
    num_null = 0
    num_timeout = 0
    for j in r:
        if j['type'] == 'null':
            num_null += 1
        elif j['type'] == 'timeout':
            num_timeout += 1
        json.dump(j, output)
        output.write('\n')
    return num_null, num_timeout

def printWithDate(s, **kwargs):
    print(f"{datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')} {s}", **kwargs)

def getItem(item_id):
    try:
        r = requests.get(urllib.parse.urljoin(baseurl, f'user/{item_id}.json'), timeout = 5)
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError, OSError):
        return {'id' : item_id, 'type' : 'timeout'}
    j = r.json()
    if j is None or j == 'null':
        return {'id':item_id, 'type' : 'null'}
    return j

if __name__ == '__main__':
    main()
