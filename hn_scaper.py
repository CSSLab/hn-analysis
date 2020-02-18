import requests
import urllib.parse
import json
import bz2
import argparse
import datetime
import os.path
import multiprocessing

import pytz
tz = pytz.timezone('Canada/Eastern')

baseurl = 'https://hacker-news.firebaseio.com/v0/'

max_id = 22183000

pool_size = 86
batch_size = 1024

def main():
    parser = argparse.ArgumentParser(description='Download HN')
    #parser.add_argument('startingID', type=int, help='first ID to try')
    parser.add_argument('output', type=str, help='output file name')
    args = parser.parse_args()
    printWithDate(f"Scanning {args.output}")
    bad_ids = set()
    if os.path.isfile(args.output):
        with bz2.open(args.output, 'rt') as f:
            for line in f:
                j = json.loads(line)
                if j['type'] == 'timeout':
                    bad_ids.add(j['id'])
                elif j['id'] in bad_ids:
                    bad_ids.remove(j['id'])
            start_id = j['id'] + 1
    else:
        start_id = 1

    printWithDate(f"Starting scrape at {start_id}, with {len(bad_ids)} bad ids")
    with bz2.open(args.output, 'at') as f, multiprocessing.Pool(pool_size) as pool:
        i = start_id
        id_batch = list(bad_ids)
        while i < max_id or len(id_batch) > 0:
            id_batch.append(i)
            i += 1
            if len(id_batch) >= batch_size:
                try:
                    num_null, num_timeout = fetch_batch(id_batch, f, pool)
                except Exception as e:
                    printWithDate(f"Exception on ID {i}:\n{e}")
                    raise
                except KeyboardInterrupt:
                    printWithDate(f"Interupting ID {i}")
                    break
                printWithDate(f"ID {i} done, {num_null} {num_timeout}, {i /max_id * 100:.4f}% done", end = '\r')
                id_batch = []
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
        r = requests.get(urllib.parse.urljoin(baseurl, f'item/{item_id}.json'), timeout = 5)
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError, OSError):
        return {'id' : item_id, 'type' : 'timeout'}
    j = r.json()
    if j is None or j == 'null':
        return {'id':item_id, 'type' : 'null'}
    return j

if __name__ == '__main__':
    main()
