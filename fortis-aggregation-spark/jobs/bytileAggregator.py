# -*- coding: utf-8 -*-
"""
Created on Wed May 11 21:45:55 2016

@author: Mike Lanzetta
"""

from dateutil.parser import parse
from pyspark import SparkConf, SparkContext
import copy
import json
import os
import random
import re
import shutil
import time
from datetime import datetime, timedelta

#from tile import Tile
import math

class Tile:

    MAX_ZOOM = 16
    MIN_ZOOM = 0

    @classmethod
    def tile_id_from_lat_long(cls, latitude, longitude, zoom):
        row = int(Tile.row_from_latitude(latitude, zoom))
        column = int(Tile.column_from_longitude(longitude, zoom))

        return Tile.tile_id_from_row_column(row, column, zoom)

    @classmethod
    def row_from_latitude(cls, latitude, zoom):
        return math.floor((1 - math.log(math.tan(latitude * math.pi / 180) + 1 / math.cos(latitude * math.pi / 180)) / math.pi) / 2 * (2 ** zoom))

    @classmethod
    def column_from_longitude(cls, longitude, zoom):
        return math.floor((longitude + 180.0) / 360.0 * (2 ** zoom))

    @classmethod
    def latitude_from_row(cls, row, zoom):
        n = math.pi - 2.0 * math.pi * row / (2 ** zoom)
        return (180.0 / math.pi * math.atan(0.5 * (math.exp(n) - math.exp(-n))))

    @classmethod
    def longitude_from_column(cls, column, zoom):
        return float(column) / (2 ** zoom) * 360.0 - 180.0

    @classmethod
    def tile_from_tile_id(cls, tile_id):
        parts = tile_id.split('_')
        if len(parts) != 3:
            return

        tile = Tile()

        tile.tile_id = tile_id
        tile.zoom = int(parts[0])
        tile.row = int(parts[1])
        tile.column = int(parts[2])

        tile.latitude_north = Tile.latitude_from_row(tile.row, tile.zoom)
        tile.latitude_south = Tile.latitude_from_row(tile.row + 1, tile.zoom)

        tile.longitude_west = Tile.longitude_from_column(tile.column, tile.zoom)
        tile.longitude_east = Tile.longitude_from_column(tile.column + 1, tile.zoom)

        tile.center_latitude = (tile.latitude_north + tile.latitude_south) / 2.0
        tile.center_longitude = (tile.longitude_east + tile.longitude_west) / 2.0

        return tile

    @classmethod
    def tile_id_from_row_column(cls, row, column, zoom):
        return '%s_%s_%s' % (zoom, row, column)

    def parent_id(self):
        return Tile.tile_id_from_lat_long(self.center_latitude, self.center_longitude, self.zoom-1)

    def parent(self):
        return Tile.tile_from_tile_id(self.parent_id())

    @classmethod
    def decode_tile_id(cls, tileId):
        parts = tileId.split('_')
        if len(parts) != 3:
            return

        return {
            'id': tileId,
            'zoom': int(parts[0]),
            'row': int(parts[1]),
            'column': int(parts[2])
        }

    @classmethod
    def tile_ids_for_all_zoom_levels(cls, tileId):
        tile = Tile.tile_from_tile_id(tileId)
        tileIds = []
        for zoom in range(Tile.MAX_ZOOM, Tile.MIN_ZOOM, -1):
            tileId = Tile.tile_id_from_lat_long(tile.center_latitude, tile.center_longitude, zoom)
            tileIds.append(tileId)
        return tileIds

    def children(self):
        midNorthLatitude = (self.center_latitude + self.latitude_north) / 2
        midSouthLatitude = (self.center_latitude + self.latitude_south) / 2
        midEastLongitude = (self.center_longitude + self.longitude_east) / 2
        midWestLongitude = (self.center_longitude + self.longitude_west) / 2
        return [
            Tile.tile_id_from_lat_long(midNorthLatitude, midEastLongitude, self.zoom + 1),
            Tile.tile_id_from_lat_long(midNorthLatitude, midWestLongitude, self.zoom + 1),
            Tile.tile_id_from_lat_long(midSouthLatitude, midEastLongitude, self.zoom + 1),
            Tile.tile_id_from_lat_long(midSouthLatitude, midWestLongitude, self.zoom + 1)
        ]

import io

epoch = datetime.utcfromtimestamp(0)

def unix_time_millis(dt):
    return int((dt - epoch).total_seconds()) * 1000

def getenv(key, default=None, converter=lambda x: x):
    if key not in os.environ and default == None:
        raise Exception('Expected %s in os.environ' % key)
    return converter(os.environ[key]) if key in os.environ else default
    
def safe_load(line):
    try:
        return ('valid', json.loads(line))
    except ValueError as err:
        return ('parse_error', u'Failed to load %s: %s' % (line, str(err)))

def has_keywords(x):
    slen = lambda a, b: len(a[b]) if b in a else 0
    total_sections = slen(x, 'Keywords')
    return total_sections > 0

def filter_to_valid(rdd, stats, stats_suffix, check_keys=True, required_keys=['Created', 'Locations', 'MessageId', 'Sentence', 'Source']):
    valid_key = 'valid_%s' % stats_suffix
    
    def validator(k):
        if k[0] != 'valid':
            return k
        s = k[1]
        if check_keys:
            for key in required_keys:
                if not key in s:
                    return ('missing_%s_%s' % (key, stats_suffix), s)
        return (valid_key, s)
        
    validated = rdd.map(safe_load).map(validator)
    print 'before countByKey'
    stats.add_stats(validated.countByKey())
    print 'after countByKey'
    return validated.filter(lambda x: x[0] == valid_key).map(lambda x: x[1])
    
def build_timespan_label(timespanType, timestampDate):
    if timespanType == 'alltime':
        return 'alltime'
    elif timespanType == 'year':
        return 'year-%d' % timestampDate.year
    elif timespanType == 'month':
        return 'month-%d-%02d' % (timestampDate.year, timestampDate.month)
    elif timespanType == 'week':
        return 'week-%d-%02d' % (timestampDate.year, timestampDate.isocalendar()[1])
    elif timespanType == 'day':
        return 'day-%d-%02d-%02d' % (timestampDate.year, timestampDate.month, timestampDate.day)
    elif timespanType == 'hour':
        return 'hour-%d-%02d-%02d-%02d:00' % (timestampDate.year, timestampDate.month, timestampDate.day, timestampDate.hour)
    
'''Simple class for managing, and then writing, job statistics
Use within a `with` block, and `add_stat` or `update_stat` for any additional stats you need.
'''
class Stats:
    def __init__(self, data_source, container):
        self.data_source = data_source
        self.container = container
        self.payload = {}
        
    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if exception_type != None:
            self.exception(exception_type, exception_value, traceback)
        self.end()
        return self
        
    def add_stat(self, key, value):
        self.payload[key] = value
        
    def add_stats(self, dict_of_stats):
        self.payload.update(dict_of_stats)
        
    def update_stat(self, key, update_value, update_fn):
        if key in self.payload:
            self.payload[key] = update_fn(self.payload[key], update_value)
        else:
            self.payload[key] = update_value
            
    def incr_stat(self, key):
        if key in self.payload:
            self.payload[key] += 1
        else:
            self.payload[key] = 1

    def __setitem__(self, key, value):
        self.add_stat(key, value)

    def __getitem__(self, key):
        return self.payload[key]

    def start(self):
        self.payload = {}
        self.payload['Start'] = unix_time_millis(datetime.utcnow())
    
    def exception(self, exception_type, exception_value, traceback):
        self.payload['Exception'] = 'Exception %s: %s: %s' % (str(exception_type), str(exception_value), str(traceback))
        
    def end(self):
        self.payload['End'] = unix_time_millis(datetime.utcnow())
        self.write_stats()
    
    def write_stats(self):
        try:
            self.data_source.saveAsJson(self.payload, self.container, 'stats.json')
        except Exception as e:
            print 'Failed to save stats %s: %s' % (json.dumps(self.payload), str(e))
        
class DataSource:
    def __init(self):
        pass
    
    def download(self, container, path):
        raise NotImplementedError('Abstract')

    def load(self, sparkContext, path, isPrev):
        raise NotImplementedError('Abstract')
        
    def saveAsJson(self, payload, container, path):
        raise NotImplementedError('Abstract')

    def saveAsText(self, rdd, container, path):
        raise NotImplementedError('Abstract')

    def deleteAllBut(self, container, exceptFolderName):
        raise NotImplementedError('Abstract')

'''Simple class for loading and saving to local file system.
'''
class FileDataSource(DataSource):
    def __init__(self):
        pass
   
    def download(self, container, path):
        return
        
    def load(self, sparkContext, folder, path):
        return sparkContext.textFile(folder + path)
        
    def saveAsJson(self, payload, folder, path):
        path = path.replace('(', '').replace(')', '').replace("'", '').replace(',', '/').replace(' ', '')
        path = folder + '/' + path
        d = os.path.dirname(path)
        if not os.path.exists(d):
            os.makedirs(d)
        json_string = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        print json_string
        try:
            with io.open(path, 'w', encoding='utf-8') as file:
                file.write(unicode(json_string))
        except Exception as e:
            print 'Failed to save %s: %s' % (path, str(e))
            raise
            
    def saveAsText(self, rdd, folder, path):
        path = folder + path
        try:
            rdd.saveAsTextFile(path)
        except Exception as e:
            print 'Failed to save %s: %s' % (path, str(e))
            raise

    def deleteAllBut(self, folder, exceptFolderName):
        prev_root = folder
        for name in os.listdir(prev_root):
            if os.path.isdir(prev_root + '/' + name):
                if not name == output_path and not name == now:
                    try:
                        shutil.rmtree(prev_root + '/' + name)
                    except Exception as e:
                        print 'Failed to delete %s: %s' % (name, str(e))
                        raise

def write_partition(data_source, value_mapper=lambda x:x):
    print 'inside write_partition'
    def writer(iterator):
        for row in iterator:
            try: 
                key = row[0]
                if type(row[1]) == list:
                    payload = [value_mapper(elt) for elt in row[1]]
                else:
                    payload = [value_mapper(cur) for cur in row[1].items()]
                output_path = ('/'.join(key) if type(key) == list else str(key).replace('(', '').replace(')', '').replace("'", '').replace(',', '/').replace(' ', '')) + '.json'
                data_source.save(payload, output_path)
            except Exception as e:
                raise TypeError('Failed to cope with %s => %s: %s' % (json.dumps(row[0]), json.dumps(row[1]), str(e)))
        yield None
    return writer

from azure.storage.blob import BlobService
import logging

logger = logging.getLogger(__name__)

class BlobSource(DataSource):
    def __init__(self):
        self.storage_account = getenv('STORAGE_ACCOUNT')
        self.blob_service = BlobService(self.storage_account, getenv('STORAGE_KEY'))
        
    def load(self, sparkContext, container, path):
        path = ('/' if path[0] != '/' else '') + path
        uri = 'wasb://%s@%s.blob.core.windows.net%s' % (container, self.storage_account, path)
        print 'Loading from %s' % uri
        return sparkContext.textFile(uri)

    def download(self, container, path):
        print 'Downloading blob from %s/%s' % (container, path)
        self.blob_service.get_blob_to_path(container, path, path)
        print 'Downloaded blob to ' + path

    def saveAsJson(self, payload, container, path):
        path = path.lstrip('/')
        print path
        print 'Saving to %s/%s' % (container, path)
        json_string = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        try:
            self.blob_service.put_blob(container, path, json_string, 'BlockBlob', x_ms_blob_cache_control='max-age=3600', x_ms_blob_content_type='application/json')
        except Exception as e:
            print 'Failed to save %s/%s: %s' % (container, path, str(e))
            raise

    def saveAsText(self, rdd, container, path):
        path = path.lstrip('/')
        path = '/' + path
        print 'Saving rdd to %s%s' % (container, path)
        uri = 'wasb://%s@%s.blob.core.windows.net%s' % (container, self.storage_account, path)
        try:
            rdd.saveAsTextFile(uri)
        except Exception as e:
            print 'Failed to save %s%s: %s' % (container, path, str(e))
            raise 

    def deleteAllBut(self, container, exceptFolderName):
        print 'deleteAllBut called'
        try:
            bloblistingresult = self.blob_service.list_blobs(container) 
            for i in bloblistingresult:
                print i.name
                if not exceptFolderName in i.name:
                    try:
                        print 'deleting'
                        self.blob_service.delete_blob(container, i.name)
                        print 'deleted'
                    except Exception as e:
                        print 'Failed to delete %s/%s: %s' % (container, i.name, str(e))
                        raise
        except Exception as e:
            print 'Failed to list things in %s: %s' % (container, str(e))
            raise 

class SentimentScorer:

    def __init__(self, lines):
        self.lookup = {}
        self.max_len = 0        
        ensure_package_path()
        from nltk.tokenize import wordpunct_tokenize as tokenize
        for line in lines:
            word_data = json.loads(line)
            # capture both positive and negative, choose one at scoring time
            pos_score, neg_score = word_data['pos'], word_data['neg']            
            terms = [word_data['word']]
            if 'word_ar' in word_data:
                terms.append(word_data['word_ar'])
            if 'word_ur' in word_data:
                terms.append(word_data['word_ur'])
            for term in terms:
                # if a scores exists for a term use the least neutral score
                existing_scores = (0., 0.)
                if term in self.lookup:
                    existing_scores = self.lookup[term]
                self.lookup[term] = (max(pos_score, existing_scores[0]), max(neg_score, existing_scores[1]))
                # update the maximum token length to check
                self.max_len = max(len(tokenize(term)), self.max_len)
    
    def score(self, sentence):
        # track both positive and negative scores for sentence
        pos_score, neg_score = 0., 0.
        # assuming no contextual forms are used for Arabic
        ensure_package_path()
        from nltk.tokenize import wordpunct_tokenize as tokenize
        tokens = tokenize(sentence.lower())
        term_count = 0
        # using nested while loops here to accomodate early termination of 
        # inner loop, and updating the index of the outer loop based on the
        #  number of tokens used in the sub-phrase
        i = 0
        while i < len(tokens):
            matched = False
            j = min(self.max_len, len(tokens) - i)
            # check phrase lengths up to `max_len`
            while j > 0 and (i + j) <= len(tokens):
                sub_tokens = tokens[i : i + j]
                sub_word = ' '.join(sub_tokens)
                # if a match exist for phrase, update scores and counts
                if sub_word in self.lookup:
                    sub_word_scores = self.lookup[sub_word]
                    pos_score += sub_word_scores[0]
                    neg_score += sub_word_scores[1]
                    term_count += 1
                    matched = True
                    i += j
                    break
                j -= 1
            # if not matched, skip token
            if not matched:
                i += 1
        # if no terms matched, or scores are equal, return a neutral score
        if pos_score == neg_score:
            return 0.5
        # if sentence is more positive than negative, use positive word sense
        elif pos_score > neg_score:
            return 0.5 + pos_score / term_count / 2 
        # if sentence is more negative than positive, use negative word sense
        else:
            return 0.5 - neg_score / term_count / 2

def extract_langid(sentence):
    ensure_package_path()
    from langid import langid
    message = sentence['Sentence']
    sentence['Language'] = langid.classify(message)[0]
    return sentence

def download_sentiment_data(data_source):
    input_container = getenv('SENTIMENT_CONTAINER')
    model_path = getenv('SENTIMENT_MODEL')
    data_source.download(input_container, model_path)

def load_sentiment_scorer(data_source):
    download_sentiment_data(data_source)
    model_path = getenv('SENTIMENT_MODEL')
    model_file = open(model_path, 'r')
    model_file_lines = model_file.readlines()
    return SentimentScorer(model_file_lines)

def compute_sentiment(rdd, scorer):
    return rdd.map(lambda x: compute_sentiment_sentence(x, scorer))

def compute_sentiment_sentence(sentence, scorer):
    sentence['Sentiment'] = scorer.score(sentence['Sentence'])
    return sentence

from azure.storage.table import TableService

def get_keywords():    
    # get table service reference
    account_name = getenv('STORAGE_ACCOUNT')
    account_key = getenv('STORAGE_KEY')
    keyword_table = getenv('KEYWORD_TABLE_NAME')
    table_service = TableService(account_name = account_name, account_key = account_key)

    # query all keyword entities
    keywords = table_service.query_entities(keyword_table, filter="PartitionKey eq 'Keyword'")

    # separate each keyword by language
    arKeywords = {}
    enKeywords = {}
    for keyword in keywords:
        # map each keyword by its canonical form (currently lowercase English)
        canonicalKeyword = keyword.en_term.lower()
        # pre-compile regex for each keyword
        arKeywordRegex = create_keyword_regex(keyword.ar_term)
        enKeywordRegex = create_keyword_regex(keyword.en_term)
        arKeywords[canonicalKeyword] = arKeywordRegex
        enKeywords[canonicalKeyword] = enKeywordRegex

    return {'ar': arKeywords, 'en': enKeywords}

def get_keyword_filters():
    # get table service reference
    account_name = getenv('STORAGE_ACCOUNT')
    account_key = getenv('STORAGE_KEY')
    filter_table = getenv('FILTER_TABLE_NAME')
    table_service = TableService(account_name = account_name, account_key = account_key)
    # query all entities
    rows = table_service.query_entities(filter_table)
    # create a list of conjunct regexes
    return [ [ create_keyword_regex(term) for term in json.loads(row.filteredTerms) ] for row in rows ]

def create_keyword_regex(keyword):
    # import nltk
    ensure_package_path()
    from nltk.tokenize import wordpunct_tokenize as tokenize
    tokens = tokenize(keyword)
    pattern = '\\s+'.join(tokens)
    pattern = '\\b%s\\b' % pattern
    return re.compile(pattern, re.I | re.UNICODE)

def extract_keywords(sentence, keywords):
    # check if there are keywords for the sentence language
    language = sentence['Language']
    if language in keywords:
        languageKeywords = keywords[language]
        keywordMatches = []
        if languageKeywords != None:
            message = sentence['Sentence']
            # tokenize the sentence
            for keyword in sorted(languageKeywords):
                keywordRegex = languageKeywords[keyword]
                if keywordRegex.search(message):
                    # if match, add keyword canonical form to list
                    keywordMatches.append(keyword)
        sentence['Keywords'] = keywordMatches
    return sentence

def filter_by_keywords(sentence, filters):
    for conjunction in filters:
        matched = True
        for r in conjunction:
            if not r.search(sentence['Sentence']):
                matched = False
                break
        if matched:
            return False
    return True

def normalize_message(sentence):
    return json.dumps(sentence)

MAX_ZOOM_LEVEL = 16
MAX_DETAIL_LIMIT = 5
MIN_DETAIL_LIMIT = 3

def segment(sentence):
    created = parse(sentence['Created'])
    source = sentence['Source'].encode('ascii')
    sentiment = sentence['Sentiment'] if 'Sentiment' in sentence else 0.0
    payload = [ 1.0, sentiment ]
    # aggregate over each timespan
    for timespanType in ['alltime', 'year', 'month', 'week', 'day', 'hour']:
        timespanLabel = build_timespan_label(timespanType, created)
        # aggregate over each location
        for location in sentence['Locations']:
            try:
                if 'coordinates' not in location:
                    continue
                longitude = location['coordinates'][0]
                latitude = location['coordinates'][1]
                # aggregate over each zoom level
                for zoom in range(15, 16):
                    try:
                        tileId = Tile.tile_id_from_lat_long(latitude, longitude, zoom)
                        keywords = sentence['Keywords']
                        keywordLength = len(keywords)
                        # aggregate over each pair of keywords
                        # since the keywords are sorted, we can choose the combination
                        # of keywords instead of the permutation of keywords
                        for i in range(0, keywordLength):
                            firstKeyword = keywords[i]
                            yield ( ('keyword', source, firstKeyword, None, timespanLabel, tileId), payload )
                            for j in range(i + 1, keywordLength):
                                secondKeyword = keywords[j]
                                yield ( ('keyword', source, firstKeyword, secondKeyword, timespanLabel, tileId ), payload )
                    except ValueError as err:
                        yield ( ('ValueError', latitude, longitude, zoom), str(err) )
            except TypeError as err:
                yield ( 'TypeError', (str(err), location))

def aggregate_by_zoom(data):
    key = data[0]
    value = data[1]
    tileId = key[3]
    tile = Tile.tile_from_tile_id(tileId)
    lowLevel = tile.zoom - MAX_DETAIL_LIMIT
    highLevel = tile.zoom - MIN_DETAIL_LIMIT
    if tile.zoom == MAX_ZOOM_LEVEL - 1:
        highLevel = MAX_ZOOM_LEVEL - 1
    if tile.zoom == MAX_ZOOM_LEVEL:
        highLevel = MAX_ZOOM_LEVEL
    for zoomLevel in range(lowLevel, highLevel):
        bucketTileId = Tile.tile_id_from_lat_long(tile.center_latitude, tile.center_longitude, zoomLevel)
        yield ( (key[0], key[1], key[2], bucketTileId), { tileId: value } )
        
def merge_sentiment(a, b):
    result = []

    # Save count variables
    aCount = a[0]
    bCount = b[0]
    totalCount = aCount + bCount
    result.append(totalCount)

    # Compute weighted average for remaining values (except last)
    for idx in range(1, len(a)):
        result.append((aCount * a[idx] + bCount * b[idx]) / totalCount)
    
    return result

def increment(kv):
    key = kv[0]
    value = kv[1]
    v1 = value[0]
    v2 = value[1]
    result = []
    if v1 == None and not v2 == None:
        result = v2
    if v2 == None and not v1 == None:
        result = v1
    if not v1 == None and not v2 == None:
        result = merge_sentiment(v1, v2)
    stripped_key = tuple(k for k in key[1:])
    return (stripped_key, result)

def normalize_keys(x):
    key = x[0]
    value = x[1]
    stripped_key = tuple(k for k in key[1:])
    return (stripped_key, value)

def tuple_loader(line):
    return [eval(line)]

def ensure_package_path():
    # update patch for local packages
    import sys
    packagePath = os.path.join(os.getcwd(), 'artifacts.zip/site-packages')
    if not packagePath in sys.path:
        sys.path.append(packagePath)

def normalize_source(source):
    if (source.startswith('facebook-')):
        return 'facebook'
    
    return source

def get_sources(x):
    return [normalize_source(x['Source']), 'all']

def matches_source(source):
    if source == 'all':
        return lambda x : True
    
    return lambda sentence : normalize_source(sentence['Source']) == source

def get_timespans(x):
    created = parse(x['Created'])
    return [build_timespan_label(timespanType, created) for timespanType in ["month", "week", "day"]]

def matches_timespan(timespan):
    if timespan == 'alltime':
        return lambda x : True
    
    timespanType = timespan[0:timespan.index('-')]
    return lambda sentence : build_timespan_label(timespanType, parse(sentence['Created'])) == timespan

def split_keywords(x):
    for k in x['Keywords']:
        yield (('Keywords', k), 1)

def by_hour(x):
    created = parse(x['Created'])
    datetime_hour = datetime(created.year, created.month, created.day, created.hour)
    yield (unix_time_millis(datetime_hour), x)

def create_agg_all(merge_value):
    agg = {}
    return lambda x: merge_value(copy.deepcopy(agg), x)

def get_section_map():
    return { 'Keywords': 'kw-' }

def merge_sentence_all(agg, s):
    section_map = get_section_map()
    for t in section_map.keys():
        for k in s[t]:
            key = section_map[t] + k
            if key not in agg:
                agg[key] = { 'mag': 0, 'pos': 0 }
            agg[key]['mag'] += 1
            is_neg = 'Sentiment' in s and s['Sentiment'] < 0.5
            if not is_neg:
                agg[key]['pos'] += 1
    return agg

def merge_agg(agg1, agg2):
    for key in set(agg1.keys()).union(set(agg2.keys())):
        if key in agg2:
            if key not in agg1:
                agg1[key] = {'mag': 0, 'pos': 0}
            agg1[key]['mag'] += agg2[key]['mag']
            agg1[key]['pos'] += agg2[key]['pos']
    return agg1

def build_agg(timespan, topN, kw = None):
    section_map = get_section_map()
    labels = [ "x" ]
    for top_count in topN:
        top = top_count[0]
        labels.append(section_map[top[0]] + top[1])
    if kw != None and kw not in labels:
        labels.append(kw)

    def builder(vals_itr):
        agg = {
            'labels': labels,
            'graphData': []
        }
        for val in vals_itr:
            cur = [ val[0] ]
            for label in labels:
                if label in val[1]:
                    cur_val = val[1][label]
                    cur.extend( [cur_val['mag'], cur_val['pos']] )
                else:
                    cur.extend([0,0])
            agg['graphData'].append(cur)
        return agg
    return builder

def build_aggs(timespan, top_5, kws, vals):
    yield ((timespan, 'top5'), build_agg(timespan, top_5)(vals))
    for kw in kws:
        yield ((timespan, kw), build_agg(timespan, top_5, kw)(vals))

def main(sc):

    is_incremental = True
    is_file_data_source = False
    tile_path = getenv('TILE_INPUT_PATTERN')
    timeseries_path = getenv('TIMESERIES_INPUT_PATTERN')

    # create path
    now = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S')
    prevrdd_path = '/' + now
    mssgrdd_path = prevrdd_path
    nextrdd_path = '/associations/' + now

    input_container = getenv('INPUT_CONTAINER')
    tile_output_container = getenv('TILE_OUTPUT_CONTAINER')
    timeseries_output_container = getenv('TIMESERIES_OUTPUT_CONTAINER')
    message_container = getenv('MESSAGE_CONTAINER')
    tile_prev_container = getenv('TILE_PREV_CONTAINER')

    data_source = None

    if is_file_data_source:
        data_source = FileDataSource()
    else:
        print 'using azure'
        data_source = BlobSource()
        
    with Stats(data_source, tile_output_container) as stats:  
        # get the list of keywords from Azure Table Storage
        keywords = get_keywords()
        
        # get noisy keyword filters
        keyword_filters = get_keyword_filters()

        # load the RDDs from storage
        lines = data_source.load(sc, input_container, tile_path)

        # filter out lines without `Created` field
        input_data = filter_to_valid(lines, stats, "tile")

        # extract language from each line
        input_data_lang = input_data.map(extract_langid)

        # filter out noisy keywords
        filtered_input_data = input_data_lang.filter(lambda x: filter_by_keywords(x, keyword_filters))

        # compute sentiment for each line
        scorer = load_sentiment_scorer(data_source)
        input_data_sentiment = compute_sentiment(filtered_input_data, scorer)

        # extract keywords from each line
        input_data_keywords = input_data_sentiment.map(lambda x: extract_keywords(x, keywords)).filter(has_keywords)
        input_data_keywords.cache()

        # dump lines with language, sentiment, and keywords
        projected_messages = input_data_keywords.map(normalize_message)
        data_source.saveAsText(projected_messages, message_container, mssgrdd_path)

        # map to keyword pairs, timespans, and tile IDs
        segmented = input_data_keywords.flatMap(segment)
        segmented.cache()

        # filter out any errors
        valid_segmented = segmented.filter(lambda x: x[0] != 'TypeError' and x[0][0] != 'ValueError')
        stats.add_stat('segment_errors', 
                       segmented.filter(lambda x: x[0] == 'TypeError' or x[0][0] == 'ValueError').count())
        
        # reduce on each keyword pair, timespan, and tile ID
        reduced_segmented = valid_segmented.reduceByKey(merge_sentiment)

        output_data = reduced_segmented
        output_data.cache()
        
        if not is_incremental:
            # save RDDs for all data
            data_source.saveAsText(output_data, tile_prev_container, prevrdd_path)
            # remove 'keyword' discriminator from data
            normalized = output_data.map(normalize_keys)
            # save RDDs for new / updated data
            data_source.saveAsText(normalized,  tile_output_container, nextrdd_path)

        else:
            loadrdd_path = '/*/part*'

            # load previous data
            prev_rdd = data_source.load(sc, tile_prev_container, loadrdd_path).flatMap(tuple_loader)
            prev_rdd.cache()

            # join previous data with new data and merge
            merged_rdd = output_data.fullOuterJoin(prev_rdd).map(increment)

            # TODO: filter out irrelevant keys (previous daily / hourly, out-of-date weeks, out-of-date months, etc.)

            # save RDDs for all data
            data_source.saveAsText(merged_rdd, tile_prev_container, prevrdd_path)

            # compute new / updated data with left outer join
            new_rdd = output_data.leftOuterJoin(prev_rdd).map(increment)
            # save RDDs for new / updated data
            data_source.saveAsText(new_rdd, tile_output_container, nextrdd_path)

            # delete previous load folders from previous container
            # rather than deleting these, we should periodically back these up
            # if a run gets botched in the middle of processing, we probably
            # do not want to re-run all data for all time
            data_source.deleteAllBut(tile_prev_container, now)
            
#        # read all messages
#        messages_lines = data_source.load(sc, message_container, timeseries_path)
#        all_messages = filter_to_valid(messages_lines, stats, "timeseries")
#        all_messages.cache()
#        
#        # get distinct sources
#        sources = all_messages.flatMap(get_sources).distinct().collect()
#        for source in sources:
#            # filter for single source
#            source_filtered = all_messages.filter(matches_source(source))
#            # get distinct timespaces
#            timespans = source_filtered.flatMap(get_timespans).distinct().collect()
#            recs_by_timespan = {}
#            for timespan in timespans:
#                # filter for single timespan
#                filtered = source_filtered.filter(matches_timespan(timespan))
#                filtered.cache()
#                
#                recs_by_timespan[timespan] = filtered.count()
#                
#                # flat map all keywords
#                sectioned = filtered.flatMap(split_keywords)
#                # count keywords
#                by_key = sectioned.reduceByKey(lambda a,b: a + b)
#                # take top 5 keyword counts
#                top_5 = by_key.takeOrdered(5, key=lambda x: -x[1])
#
#                # flat my messages by hour
#                filtered_by_hour = filtered.flatMap(by_hour)
#
#                # count message keywords by hour
#                hourly_counts = filtered_by_hour.combineByKey(create_agg_all(merge_sentence_all), merge_sentence_all, merge_agg)
#                # sort aggregated counts by hour
#                ordered_counts = hourly_counts.sortByKey().groupBy(lambda x: timespan)
#                # get distinct hour keys
#                all_keys = hourly_counts.flatMap(lambda x: x[1].keys()).distinct().collect()
#                # build hourly aggregation graph
#                all_kw_groups = ordered_counts.flatMap(lambda x: build_aggs(x[0], top_5, all_keys, x[1]))
#                # write data to blob storage
#                all_kw_groups.foreach(lambda x: data_source.saveAsJson(x[1], timeseries_output_container, '%s/%s/%s.json' % (source, x[0][0], x[0][1])))
#            stats.add_stat(source + '_records_by_timespan', recs_by_timespan)

if __name__ == '__main__':
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    import sys
    # Keep in sync with 
    os.environ['STORAGE_ACCOUNT'] = str(sys.argv[1])
    os.environ['STORAGE_KEY'] = str(sys.argv[2])
    os.environ['INPUT_CONTAINER'] = str(sys.argv[3])
    os.environ['TILE_INPUT_PATTERN'] = str(sys.argv[4])
    os.environ['TILE_PREV_CONTAINER'] = str(sys.argv[5])
    os.environ['TILE_OUTPUT_CONTAINER'] = str(sys.argv[6])
    os.environ['MESSAGE_CONTAINER'] = str(sys.argv[7])
    os.environ['TIMESERIES_INPUT_PATTERN'] = str(sys.argv[8])
    os.environ['TIMESERIES_OUTPUT_CONTAINER'] = str(sys.argv[9])
    os.environ['KEYWORD_TABLE_NAME'] = str(sys.argv[10])
    os.environ['FILTER_TABLE_NAME'] = str(sys.argv[11])
    os.environ['SENTIMENT_CONTAINER'] = str(sys.argv[12])
    os.environ['SENTIMENT_MODEL'] = str(sys.argv[13])
    main(sc)

