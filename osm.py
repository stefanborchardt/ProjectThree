# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 13:12:57 2015

@author: Stefan
"""
import collections
import pprint
import re
import xml.sax 
import xml.sax.saxutils
import codecs
import json
import pymongo
import math

# global variables only for statistics output from ContentHandlers
other_keys = collections.Counter()
problemchars = collections.Counter()
nodes_processed = collections.Counter()
unexpected = collections.Counter()
address_keys = collections.Counter()
excl_nodes = []
nodes_extracted = collections.Counter()

# for relevant node names
toplevel = ['node', 'way']
nodes = ['node', 'way', 'tag', 'nd']
    
class OsmHandler(xml.sax.ContentHandler):
    ''' Transforms OSM XML to JSON format as specified in lesson 6. Checks
    for problematic `tag` `key` values, non 5-digit postcodes, alternative notations 
    of postcode and nodes containing a tag like 'fixme', which will be skipped.
    
    '''
    problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
    lower = re.compile(r'^[a-zA-Z0-9_:\-]*$')

    created = ['version', 'changeset', 'timestamp', 'user', 'uid']
    tag_keys_exclude = ['fixme', 'FIXME'] 
    
    def __init__(self, filename):
        xml.sax.ContentHandler.__init__(self)
        self.jfile = codecs.open(filename + '.json', 'w')
        self.reset()
        
    def __del__(self):
        self.jfile.close()
    
    def reset(self):
        self.element = {} # hold data for Json output
        self.crea_dict = {}
        self.pos = [0, 0]
        self.addr = {}
        self.refs = []
        self.write_children = True #only write children of relevant top level nodes
        self.discard = False # do not write top level nodes with excluded keys
    
    def postcode_ok(self, val):
        return len(val) == 5 and not math.isnan(int(val))
        
    def startElement(self, name, attrs):
        if name not in nodes:
            self.write_children = False
            return # redundant, only to speed things up 
        if name in toplevel:
            # start of relevant elements
            self.reset()
            nodes_processed[name] += 1
            self.element['type'] = name
            # handle attributes    
            for attr in attrs.getNames():
                if attr in self.created:
                    self.crea_dict[attr] = attrs[attr]
                    self.element['created'] = self.crea_dict
                elif attr == 'lat':
                    self.pos[0] = float(attrs['lat'])
                    self.element['pos'] = self.pos
                elif attr == 'lon':
                    self.pos[1] = float(attrs['lon'])
                    self.element['pos'] = self.pos
                else:
                    self.element[attr] = attrs[attr]
        elif self.write_children and (name == 'tag'):
            nodes_processed[name] += 1
            key = attrs['k']
            if self.lower.match(key) and not self.problemchars.match(key):
                val = attrs['v']
                if key.startswith('addr:'):
                    # handle address data
                    if key.count(':') == 1: 
                        if key == 'addr:street' \
                        and (val.endswith('str.') or val.endswith('strasse')):    
                                # Only note the common abbreviations or misspellings.
                                # Other attempts for fixing are really, really hard
                                # in German, there's a bot scanning OSM regularly
                                # for that, though.
                                unexpected[val] += 1
                        elif key == 'addr:postcode' and not self.postcode_ok(val):
                                unexpected[val] += 1
                        else:
                            # starts with 'addr:'
                            adr_key = key.split(':')[1]
                            address_keys[adr_key] += 1
                            self.addr[adr_key] = val
                            self.element['address'] = self.addr
                elif key == 'postal_code':
                    # move postcode in alternative notation to standard
                    if not self.postcode_ok(val):
                        unexpected[val] += 1
                    else:
                        address_keys['postcode'] += 1
                        self.addr['postcode'] = val
                        self.element['address'] = self.addr
                elif key not in self.tag_keys_exclude:
                    # does not start with 'addr:' but is kept
                    other_keys[key] += 1
                    self.element[key] = val
                else:
                    # discard node if it contains a tag to indicate exclusion
                    self.discard = True   
                    self.element[key] = val
                    excl_nodes.append(self.element)
            else:
                # contains problem char
                problemchars[key] += 1
        elif self.write_children and (name == 'nd'):
            nodes_processed[name] += 1
            ref = attrs['ref']
            self.refs.append(ref)
            self.element['node_refs'] = self.refs     
            
            
    def endElement(self, name):
        if name in toplevel and not self.discard:
            # end of relevant elements, write
            self.jfile.write(json.dumps(self.element) + '\n')
                

class ExtractHandler(xml.sax.ContentHandler):
    ''' Extracts a sample from a large OSM XML. Will only pay attention to
    `node`, `way`, `tag` and `nd` nodes. A sequence of `tag_count` of each `node` and 
    `way` tags will be written to a file named `filename` _extract.osm, including
    their child nodes. A number of nodes to skip initially can be 
    specified by `skip_count`.
    '''
    
    def __init__(self, filename, tag_count, skip_count):
        xml.sax.ContentHandler.__init__(self)
        self.skipped = collections.Counter()
        self.max_count = tag_count
        self.to_skip = skip_count
        self.write_children = False
        self.efile = open(filename + '_extract.osm', 'wb')
        self.out = xml.sax.saxutils.XMLGenerator(self.efile)
        self.out.startDocument()
        attrs = xml.sax.xmlreader.AttributesImpl({})
        self.out.startElement('osm', attrs)
        
    def __del__(self):
        self.out.endElement('osm')
        self.out.endDocument()
        self.efile.close()
   
    def startElement(self, name, attrs):
        if name not in nodes:
            return
        if name in toplevel:
            if self.skipped[name] < self.to_skip:
                self.skipped[name] += 1
            elif nodes_extracted[name] < self.max_count:
                self.write_children = True
                nodes_extracted[name] += 1
                self.out.startElement(name, attrs)
            else:
                self.write_children = False
        elif self.write_children:
            nodes_extracted[name] += 1
            self.out.startElement(name, attrs)
      
    def endElement(self, name):
        if name not in nodes:
            return
        if self.write_children:
                self.out.endElement(name)
       


def extract(filename, tag_count, skip_count=0):
    eh = ExtractHandler(filename, tag_count, skip_count)
    ehparser = xml.sax.make_parser()
    ehparser.setContentHandler(eh)

    with open(filename + '.osm', 'rb') as f:
        ehparser.parse(f)
        
    print '\n============== nodes extracted'
    pprint.pprint(dict(nodes_extracted))
    

def convert(filename):
    oh = OsmHandler(filename)
    ohparser = xml.sax.make_parser()
    ohparser.setContentHandler(oh)

    with open(filename + '.osm', 'rb') as f:
        ohparser.parse(f)
        
    print '\n============== excluded nodes'
    pprint.pprint(list(excl_nodes))    
    print '\n============== address keys'
    pprint.pprint(dict(address_keys))
    print '\n============== other keys'
    print(other_keys.most_common(300))
    print '\n============== problem characters'
    pprint.pprint(dict(problemchars))
    print '\n============== unexpected streets or postcodes'
    pprint.pprint(dict(unexpected))
    print '\n============== nodes processed'
    pprint.pprint(dict(nodes_processed))


def query(db):
    client = pymongo.MongoClient()
    db_collection = client[db][db]
    ########################
    print 'number of documents: ' + str(db_collection.find().count())
    print 'number of nodes: ' + str(db_collection.find({"type":"node"}).count())
    print 'number of ways: ' + str(db_collection.find({"type":"way"}).count())
    print 'number of unique users: ' + str(len(db_collection.distinct('created.user')))
    ########################
    # postcode ranking
    pipeline = [{'$match': {'address.postcode': {'$exists': True}}}, 
                {'$group': {'_id': '$address.postcode', 
                            'count': {'$sum': 1}}},
                
                {'$sort': {'count': 1}} ]
    result = [doc for doc in db_collection.aggregate(pipeline)]
    pprint.pprint(result)
    ########################
    # alphabetic list of amenities that are tagged more than 100 times
    pipeline = [{'$match': {'amenity': {'$exists': True}}}, 
                {'$group': {'_id': '$amenity', 
                            'count': {'$sum': 1}}},
                {'$match': {'count': {'$gt': 100}}},
                {'$project': {'_id': True}},
                {'$sort': {'_id': 1}} ]
    result = [doc for doc in db_collection.aggregate(pipeline)]
    pprint.pprint(result)
    ########################
    # the top 10 amenities
    pipeline = [{'$match': {'amenity': {'$exists': True}}}, 
                {'$group': {'_id': '$amenity', 
                            'count': {'$sum': 1}}},
                {'$sort': {'count': -1}},
                {'$limit': 10}]
    result = [doc for doc in db_collection.aggregate(pipeline)]
    pprint.pprint(result)
    ########################
    #get the positions of police, where available
    pipeline = [{'$match': {'$and': [{'amenity': 'police'}, 
                                     {'pos': {'$exists': True}}]}}, 
                {'$project': {'_id': '$pos'}}]
    coords = [doc['_id'] for doc in db_collection.aggregate(pipeline)]
    amenities = collections.Counter() 
    # for all positions, get the amenities within 1km that are not the police themself
    for coord in coords:
        pipeline = [{'$geoNear': {
                        'near': {'type': 'Point', 'coordinates': coord},
                        'distanceField': "distance",
                        'maxDistance': 1000,
                        'spherical': True}},
                    {'$match': {'$and': [{'amenity': {'$exists': True}},
                                         {'distance': {'$gt': 0}}]}}]
        result = [doc['amenity'] for doc in db_collection.aggregate(pipeline)]
        # count them, not unique 
        for amenity in result:
            amenities[amenity] += 1
    pprint.pprint(amenities.most_common(10))

          
if __name__ == '__main__':
    #uncomment for processing a small part of the data only
    #extract('berlin_germany', 100000, 2000)
    #convert('berlin_germany_extract')

    #uncomment for converting the complete dataset
    #convert('berlin_germany') 

    #uncomment to query mongodb on local machine
    query('berlin')          