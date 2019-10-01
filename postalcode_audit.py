# Audting Postal Codes

import unicodecsv
import pprint  
import re
import xml.etree.cElementTree as ET
import cerberus
import schema
from collections import defaultdict

OSM_FILE =  "D:\Desktop\WGU Projects\data_analyst_nanodegree\data_wrangling\inglewood_openstreetmap\inglewood_map"

post_code_re = re.compile(r'^\d{5}$')

def audit_post_codes(bad_post_codes, post_code):
    m = post_code_re.match(post_code)
    if not m:
        bad_post_codes.add(post_code)

def is_post_code(elem):
    return (elem.attrib['k'] == "addr:postcode")

def audit(osmfile):
    osm_file = open(osmfile, "rb")
    bad_post_codes = set()
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_post_code(tag):
                    audit_post_codes(bad_post_codes, tag.attrib['v'])

    osm_file.close()
    return bad_post_codes



postal_types = audit(OSM_FILE)
pprint.pprint(postal_types)