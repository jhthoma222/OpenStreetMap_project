# Audting Street names

import unicodecsv
import pprint
import re
import xml.etree.cElementTree as ET
import cerberus
import schema
from collections import defaultdict

OSM_FILE =  "D:\Desktop\WGU Projects\data_analyst_nanodegree\data_wrangling\inglewood_openstreetmap\inglewood_map"


street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

# valid street names
# Added 'Cienega' and 'Thornburn' because they are valid street names.

expected_street_types = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road",
            "Trail", "Parkway", "Commons", "Northeast", "South", "Southeast", "Southwest","Northwest",
            "East", "West", "North", "Highway", "Way", "Terrace", "Freeway", "Highway", "Point", 
           "Alley", "Circle", "Cienega", "Thornburn"]

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected_street_types:
            street_types[street_type].add(street_name)


def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")


def audit(osmfile):
    osm_file = open(osmfile, "rb")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
    osm_file.close()
    return street_types


def update_street_name(value):
    m = street_type_re.search(value)
    if m:
        if m.group() in street_type_mapping:
            startpos = value.find(m.group())
            value = value[:startpos] + street_type_mapping[m.group()]
        return value
    else:
        return None

st_types = audit(OSM_FILE)
pprint.pprint(dict(st_types))
