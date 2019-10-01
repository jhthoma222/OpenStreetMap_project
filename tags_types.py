import xml.etree.cElementTree as ET
import pprint
import re

"""
Counting each tag category in a dictionary.  Check 'lower', 'lower_colon' and
'problemchars'
"""

OSM_FILE =  "D:\Desktop\WGU Projects\data_analyst_nanodegree\data_wrangling\inglewood_openstreetmap\inglewood_map"
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')


def key_type(element, keys):
    if element.tag == "tag":
        # YOUR CODE HERE
        if lower.search(element.attrib['k']):
            keys['lower'] += 1
        elif lower_colon.search(element.attrib['k']):
            keys['lower_colon'] += 1
        elif problemchars.search(element.attrib['k']):
            keys['problemchars'] += 1
        else:
            keys['other'] += 1

    return keys



def process_map(filename):
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    for _, element in ET.iterparse(filename):
        keys = key_type(element, keys)

    return keys

keys = process_map(OSM_FILE)
pprint.pprint(keys)
