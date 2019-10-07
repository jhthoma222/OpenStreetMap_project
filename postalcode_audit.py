'''
The following functions are performed to audit the postal codes 
vs 'postcode_re'.
Print the postal codes that do not match
'''

import unicodecsv
import pprint  
import re
import xml.etree.cElementTree as ET
import cerberus
import schema
from collections import defaultdict

OSM_FILE =  "D:\Desktop\WGU Projects\data_analyst_nanodegree\data_wrangling\inglewood_openstreetmap\inglewood_map"

post_code_re = re.compile(r'^\d{5}$')
"""Regular expression to recognise valid post codes.
"""

def audit_post_codes(bad_post_codes, post_code):
    """Build a set of bad post codes.
    Args:
        bad_post_codes (set): bad post codes.
        post_code (str): post code data.
    """
    m = post_code_re.match(post_code)
    if not m:
        bad_post_codes.add(post_code)

def is_post_code(elem):
    """Search for postal code data.
    Args:
        elem (obj): element found using ET.iterparse().
    Returns:
        True for 'postcode', Anything other than 'postcode' is False.
    """
    return (elem.attrib['k'] == "addr:postcode")


def audit(osmfile):
    """Audit post code data.
    Args:
        osmfile (obj): OSM (XML) file to audit.
    Returns:
        bad_post_codes (set): bad post codes.
    """
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