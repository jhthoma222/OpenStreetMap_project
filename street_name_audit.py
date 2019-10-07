'''
The following functions are performed to audit the street names 
vs 'street+type_re and the 'expected_street_types'.
Print the street names that do not match
'''

import unicodecsv
import pprint
import re
import xml.etree.cElementTree as ET
import cerberus
import schema
from collections import defaultdict

OSM_FILE =  "D:\Desktop\WGU Projects\data_analyst_nanodegree\data_wrangling\inglewood_openstreetmap\inglewood_map"


street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
"""Regular expression to recognise street type."""

# valid street names
# Added 'Cienega' and 'Thornburn' because they are valid street names.
expected_street_types = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road",
            "Trail", "Parkway", "Commons", "Northeast", "South", "Southeast", "Southwest","Northwest",
            "East", "West", "North", "Highway", "Way", "Terrace", "Freeway", "Highway", "Point", 
           "Alley", "Circle", "Cienega", "Thornburn"]

def audit_street_type(street_types, street_name):
    """Build defaultdict of unexpected street types.
    Args:
        street_types (dict): defaultdict(set) of unexpected street types.
        street_name (str): street name data.
    """

    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected_street_types:
            street_types[street_type].add(street_name)


def is_street_name(elem):
    """Search for street name data.
    Args:
        elem (obj): element found using ET.iterparse().
    Returns:
        True for 'street', Anything other than 'street' is False.
    """
    return (elem.attrib['k'] == "addr:street")

def audit(osmfile):
    """Audit street type.
    Args:
        osmfile (obj): OSM (XML) file to audit.
    Returns:
        street_types (dict): defaultdict(set) of unexpected street types.
    """
    osm_file = open(osmfile, "rb")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
    osm_file.close()
    return street_types


st_types = audit(OSM_FILE)
pprint.pprint(dict(st_types))
