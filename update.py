import unicodecsv
import pprint
import re
import xml.etree.cElementTree as ET
import cerberus
import schema
from collections import defaultdict

OSM_FILE =  "ing_intermediate_sample.osm"



# Regex to match correct street name types  
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

# List of valid street names
expected_street_types = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road",
            "Trail", "Parkway", "Commons", "Northeast", "South", "Southeast", "Southwest","Northwest",
            "East", "West", "North", "Highway", "Way", "Terrace", "Freeway", "Highway", "Point", 
           "Alley", "Circle", "426", "Ic"]

street_type_mapping = { "Ave": "Avenue",
            "Blvd": "Boulevard",
            "Bld.": "Boulevard",
            "Blv.": "Boulevard",
            "St": "Street"
          }

def update_street_name(value):
    '''This function checks to see if the street names matches the correct pattern and type.  
        Args:
        Returns:
    '''
    m = street_type_re.search(value)
    if m:
        if m.group() in street_type_mapping:
            startpos = value.find(m.group())
            value = value[:startpos] + street_type_mapping[m.group()]
        return value
    else:
        return None

    
# Audit and update postal codes
postcode_re = re.compile(r'^\d{5}$')

def audit_post_codes(post_codes, post_code):
    m = post_code_re.match(post_code)
    if not m:
        post_codes.add(post_code)

def is_postcode(elem):
    return (elem.attrib['k'] == "addr:postcode")
def audit(osmfile):
    osm_file = open(osmfile, "rb")
    post_codes = set()
    bad_postcode = set()
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
               if is_postcode(tag):
                    m = postcode_re.search(tag.attrib['v'])
                    if m:
                        post_codes.add(tag.attrib['v'])  
                    else:
                        bad_postcode.add(tag.attrib['v'])

    osm_file.close()
    return (bad_postcode)

# Split the post code at the "-" and return the first part
def update_postcode(bad_postcode):
    postcode = bad_postcode.split("-")[0]
    return postcode
