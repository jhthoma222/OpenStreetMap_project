import xml.etree.cElementTree as ET
import pprint

# Iterative parsing to process the map file and find out the tags.

OSM_FILE =  "D:\Desktop\WGU Projects\data_analyst_nanodegree\data_wrangling\inglewood_openstreetmap\inglewood_map"

def count_tags(filename):
    tags = {}
    for event, elem in ET.iterparse(filename):
        if elem.tag not in tags:
            tags[elem.tag] = 1
        else:
            tags[elem.tag] += 1
    return tags

tags = count_tags(OSM_FILE)
pprint.pprint(tags)