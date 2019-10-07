# Preparing the database - SQL
# File = ing.import.py


"""
After auditing is complete the next step is to prepare the data to be inserted into a SQL database.
To do so you will parse the elements in the OSM XML file, transforming them from document format to
tabular format, thus making it possible to write to .csv files.  These csv files can then easily be
imported to a SQL database as tables.

The process for this transformation is as follows:
- Use iterparse to iteratively step through each top level element in the XML
- Shape each element into several data structures using a custom function
- Utilize a schema and validation library to ensure the transformed data is in the correct format
- Write each data structure to the appropriate .csv files

We've already provided the code needed to load the data, perform iterative parsing and write the
output to csv files. Your task is to complete the shape_element function that will transform each
element into the correct format. To make this process easier we've already defined a schema (see
the schema.py file in the last code tab) for the .csv files and the eventual tables. Using the 
cerberus library we can validate the output against this schema to ensure it is correct.

## Shape Element Function
The function should take as input an iterparse Element object and return a dictionary.

### If the element top level tag is "node":
The dictionary returned should have the format {"node": .., "node_tags": ...}

The "node" field should hold a dictionary of the following top level node attributes:
- id
- user
- uid
- version
- lat
- lon
- timestamp
- changeset
All other attributes can be ignored

The "node_tags" field should hold a list of dictionaries, one per secondary tag. Secondary tags are
child tags of node which have the tag name/type: "tag". Each dictionary should have the following
fields from the secondary tag attributes:
- id: the top level node id attribute value
- key: the full tag "k" attribute value if no colon is present or the characters after the colon if one is.
- value: the tag "v" attribute value
- type: either the characters before the colon in the tag "k" value or "regular" if a colon
        is not present.

Additionally,

- if the tag "k" value contains problematic characters, the tag should be ignored
- if the tag "k" value contains a ":" the characters before the ":" should be set as the tag type
  and characters after the ":" should be set as the tag key
- if there are additional ":" in the "k" value they and they should be ignored and kept as part of
  the tag key. For example:

  <tag k="addr:street:name" v="Lincoln"/>
  should be turned into
  {'id': 12345, 'key': 'street:name', 'value': 'Lincoln', 'type': 'addr'}

- If a node has no secondary tags then the "node_tags" field should just contain an empty list.

The final return value for a "node" element should look something like:

{'node': {'id': 757860928,
          'user': 'uboot',
          'uid': 26299,
       'version': '2',
          'lat': 41.9747374,
          'lon': -87.6920102,
          'timestamp': '2010-07-22T16:16:51Z',
      'changeset': 5288876},
 'node_tags': [{'id': 757860928,
                'key': 'amenity',
                'value': 'fast_food',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'cuisine',
                'value': 'sausage',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'name',
                'value': "Shelly's Tasty Freeze",
                'type': 'regular'}]}

### If the element top level tag is "way":
The dictionary should have the format {"way": ..., "way_tags": ..., "way_nodes": ...}

The "way" field should hold a dictionary of the following top level way attributes:
- id
-  user
- uid
- version
- timestamp
- changeset

All other attributes can be ignored

The "way_tags" field should again hold a list of dictionaries, following the exact same rules as
for "node_tags".

Additionally, the dictionary should have a field "way_nodes". "way_nodes" should hold a list of
dictionaries, one for each nd child tag.  Each dictionary should have the fields:
- id: the top level element (way) id
- node_id: the ref attribute value of the nd tag
- position: the index starting at 0 of the nd tag i.e. what order the nd tag appears within
            the way element

The final return value for a "way" element should look something like:

{'way': {'id': 209809850,
         'user': 'chicago-buildings',
         'uid': 674454,
         'version': '1',
         'timestamp': '2013-03-13T15:58:04Z',
         'changeset': 15353317},
 'way_nodes': [{'id': 209809850, 'node_id': 2199822281, 'position': 0},
               {'id': 209809850, 'node_id': 2199822390, 'position': 1},
               {'id': 209809850, 'node_id': 2199822392, 'position': 2},
               {'id': 209809850, 'node_id': 2199822369, 'position': 3},
               {'id': 209809850, 'node_id': 2199822370, 'position': 4},
               {'id': 209809850, 'node_id': 2199822284, 'position': 5},
               {'id': 209809850, 'node_id': 2199822281, 'position': 6}],
 'way_tags': [{'id': 209809850,
               'key': 'housenumber',
               'type': 'addr',
               'value': '1412'},
              {'id': 209809850,
               'key': 'street',
               'type': 'addr',
               'value': 'West Lexington St.'},
              {'id': 209809850,
               'key': 'street:name',
               'type': 'addr',
               'value': 'Lexington'},
              {'id': '209809850',
               'key': 'street:prefix',
               'type': 'addr',
               'value': 'West'},
              {'id': 209809850,
               'key': 'street:type',
               'type': 'addr',
               'value': 'Street'},
              {'id': 209809850,
               'key': 'building',
               'type': 'regular',
               'value': 'yes'},
              {'id': 209809850,
               'key': 'levels',
               'type': 'building',
               'value': '1'},
              {'id': 209809850,
               'key': 'building_id',
               'type': 'chicago',
               'value': '366409'}]}
"""

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
"""Regular expression to recognise problem characters"""

SCHEMA = {
    'node': {
        'type': 'dict',
        'schema': {
            'id': {'required': True, 'type': 'integer', 'coerce': int},
            'lat': {'required': True, 'type': 'float', 'coerce': float},
            'lon': {'required': True, 'type': 'float', 'coerce': float},
            'user': {'required': True, 'type': 'string'},
            'uid': {'required': True, 'type': 'integer', 'coerce': int},
            'version': {'required': True, 'type': 'string'},
            'changeset': {'required': True, 'type': 'integer', 'coerce': int},
            'timestamp': {'required': True, 'type': 'string'}
        }
    },
    'node_tags': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'id': {'required': True, 'type': 'integer', 'coerce': int},
                'key': {'required': True, 'type': 'string'},
                'value': {'required': True, 'type': 'string'},
                'type': {'required': True, 'type': 'string'}
            }
        }
    },
    'way': {
        'type': 'dict',
        'schema': {
            'id': {'required': True, 'type': 'integer', 'coerce': int},
            'user': {'required': True, 'type': 'string'},
            'uid': {'required': True, 'type': 'integer', 'coerce': int},
            'version': {'required': True, 'type': 'string'},
            'changeset': {'required': True, 'type': 'integer', 'coerce': int},
            'timestamp': {'required': True, 'type': 'string'}
        }
    },
    'way_nodes': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'id': {'required': True, 'type': 'integer', 'coerce': int},
                'node_id': {'required': True, 'type': 'integer', 'coerce': int},
                'position': {'required': True, 'type': 'integer', 'coerce': int}
            }
        }
    },
    'way_tags': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'id': {'required': True, 'type': 'integer', 'coerce': int},
                'key': {'required': True, 'type': 'string'},
                'value': {'required': True, 'type': 'string'},
                'type': {'required': True, 'type': 'string'}
            }
        }
    }
}


# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']


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
    """update street name values based on mapping.
    Args:
        value (str): original street name value.
    Returns:
        value (str): cleaned street name value.
        None if data does not match expected format.
    """
    m = street_type_re.search(value)
    if m:
        if m.group() in street_type_mapping:
            startpos = value.find(m.group())
            value = value[:startpos] + street_type_mapping[m.group()]
        return value
    else:
        return None


def update_postcode(bad_postcode):
    """update postcode based on format.  
    Args:
        bad_postcode (str): bad postcode to be split at the "-"
    Returns:
        postcode: cleaned postcode.  only the first five digits
    """
    postcode = bad_postcode.split("-")[0]
    return postcode

    
def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict.
    Args:
        element (obj): element found using ET.iterparse().
        node_attr_fields (list): node attribute fields to be passed to output dict
        way_attr_fields (list): way attribute fields to be passed to output dict
        problem_chars (regex): regular expression to recognise problem characters
        default_tag_category (str): default value to be passed to the 'category' 
            field in output dict
    Returns:
        dict of node/way element attributes and attributes of child elements (tags)
        format if node: {'node': node_attribs, 'node_tags': tags}
        format if way: {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}
    """
    
    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements

    # Write the value of each node element attribute into a dictionary.
    if element.tag == 'node':
        for field in node_attr_fields:  
            # node_attribs contains top level node attributes
            node_attribs[field] = element.attrib[field]
           
        for child in element: 
            # Create dictionary for child tag
            child_tag = {}
            value_k = child.attrib['k']
            child_tag['id'] = node_attribs['id']
            
            # If tag "k" contains problematic characters, ignore it
            if PROBLEMCHARS.search(value_k): 
                continue
            # If the k attribute contains ":" - Take the first part as type, and the remainder as key.
            elif LOWER_COLON.search(value_k):
                value_k_split = value_k.split(':', 1)
                child_tag['type'] = value_k_split[0]
                child_tag['key'] = value_k_split[1]
                child_tag['value'] = child.attrib['v']
            # Else assign the attribute value to the keys
            else: 
                child_tag['key'] = value_k
                child_tag['value'] = child.attrib['v']
                child_tag['type'] = default_tag_type

            # Update Tags for street names and postal codes
            if value_k == "addr:street":
                child_tag['value']= update_street_name(child_tag['value'])
            elif value_k == "addr:postcode":
                child_tag['value'] = update_postcode(child_tag['value'])
        
            # Append dictionary tag to list tags
            tags.append(child_tag) 
        
        return {'node': node_attribs, 'node_tags': tags}
        
    # Write the value of each way element attribute into a dictionary.    
    elif element.tag == 'way':
        for field in way_attr_fields:  
            # way_attribs contains top level way attributes
            way_attribs[field] = element.attrib[field]
        
        counter = 0
        for child in element:
            # Create dictionary for child tag
            child_tag = {}
            if child.tag == 'tag':  
                value_k = child.attrib['k']
                child_tag['id'] = way_attribs['id']
                                
                # If tag "k" contains problematic characters, ignore it
                if PROBLEMCHARS.search(value_k): 
                    continue
                # If the k attribute contains ":" - Take the first part as type, and the remainder as key.
                elif LOWER_COLON.search(value_k): 
                    value_k_split = value_k.split(':', 1)
                    child_tag['type'] = value_k_split[0]
                    child_tag['key'] = value_k_split[1]
                    child_tag['value'] = child.attrib['v']
                # Else assign the attribute value to the keys
                else: 
                    child_tag['key'] = value_k
                    child_tag['value'] = child.attrib['v']
                    child_tag['type'] = default_tag_type
                    
                 # Update Tags for street names and postal codes
                if value_k == "addr:street":
                    child_tag['value']= update_street_name(child_tag['value'])
                elif value_k == "addr:postcode":
                    child_tag['value'] = update_postcode(child_tag['value'])

                # Append dictionary tag to list tags
                tags.append(child_tag) 
            
            if child.tag =='nd': #
                way_node = {}
                way_node['id'] = way_attribs['id']
                way_node['node_id'] = child.attrib['ref']
                way_node['position'] = counter
                counter += 1
                way_nodes.append(way_node)
                
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}

# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag.
    Args:
        osmfile (obj): XML file to audit.
        tags (list): element types to be yielded,
    Yields:
        elem (obj): element found using ET.iterparse().
    """
   
    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema.
    Args:
        element (dict): dict of node/way element attributes and attributes of child elements
            returned by shape_element()
        validator (obj): cerberus.validator object
        schema (dict): schema of desired data structure
    Raises:
        exception if element structure does not match schema
    """
    
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        
        raise Exception(message_string.format(field, error_string))


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate=False):
     """Iteratively process each XML element and write to csv(s).
    Args:
        file_in (obj): XML file to audit.
        validate (bool): if True, will validate each element using validate_element()
    Returns:
        five CSV files:  nodes, nodes_tags, ways, ways_tags and ways_nodes 
    """
    with open(NODES_PATH, 'wb') as nodes_file, \
         open(NODE_TAGS_PATH, 'wb') as nodes_tags_file, \
         open(WAYS_PATH, 'wb') as ways_file, \
         open(WAY_NODES_PATH, 'wb') as way_nodes_file, \
         open(WAY_TAGS_PATH, 'wb') as way_tags_file:

        nodes_writer = unicodecsv.DictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = unicodecsv.DictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = unicodecsv.DictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = unicodecsv.DictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = unicodecsv.DictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])
                    
process_map(OSM_FILE)