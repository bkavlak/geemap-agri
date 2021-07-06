import os
import ee
import geemap
import geopandas

# Define Variables
id_attributte = 'TarlaId';
polygon_name = ee.String('users/batuhankavlak4/{YOUR_POLYGON_ASSET}');
ee_dataset = "COPERNICUS/S2_SR";
date_start = ee.Date('2020-02-01');
date_end = ee.Date('2020-11-30');
cloud_perc = 20;
band_names = ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B11', 'B12'];


def set_field_id(id_int):
    '''
    Adds integer as FieldId att to an EE feature object.

    Parameters:
        feature: an EE feature object.
        id_int (int): Id to be added.

    Returns:
        feature(ee feature): an EE feature object. 
    '''
    def wrapper_f(feature):
        base_dict = ee.Dictionary({
            'FieldId': id_int
        })
        feature = feature.set(base_dict)
        return feature
    return wrapper_f


def set_coord_att(feature):
    '''
    Adds coordinates as Coords att to an EE feature object.

    Parameters:
        feature: an EE feature object.

    Returns:
        feature(ee feature): an EE feature object. 
    '''
    coords = feature.geometry().coordinates()
    base_dict = ee.Dictionary({
        'Coords': coords
    })
    feature = feature.set(base_dict)
    return feature


# Generic Function to remove a property from a feature
def remove_property(property_name):
    '''
    Remove only one att from an EE feature object.

    Parameters:
        feature: an EE feature object.
        property_name (str): Attribute to be dropped.

    Returns:
        feature(ee feature): an EE feature object. 
    '''
    def wrapper_f(feature):
        properties = feature.propertyNames()
        select_properties = properties.filter(ee.Filter.neq('item', property_name))
        return feature.select(select_properties)
    return wrapper_f
        
    

# Get Target Polygon File
polygon_collection = ee.FeatureCollection(polygon_name);
# Convert  in a Python list
polygon_collection_list = polygon_collection.aggregate_array(id_attributte);
polygon_collection_list_py = polygon_collection_list.getInfo();

# LOOP 1 - OVER POLYGONS
for field_id in polygon_collection_list_py:
    
    # Get target polygon
    polygon_feature = ee.Feature(polygon_collection.filter(ee.Filter.eq(id_attributte, field_id)).first());
    
    # Add Earth Engine dataset
    s2_col = ee.ImageCollection(ee_dataset) \
    .filterBounds(polygon_feature.geometry()) \
    .filterDate(date_start, date_end) \
    .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', cloud_perc)) \
    .select(band_names);
    
    # Extract Pixels as Point
    base_image = ee.Image(ee.ImageCollection("COPERNICUS/S2_SR") \
                               .filterBounds(polygon_feature.geometry()) \
                               .filterDate(date_start, date_end) \
                               .first() \
                               .select(['B2']))
    polygon_feature =  base_image \
    .sample(**{
        'region': polygon_feature.geometry(),
        'geometries': True,  # if you want points,
        'scale': 10,
        'projection': base_image.projection().crs()
    });
    
    # Add FieldId & Coords
    polygon_feature = polygon_feature.map(set_field_id(field_id))
    polygon_feature = polygon_feature.map(set_coord_att)
    # Remove B2 & system:index
    polygon_feature = polygon_feature.map(remove_property('B2'))
    polygon_feature = polygon_feature.map(remove_property('system:index'))
    
    # Get PRODUCT_ID in a Python list
    collection_list = s2_col.aggregate_array('PRODUCT_ID');
    collection_list_py = collection_list.getInfo();
    
    # LOOP 2 - OVER IMAGES
    for product_name in collection_list_py:
        new_band_names = [product_name + '_' + s for s in band_names]
        # Add Earth Engine dataset
        s2_image = ee.Image(s2_col \
                            .filter(ee.Filter.eq('PRODUCT_ID', product_name)) \
                            .first()) \
        .rename(new_band_names);
        output_dir = os.path.expanduser('./csv/');
        out_csv = os.path.join(output_dir, product_name + '_' + str(field_id) + '.csv');
        geemap.extract_values_to_points(polygon_feature, s2_image, out_csv, scale = 10);
    