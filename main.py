import os
import io
import sys
import glob
import argparse
import sqlite3 as lite
from PIL import Image
from datetime import datetime
from bson.binary import Binary
from pymongo import MongoClient

def get_file_list(dir_path,extension_list):
    '''
    find all files in dir_path filter with extension list
    '''
    print dir_path
    os.chdir(dir_path)
    file_list = []
    for extension in extension_list:
        extension = '*.' + extension
        file_list += [os.path.realpath(e) for e in glob.glob(extension) ] 
    return file_list

def create_dir(base_dir):
    '''
    create dir (if not exists)
    '''
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)


def resize(w, h, w_box, h_box, pil_image):
    f1 = 1.0 * w_box / w
    f2 = 1.0 * h_box / h
    factor = min([f1, f2])
    width = int(w * factor)
    height = int(h * factor)
    return pil_image.resize((width, height), Image.ANTIALIAS)

def save_bin_file(file, bytes):
    '''
    save binary file
    '''
    file_stream = io.open(file,'wb')
    with file_stream:
        file_stream.write(bytes)

def get_bin_file(file):
    '''
    read binary file
    '''
    file_stream = io.open(file,'rb')
    with file_stream:
        return file_stream.read()

def save_image(image_file, image_bytes,need_resize=False):
    data_stream = io.BytesIO(image_bytes)
    pil_image = Image.open(data_stream)
    if not need_resize:
        pil_image.save(image_file)
    else:
        w, h = pil_image.size
        w_box = 800
        h_box = 800
        pil_image_resized = resize(w, h, w_box, h_box, pil_image)
        pil_image_resized.save(image_file)


def get_boundary(ul_lon, ul_lat,ur_lon, ur_lat,ll_lon, ll_lat,lr_lon, lr_lat):
    '''
    build geojson with four coordinates
    '''
    return {
        "type" : "Polygon",
        "coordinates" : [[[ul_lon, 
                           ul_lat], 
                [ll_lon, 
                    ll_lat],
                [lr_lon, 
                    lr_lat], 
                [ur_lon, 
                    ur_lat], 
                [ul_lon, 
                    ul_lat]]]
    }

def get_rows(db_file):
    '''
    get all rows in metadata table of sqlite database
    '''
    cx = lite.connect(db_file)
    with cx:
        cx.row_factory = lite.Row # its key
        cu = cx.cursor() 
        cu.execute("select * from metadata") 
        for row in cu:
            yield row
        cu.close()

def process(input_file, output_dir, db_url):
    '''
    process sqlite db file
    '''
    # mongodb instance
    client = MongoClient(db_url)
    db = client.satimage

    # process all rows in metadata table
    for row in get_rows(input_file):
        # column values
        productid = row['DATAID']
        filename = row['F_PRODUCTNAME']
        satelliteid = row['SATELLITEID']
        receive_station_id = row['RECSTATIONID']
        sensor_id = row['SENSORID']
        acquisition_time = row['IMAGINGSTARTTIME']
        print acquisition_time
        cloud_percent = row['CLOUDAMOUNT']
        track_id = row['TRACKID']
        scene_path = row['SCENEPATH']
        scene_row = row['SCENEROW']
        metadata_bin = row['F_METADATA']
        quickview_bin = row['F_QUICKIMAGE1']
        thumb_bin = row['F_THUMIMAGE']
        png_bin = row['F_SHAPEIMAGE']
        data_ul_lat = row['DATAUPPERLEFTLAT']
        data_ul_lon = row['DATAUPPERLEFTLONG']
        data_ur_lat = row['DATAUPPERRIGHTLAT']
        data_ur_lon = row['DATAUPPERRIGHTLONG']
        data_ll_lat = row['DATALOWERLEFTLAT']
        data_ll_lon = row['DATALOWERLEFTLONG']
        data_lr_lat = row['DATALOWERRIGHTLAT']
        data_lr_lon = row['DATALOWERRIGHTLONG']
        product_ul_lat = row['PRODUCTUPPERLEFTLAT']
        product_ul_lon = row['PRODUCTUPPERLEFTLONG']
        product_ur_lat = row['PRODUCTUPPERRIGHTLAT']
        product_ur_lon = row['PRODUCTUPPERRIGHTLONG']
        product_ll_lat = row['PRODUCTLOWERLEFTLAT']
        product_ll_lon = row['PRODUCTLOWERLEFTLONG']
        product_lr_lat = row['PRODUCTLOWERRIGHTLAT']
        product_lr_lon = row['PRODUCTLOWERRIGHTLONG']

        if db.metadata.find({"productid" : productid}).count() > 0:
            print str(productid) + ' is existed.'
            return
        
        print input_file
        print productid

        # get geojson boundary
        boundary = {}
        if data_ul_lat == 0 and data_ul_lon == 0 and data_ur_lat == 0 and data_ur_lon == 0:
            boundary = get_boundary(product_ul_lon, product_ul_lat,
                                    product_ur_lon, product_ur_lat,
                                    product_ll_lon, product_ll_lat,
                                    product_lr_lon, product_lr_lat)
        else:
            boundary = get_boundary(data_ul_lon, data_ul_lat,
                                    data_ur_lon, data_ur_lat,
                                    data_ll_lon, data_ll_lat,
                                    data_lr_lon, data_lr_lat)
        
        # save files 
        try:
            base_dir = output_dir + '/' + str(track_id) + '/' + str(scene_path) + '/' + str(scene_row)
            create_dir(base_dir)
            meta_file = base_dir + '/' + filename + '.xml'
            save_bin_file(meta_file, metadata_bin)
            quick_file = base_dir + '/' + filename + '_pre.jpg'
            save_bin_file(quick_file, quickview_bin)
            thumb_file = base_dir + '/' + filename + '_thumb.jpg'
            save_bin_file(thumb_file, thumb_bin)
            png_file = base_dir + '/' + filename + '.png'
            save_image(png_file, png_bin, True)
        except Exception, expinfo:
            print Exception,":",expinfo

        try:
            thumb_arrays = get_bin_file(thumb_file)
            standard_acquisition_time = acquisition_time.replace('/', '-')
            # build product
            product = {
                "productid" : productid,
                "filename" : filename + '.tar',
                "satelliteid" : satelliteid,
                "receivestationid" : receive_station_id,
                "sensorid" : sensor_id,
                "acquisitiontime" : datetime.strptime(standard_acquisition_time, "%Y-%m-%d %H:%M:%S"),
                "inputtime" : datetime.today(),
                "cloudpercent" : cloud_percent,
                "sceneid" : 0,
                "orbitid" : track_id,
                "scenepath" : scene_path,
                "scenerow" : scene_row,
                "taruri" : "",
                "quickviewuri" : "",
                "boundary" : boundary,
                "thumbview" : Binary(thumb_arrays),
                "metadata" : {}
            }

            print 'insert ' + str(productid)
            db.metadata.insert_one(product)
        except Exception, expinfo:
            print Exception,":",expinfo



def process_dir_list(input_dir_list, output_dir, db_url):
    '''
    process input folder list, get sqlite db files in each folder and process data
    '''
    extension_list = ['db']
    for input_dir in input_dir_list:
        db_file_list = get_file_list(input_dir, extension_list)
        for db_file in db_file_list:
            process(db_file, output_dir, db_url)

def process_file_list(db_list, out, db_url):
    '''
    process db file array
    '''
    for db_file in db_list:
        process(db_file, out, db_url)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-db','--db_url')
    parser.add_argument('-i', '--input', nargs='+')
    parser.add_argument('-o', '--output')
    parser.add_argument('-v', dest='verbose', action='store_true')
    args = parser.parse_args()

    # save confige to variable
    #"mongodb://localhost:27017"
    DB_URL = args.db_url
    input_files = args.input
    output_dir = args.output
    verbose = args.verbose

    print '==========input args infos=========='

    print '==mongodb:=='
    print DB_URL

    print '==ouput-dir:=='
    print output_dir

    print '==input-dirs:=='
    for dir in input_files:  
         print dir 
    
    print '==========start process==========='
    process_dir_list(input_files, output_dir, DB_URL)
    
    print "process finished."
