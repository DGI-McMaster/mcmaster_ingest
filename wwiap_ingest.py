'''
Created on January 24, 2012
This file handles the batch ingest of the Italian topographical map collection for McMaster University Libray
@adapted from Will Panting's Hamilton College ingest script (https://github.com/DGI-Hamilton-College/Hamilton_ingest)
@author: Nick Ruest
'''
import logging, sys, os, ConfigParser, time, subprocess#, shutil
from fcrepo.connection import Connection, FedoraConnectionException
from fcrepo.client import FedoraClient
from islandoraUtils.metadata import fedora_relationships
from lxml import etree

if __name__ == '__main__':
    if len(sys.argv) == 2:
        source_directory = sys.argv[1]
    else:
        print('Please verify source directory.')
        sys.exit(-1)
    
    
    '''
    setup
    '''
    macrepo_rdf_name_space = fedora_relationships.rels_namespace('macrepo', 'http://repository.mcmaster.ca/ontology#')
    fedora_model_namespace = fedora_relationships.rels_namespace('fedora-model','info:fedora/fedora-system:def/model#')
    
        
    #configure logging
    log_directory = os.path.join(source_directory,'logs')
    if not os.path.isdir(log_directory):
        os.mkdir(log_directory)
    logFile = os.path.join(log_directory,'/big2/dc/Digital-Collections/archival-objects/WWIAP' + time.strftime('%y_%m_%d') + '.log')
    logging.basicConfig(filename=logFile, level=logging.DEBUG)

    #get config
    config = ConfigParser.ConfigParser()
    config.read(os.path.join(source_directory,'mcmaster.cfg'))
    solrUrl = config.get('Solr','url')
    fedoraUrl = config.get('Fedora','url')
    fedoraUserName = config.get('Fedora', 'username')
    fedoraPassword = config.get('Fedora','password')
            
    #get fedora connection
    connection = Connection(fedoraUrl,
                    username=fedoraUserName,
                    password=fedoraPassword)
    try:
        fedora=FedoraClient(connection)
    except FedoraConnectionException:
        logging.error('Error connecting to fedora, exiting'+'\n')
        sys.exit()

    #setup the directories
    mods_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/WWIAP/mods')
    if not os.path.isdir(mods_directory):
        logging.error('MODS directory invalid \n')
        sys.exit()
    
    tif_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/WWIAP/tif')
    if not os.path.isdir(tif_directory):
        logging.error('TIF directory invalid \n')
        sys.exit()

    jpg_med_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/WWIAP/jpg_medium')
    if not os.path.isdir(jpg_med_directory):
        logging.error('JPG medium directory invalid \n')
        sys.exit()

    jpg_thumb_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/WWIAP/jpg_thumb')
    if not os.path.isdir(jpg_thumb_directory):
        logging.error('JPG thumbnail directory invalid \n')
        sys.exit()    

    jp2_lossy_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/WWIAP/jp2_lossy')
    if not os.path.isdir(jp2_lossy_directory):
        logging.error('JP2 lossy directory invalid \n')
        sys.exit()
    
    jp2_lossless_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/WWIAP/jp2_lossless')
    if not os.path.isdir(jp2_lossless_directory):
        logging.error('JP2 lossless directory invalid \n')
        sys.exit()
   
    fits_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/WWIAP/fits')
    if not os.path.isdir(fits_directory):
      logging.error('FITS directroy invalid \n')
      sys.exit()

    macrepo_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/WWIAP/macrepo')
    if not os.path.isdir(macrepo_directory):
      logging.error('MACREPO directroy invalid \n')
      sys.exit()

    #prep data structures (files)
    mods_files = os.listdir(mods_directory)
    tif_files = os.listdir(tif_directory)
    jpg_med_files = os.listdir(jpg_med_directory)
    jpg_thumb_files = os.listdir(jpg_thumb_directory)
    jp2_lossy_files = os.listdir(jp2_lossy_directory)
    jp2_lossless_files = os.listdir(jp2_lossless_directory)
    fits_files = os.listdir(fits_directory)
    macrepo_files = os.listdir(macrepo_directory) 

    name_space = u'macrepo'
    
    '''
    do ingest
    '''
    #put in the World War, 1914-1918, aerial photograph object
    try:
        collection_label = u'53'
        collection_pid = unicode(name_space + ':' + collection_label)
        collection_policy = u'<collection_policy xmlns="http://www.islandora.ca" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" name="" xsi:schemaLocation="http://www.islandora.ca http://syn.lib.umanitoba.ca/collection_policy.xsd"> <content_models> <content_model dsid="ISLANDORACM" name="Islandora Collection Model ~ islandora:collectionCModel" namespace="islandora:1" pid="islandora:collectionCModel"/> <content_model dsid="ISLANDORACM" name="Islandora large image content model" namespace="macrepo:1" pid="islandora:sp_large_image_cmodel"/> </content_models> <search_terms/> <staging_area/> <relationship>isMemberOfCollection</relationship> </collection_policy> '
        fedora.getObject(collection_pid)
    except FedoraConnectionException, object_fetch_exception:
        if object_fetch_exception.httpcode in [404]:
            logging.info(name_space + ':wwiap missing, creating object.\n')
            collection_object = fedora.createObject(collection_pid, label = collection_label)
            #collection_policy
            try:
                collection_object.addDataStream(u'COLLECTION_POLICY', collection_policy, label=u'COLLECTION_POLICY',
                mimeType=u'text/xml', controlGroup=u'X',
                logMessage=u'Added basic COLLECTION_POLICY data.')
                logging.info('Added COLLECTION_POLICY datastream to:' + collection_pid)
            except FedoraConnectionException:
                logging.error('Error in adding COLLECTION_POLICY datastream to:' + collection_pid + '\n')
            
            #add relationships
            collection_object_RELS_EXT = fedora_relationships.rels_ext(collection_object, fedora_model_namespace)
            collection_object_RELS_EXT.addRelationship('isMemberOfCollction','islandora:root')
            collection_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'),'islandora:collectionCModel')
            collection_object_RELS_EXT.update()

    #loop through the mods folder
    for mods_file in mods_files:
        if mods_file.endswith('-MODS.xml'):
            #get mods file contents
            mods_file_path = os.path.join(source_directory, 'mods', mods_file)
            mods_file_handle = open(mods_file_path)
            mods_contents = mods_file_handle.read()
           
            #get map_label from mods title
            mods_tree = etree.parse(mods_file_path)
            map_label = mods_tree.xpath("*[local-name() = 'titleInfo']/*[local-name() = 'title']/text()")
            map_label = map_label[0]
            if len(map_label) > 255:
                map_label = map_label[0:250] + '...'
            #print(map_label)
            map_label = unicode(map_label)
            map_name = mods_tree.xpath("*[local-name() = 'identifier']/text()")[0].strip("\t\n\r")        
 
            #create a map object
            map_pid = fedora.getNextPID(name_space)
	    map_object = fedora.createObject(map_pid, label = map_label)
            print(map_pid)

	    #add mods datastream
            
            mods_file_handle.close()
            try:
                map_object.addDataStream(u'MODS', unicode(mods_contents), label = u'MODS',
                mimeType = u'text/xml', controlGroup = u'X',
                logMessage = u'Added basic mods meta data.')
                logging.info('Added MODS datastream to:' + map_pid)
            except FedoraConnectionException:
                logging.error('Error in adding MODS datastream to:' + map_pid + '\n')

            #add fits datastream

            fits_file = map_name + '-FITS.xml'
            fits_file_path = os.path.join(source_directory, 'fits', fits_file)
            fits_file_handle = open(fits_file_path)
            fits_contents = fits_file_handle.read()
            
            try:
                map_object.addDataStream(u'FITS', unicode(fits_contents), label = u'FITS',
                mimeType = u'text/xml', controlGroup = u'X',
                logMessage = u'Added fits meta data.')
                logging.info('Added FITS datastream to:' + map_pid)
            except FedoraConnectionException:
                logging.error('Error in adding FITS datastream to:' + map_pid + '\n')
            fits_file_handle.close()

            #add macrepo datastream

            macrepo_file = map_name + '-MACREPO.xml'
            macrepo_file_path = os.path.join(source_directory, 'macrepo', fits_file)
            macrepo_file_handle = open(macrepo_file_path)
            macrepo_contents = macrepo_file_handle.read()

            try:
                map_object.addDataStream(u'MACREPO', unicode(macrepo_contents), label = u'MACREPO',
                mimeType = u'text/xml', controlGroup = u'X',
                logMessage = u'Added macrepo meta data.')
                logging.info('Added macrepo datastream to:' + map_pid)
            except FedoraConnectionException:
                logging.error('Error in adding MACREPO datastream to:' + map_pid + '\n')
            macrepo_file_handle.close()
            
	    #add tif datastream
           
            tif_file = map_name + '.tif'
            tif_file_path = os.path.join(source_directory, 'tif', tif_file)
            tif_file_handle = open(tif_file_path, 'rb')
            
            try:
                map_object.addDataStream(u'OBJ', u'aTmpStr', label=u'OBJ',
                mimeType = u'image/tif', controlGroup = u'M',
                logMessage = u'Added TIFF datastream.')
                datastream = map_object['OBJ']
                datastream.setContent(tif_file_handle)
                logging.info('Added TIFF datastream to:' + map_pid)
            except FedoraConnectionException:
                logging.error('Error in adding TIFF datastream to:' + map_pid + '\n')
            tif_file_handle.close()
            
            #add jpg medium datastream
            
            jpg_med_file = map_name + '.jpg'
            jpg_med_file_path = os.path.join(source_directory, 'jpg_medium', jpg_med_file)
            jpg_med_file_handle = open(jpg_med_file_path, 'rb')

            try:
                map_object.addDataStream(u'JPG', u'aTmpStr', label=u'JPG',
                mimeType = u'image/jpeg', controlGroup = u'M',
                logMessage = u'Added JPG medium datastream.')
                datastream = map_object['JPG'] #double check datastream name w/large image solution pack
                datastream.setContent(jpg_med_file_handle)
                logging.info('Added JPG medium datastream to:' + map_pid)
            except FedoraConnectionException:
                logging.error('Error in adding JPG medium  datastream to:' + map_pid + '\n')
            jpg_med_file_handle.close()

            #add jpg thumbnail datastream
           
            jpg_thumb_file = map_name + '.jpg'
            jpg_thumb_file_path = os.path.join(source_directory, 'jpg_thumb', jpg_thumb_file)
            jpg_thumb_file_handle = open(jpg_thumb_file_path, 'rb')

            try:
                map_object.addDataStream(u'TN', u'aTmpStr', label=u'TN',
                mimeType = u'image/jpeg', controlGroup = u'M',
                logMessage = u'Added JPG thumb datastream.')
                datastream = map_object['TN'] 
                datastream.setContent(jpg_thumb_file_handle)
                logging.info('Added JPG thumb datastream to:' + map_pid)
            except FedoraConnectionException:
                logging.error('Error in adding JPG thumb datastream to:' + map_pid + '\n')
            jpg_thumb_file_handle.close()

            #add jp2 lossy datastream

            jp2_lossy_file = map_name + '.jp2'
            jp2_lossy_file_path = os.path.join(source_directory, 'jp2_lossy', jp2_lossy_file)
            jp2_lossy_file_handle = open(jp2_lossy_file_path, 'rb')

            try:
                map_object.addDataStream(u'JP2', u'aTmpStr', label=u'JP2',
                mimeType = u'image/jp2', controlGroup = u'M',
                logMessage = u'Added JP2 lossy datastream.')
                datastream = map_object['JP2'] 
                datastream.setContent(jp2_lossy_file_handle)
                logging.info('Added JP2 lossy datastream to:' + map_pid)
            except FedoraConnectionException:
                logging.error('Error in adding JP2 lossy datastream to:' + map_pid + '\n')
            jp2_lossy_file_handle.close()

            #add jp2 lossless datastream

            jp2_lossless_file = map_name + '.jp2'
            jp2_lossless_file_path = os.path.join(source_directory, 'jp2_lossless', jp2_lossless_file)
            jp2_lossless_file_handle = open(jp2_lossless_file_path, 'rb')

            try:
                map_object.addDataStream(u'LOSSLESS_JP2', u'aTmpStr', label=u'LOSSLESS_JP2',
                mimeType = u'image/jp2', controlGroup = u'M',
                logMessage = u'Added JP2 lossless datastream.')
                datastream = map_object['LOSSLESS_JP2'] 
                datastream.setContent(jp2_lossless_file_handle)
                logging.info('Added JP2 lossless datastream to:' + map_pid)
            except FedoraConnectionException:
                logging.error('Error in adding lossless datastream to:' + map_pid + '\n')
            jp2_lossless_file_handle.close()

	    #add relationships
            objRelsExt = fedora_relationships.rels_ext(map_object, fedora_model_namespace)
            objRelsExt.addRelationship('isMemberOfCollection', collection_pid)
            objRelsExt.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'),'islandora:sp_large_image_cmodel')
            objRelsExt.update()
            
    sys.exit()
