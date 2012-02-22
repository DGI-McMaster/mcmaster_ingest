'''
Created on Feb 21, 2012
This file handles the batch ingest of the ontario conservation reports
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
    logFile = os.path.join(log_directory,'/big2/dc/Digital-Collections/archival-objects/WWIIJUR' + time.strftime('%y_%m_%d') + '.log')
    logging.basicConfig(filename=logFile, level=logging.DEBUG)

    #get config
    config = ConfigParser.ConfigParser()
    config.read(os.path.join(source_directory,'mcmaster.cfg'))
    #config.read(os.path.join(source_directory,'TEST.cfg'))
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
    mods_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/WWIIJUR/mods')
    if not os.path.isdir(mods_directory):
        logging.error('MODS directory invalid \n')
        sys.exit()
 
    macrepo_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/WWIIJUR/macrepo')
    if not os.path.isdir(macrepo_directory):
        logging.error('MODS directory invalid \n')
        sys.exit()
   
    tif_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/WWIIJUR/tif')
    if not os.path.isdir(tif_directory):
        logging.error('TIF directory invalid \n')
        sys.exit()

    jpg_thumb_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/WWIIJUR/jpg_thumb')
    if not os.path.isdir(tif_directory):
        logging.error('TIF directory invalid \n')
        sys.exit()

    jpg_medium_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/WWIIJUR/jpg_medium')
    if not os.path.isdir(jpg_medium_directory):
        logging.error('TIF directory invalid \n')
        sys.exit()
    
    fits_page_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/WWIIJUR/fits')
    if not os.path.isdir(fits_page_directory):
        logging.error('FITS pages directory invalid \n')
        sys.exit()
    
    jp2_lossless_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/WWIIJUR/jp2_lossless')
    if not os.path.isdir(jp2_lossless_directory):
        logging.error('JP2 lossless directory invalid \n')
        sys.exit()
    
    jp2_lossy_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/WWIIJUR/jp2_lossy')
    if not os.path.isdir(jp2_lossy_directory):
        logging.error('JP2 lossy directory invalid \n')
        sys.exit()
    
    #prep data structures (files)
    mods_files = os.listdir(mods_directory)
    macrepo_files = os.listdir(macrepo_directory)
    tif_page_files = os.listdir(tif_directory)
    fits_page_files = os.listdir(fits_directory)
    jpg_medium_page_files = os.listdir(jpg_medium_directory)
    jpg_thumb_page_files = os.listdir(jpg_thumb_directory)
    jp2_lossless_files = os.listdir(jp2_lossless_directory)
    jp2_lossy_files = os.listdir(jp2_lossy_directory)
    
    name_space = u'macrepo'
    
    '''
    do ingest
    '''
        #put in the JapaneseSilentFilmCollection collection object
    try:
        collection_label = u'30'
        collection_pid = unicode(name_space + ':' + collection_label)
        collection_policy = u'<collection_policy xmlns="http://www.islandora.ca" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" name="" xsi:schemaLocation="http://www.islandora.ca">  <content_models>    <content_model dsid="ISLANDORACM" name="object Content Model" namespace="islandora:1" pid="islandora:objectCModel"></content_model>  </content_models>  <search_terms></search_terms>  <staging_area></staging_area>  <relationship>isMemberOf</relationship></collection_policy>'
        fedora.getObject(collection_pid)
    except FedoraConnectionException, object_fetch_exception:
        if object_fetch_exception.httpcode in [404]:
            logging.info(name_space + ':UnitedStatesCivilWarLetters missing, creating object.\n')
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
            collection_object_RELS_EXT.addRelationship('isMemberOf','islandora:root')
            collection_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'),'islandora:collectionCModel')
            collection_object_RELS_EXT.update()

    #loop through the mods folder
    for mods_file in mods_files:
        if mods_file.endswith('-MODS.xml'):
            #get mods file contents
            mods_file_path = os.path.join(source_directory, 'mods', mods_file)
            mods_file_handle = open(mods_file_path)
            mods_contents = mods_file_handle.read()
            
            #get object_label from mods title
            mods_tree = etree.parse(mods_file_path)
            object_label = mods_tree.xpath("*[local-name() = 'titleInfo']/*[local-name() = 'title']/text()")
            object_label = object_label[0]
            if len(object_label) > 255:
                object_label = object_label[0:250] + '...'
            print(object_label)
            object_label = unicode(object_label)
            
            #create a object object
            object_pid = fedora.getNextPID(name_space)
            object_object = fedora.createObject(object_pid, label = object_label)
            
            #add mods datastream
            mods_file_handle.close()
            try:
                object_object.addDataStream(u'MODS', unicode(mods_contents, encoding = 'UTF-8'), label = u'MODS',
                mimeType = u'text/xml', controlGroup = u'X',
                logMessage = u'Added basic mods meta data.')
                logging.info('Added MODS datastream to:' + object_pid)
            except FedoraConnectionException:
                logging.error('Error in adding MODS datastream to:' + object_pid + '\n')
            
            
            #add fits ds
            fits_file = object_name + '-FITS.xml'
            fits_file_path = os.path.join(source_directory, 'fits', fits_file)
            fits_file_handle = open(fits_file_path)
            fits_contents = fits_file_handle.read()
            fits_file_handle.close()
            try:
                object_object.addDataStream(u'FITS', unicode(fits_contents), label=u'FITS',
                mimeType = u'text/xml', controlGroup=u'M',
                logMessage = u'Added basic FITS meta data.')
                logging.info('Added FITS datastream to:' + object_pid)
            except FedoraConnectionException:
                logging.error('Error in adding FITS datastream to:' + object_pid + '\n')
            
            #add macrepo ds
            macrepo_file = object_name + '-MACREPO.xml'
            macrepo_file_path = os.path.join(source_directory, 'macrepo', macrepo_file)
            macrepo_file_handle = open(macrepo_file_path)
            macrepo_contents = macrepo_file_handle.read()
            macrepo_file_handle.close()
            try:
                object_object.addDataStream(u'MACREPO', unicode(fits_contents, encoding = 'UTF-8'), label=u'MACREPO',
                mimeType = u'text/xml', controlGroup=u'M',
                logMessage = u'Added basic MACREPO meta data.')
                logging.info('Added MACREPO datastream to:' + object_pid)
            except FedoraConnectionException:
                logging.error('Error in adding MACREPO datastream to:' + object_pid + '\n')

            #add relationships
            objRelsExt = fedora_relationships.rels_ext(object_object, fedora_model_namespace)
            objRelsExt.addRelationship('isMemberOf', collection_pid)
            objRelsExt.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'),'islandora:objectCModel')
            objRelsExt.update()
            
            #get the object page datastructures
            object_page_tif_files = list()
            for tif_file in tif_files:
                if jp2_file[:tif_file.find('-')] == object_name:
                    object_page_tif_files.append(tif_file)


            object_page_jp2_lossy_thumb_files = list()
            for jp2_lossy_file in jp2_lossy_files:
                if jp2_lossy_file[:jp2_lossy_file.find('-')] == object_name:
                    object_page_jp2_lossy_files.append(jp2_lossy_file)

            object_page_jp2_lossless_files = list()
            for jp2_lossless_file in jp2_lossless_files:
                if jpg_thumb_file[:jp2_lossless_file.find('-')] == object_name:
                    object_page_jp2_lossless_files.append(jp2_lossless_file)
                    
            #loop through the jp2 files that are associated with the mods
            for tif_file in object_page_tif_files:
                #create an object for each
                page_name = tif_file[tif_file.find('-') + 1:tif_file.find('.')]
                #page_pid = fedora.getNextPID(name_space)
                page_label = object_label + '-' + page_name
                page_pid = name_space + object_pid[object_pid.find(':'):] + '-' + page_name
                page_label = unicode(page_label)
                page_object = fedora.createObject(page_pid, label = page_label)
                jp2_file_path = os.path.join(source_directory, 'tif', tif_file)
                
                #add a thumnail to the object if apropriate
                if page_name == '001':
                    #create thumbnail
                    tn_file_path = tif_file_path + '.jpg'
                    image_magic_call = ["convert", tif_file_path, '-compress', 'JPEG', "-thumbnail", "x100", "-gravity", "center", "-extent", "x100", tn_file_path]
                    response = subprocess.call(image_magic_call)
                    
                    #ingest thumbnail
                    #tn_file_path = os.path.join(source_directory, 'tif', tif_file)
                    tn_file_handle = open(tn_file_path, 'rb')
                    try:
                        object_object.addDataStream(u'TN', u'aTmpStr', label = u'TN',
                        mimeType = u'image/jpg', controlGroup = u'M',
                        logMessage = u'Added TN datastream.')
                        datastream = object_object['TN']
                        datastream.setContent(tn_file_handle)
                        logging.info('Added TN datastream to:' + object_pid)
                    except FedoraConnectionException:
                        logging.error('Error in adding TN datastream to:' + object_pid + '\n')
                    tn_file_handle.close()
                
                #add tif ds
                tif_file_handle = open(tif_file_path, 'rb')
                try:
                    page_object.addDataStream(u'OBJ', u'aTmpStr', label=u'OBJ',
                    mimeType = u'image/tif', controlGroup = u'M',
                    logMessage = u'Added JP2 datastream.')
                    datastream = page_object['OBJ']
                    datastream.setContent(tif_file_handle)
                    logging.info('Added TIF datastream to:' + page_pid)
                except FedoraConnectionException:
                    logging.error('Error in adding TIF datastream to:' + page_pid + '\n')
                tif_file_handle.close()
                
                #add jp2 ds
                jp2_lossy_file_handle = open(jp2_lossy_file_path, 'rb')
                try:
                    page_object.addDataStream(u'JP2', u'aTmpStr', label=u'JP2',
                    mimeType = u'image/jp2', controlGroup = u'M',
                    logMessage = u'Added JP2 datastream.')
                    datastream = page_object['JP2']
                    datastream.setContent(jp2_lossy_file_handle)
                    logging.info('Added JP2 lossy datastream to:' + page_pid)
                except FedoraConnectionException:
                    logging.error('Error in adding JP2 lossy datastream to:' + page_pid + '\n')
                jp2_lossy_file_handle.close()

                #add jp2 ds
                jp2_lossless_file_handle = open(jp2_lossless_file_path, 'rb')
                try:
                    page_object.addDataStream(u'LOSSLESS_JP2', u'aTmpStr', label=u'LOSSLESS_JP2',
                    mimeType = u'image/jp2', controlGroup = u'M',
                    logMessage = u'Added JP2 datastream.')
                    datastream = page_object['LOSSLESS_JP2']
                    datastream.setContent(jp2_lossless_file_handle)
                    logging.info('Added JP2 lossless datastream to:' + page_pid)
                except FedoraConnectionException:
                    logging.error('Error in adding JP2 lossless datastream to:' + page_pid + '\n')
                jp2_lossless_file_handle.close()


                #add fits ds
                fits_file = object_name + str(int(page_name)) + '-FITS'  + '.xml'
                fits_file_path = os.path.join(source_directory, 'fits', fits_file)
                if os.path.isfile(fits_file_path):
                    fits_file_handle = open(fits_file_path)
                    fits_contents = fits_file_handle.read()
                    fits_file_handle.close()
                    try:
                        page_object.addDataStream(u'FITS', unicode(fits_contents), label=u'FITS',
                        mimeType=u'text/xml', controlGroup=u'M',
                        logMessage=u'Added basic FITS.')
                        logging.info('Added FITS datastream to:' + page_pid)
                    except FedoraConnectionException:
                        logging.error('Error in adding FITS datastream to:' + page_pid + '\n')
                
                
                #add relationships
                objRelsExt=fedora_relationships.rels_ext(page_object, fedora_model_namespace)
                objRelsExt.addRelationship('isMemberOf', object_pid)
                objRelsExt.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'),'islandora:pageCModel')
                objRelsExt.update()
    sys.exit()
