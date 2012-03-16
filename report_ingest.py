#!/usr/bin/env python

"""
Created on Feb 14, 2012
This file handles the batch ingest of the ontario conservation reports
@adapted from Will Panting's Hamilton College ingest script (https://github.com/DGI-Hamilton-College/Hamilton_ingest)
This file handles the batch ingest of the Ontario Conservation Report collection for McMaster University Library
@author: Nick Ruest, Matt McCollow
"""

import logging, sys, os, ConfigParser, time, subprocess
from fcrepo.connection import Connection, FedoraConnectionException
from fcrepo.client import FedoraClient
from islandoraUtils.metadata import fedora_relationships
from lxml import etree
import urlparse, urllib

if __name__ == '__main__':
    if len(sys.argv) == 2:
        source_directory = sys.argv[1]
    else:
        print('Please verify source directory.')
        sys.exit(-1)
    
    
    '''
    setup
    '''
    mcmaster_rdf_name_space = fedora_relationships.rels_namespace('mcmaster', 'http://repository.mcmaster.ca/ontology#')
    fedora_model_namespace = fedora_relationships.rels_namespace('fedora-model','info:fedora/fedora-system:def/model#')
    
        
    #configure logging
    log_directory = os.path.join(source_directory,'logs')
    if not os.path.isdir(log_directory):
        os.mkdir(log_directory)
    logFile = os.path.join(log_directory,'/big2/dc/Digital-Collections/archival-objects/mcmaster-ontario-conservation-reports' + time.strftime('%y_%m_%d') + '.log')
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
    metadata_directory = os.path.join(source_directory, 'metadata')
    if not os.path.isdir(metadata_directory):
        logging.error('METADATA directory invalid \n')
        sys.exit()

    mods_directory = os.path.join(source_directory, 'metadata')
    if not os.path.isdir(mods_directory):
        logging.error('MODS directory invalid \n')
        sys.exit()
    
    jpg_page_directory = os.path.join(source_directory, 'jpg')
    if not os.path.isdir(jpg_page_directory):
        logging.error('FITS directory invalid \n')
        sys.exit()
    
    jp2_page_directory = os.path.join(source_directory, 'jp2')
    if not os.path.isdir(jp2_page_directory):
        logging.error('JP2 pages directory invalid \n')
        sys.exit()
    
    tif_page_directory = os.path.join(source_directory, 'tif')
    if not os.path.isdir(tif_page_directory):
        logging.error('TIF directory invalid \n')
        sys.exit()
    
    pdf_directory = os.path.join(source_directory, 'pdf')
    if not os.path.isdir(pdf_directory):
        logging.error('PDF directory invalid \n')
        sys.exit()
    
    tn_page_directory = os.path.join(source_directory, 'tn')
    if not os.path.isdir(tn_page_directory):
        logging.error('TN directory invalid \n')
        sys.exit()

    #prep data structures (files)
    metadata_files = os.listdir(metadata_directory)
    mods_files = os.listdir(mods_directory)
    jpg_page_files = os.listdir(jpg_page_directory)
    tif_page_files = os.listdir(tif_page_directory)
    jp2_page_files = os.listdir(jp2_page_directory)
    pdf_files = os.listdir(pdf_directory)
    tn_page_files = os.listdir(tn_page_directory)
 
    name_space = u'macrepo'
    
    '''
    do ingest
    '''
        #put in the JapaneseSilentFilmCollection collection object
    try:
        collection_label = u'5946'
        collection_pid = unicode(name_space + ':' + collection_label)
        collection_policy = u'<collection_policy xmlns="http://www.islandora.ca" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" name="" xsi:schemaLocation="http://www.islandora.ca http://syn.lib.umanitoba.ca/collection_policy.xsd"> <content_models> <content_model dsid="ISLANDORACM" name="Islandora Collection Model ~ islandora:collectionCModel" namespace="islandora:1" pid="islandora:collectionCModel"/> <content_model dsid="ISLANDORACM" name=""Book Content Model" namespace="macrepo:1" pid="islandora:bookCModel"/> </content_models> <search_terms/> <staging_area/> <relationship>isMemberOfCollection</relationship> </collection_policy> '
        fedora.getObject(collection_pid)
    except FedoraConnectionException, object_fetch_exception:
        if object_fetch_exception.httpcode in [404]:
            logging.info(name_space + ':conversationReport missing, creating object.\n')
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
        if mods_file.endswith('_MODS.xml'):
            #get mods file contents
            mods_file_path = os.path.join(source_directory, 'metadata', mods_file)
            mods_file_handle = open(mods_file_path)
            mods_contents = mods_file_handle.read()
            
            #get book_label from mods
            parser = etree.XMLParser(encoding='utf-8')       
            mods_tree = etree.parse(mods_file_path, parser)
            book_label = mods_tree.xpath("*[local-name() = 'titleInfo']/*[local-name() = 'title']/text()")
            book_label = book_label[0].strip("\t\n\r")
            if type(book_label) is str:
                book_label = book_label.decode('utf-8')
            book_label = book_label.encode('ascii', 'xmlcharrefreplace').decode('utf-8')
            if len(book_label) > 255:
                book_label = book_label[0:250] + '...'
            book_label = unicode(book_label)
	    book_name = mods_tree.xpath("*[local-name() = 'recordInfo']/*[local-name() = 'recordIdentifier']/text()")[0].strip('ocm') 
 
            #create a book object
            book_pid = fedora.getNextPID(name_space)
            book_object = fedora.createObject(book_pid, label = book_label)
            print(book_pid)           
 
            #add mods datastream
            mods_file_handle.close()
            try:
                book_object.addDataStream(u'MODS', unicode(mods_contents), label = u'MODS',
                mimeType = u'text/xml', controlGroup = u'X',
                logMessage = u'Added basic mods meta data.')
                logging.info('Added MODS datastream to:' + book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding MODS datastream to:' + book_pid + '\n')
            
            #add pdf ds
            
            book_name = mods_file[:mods_file.find('_')]
            pdf_file = book_name + '.pdf'
            pdf_file_path = os.path.join(source_directory, 'pdf', pdf_file)
            pdf_file_handle = open(pdf_file_path, 'rb')
            
            try:
                book_object.addDataStream(u'PDF', u'aTmpStr', label=u'PDF',
                mimeType = u'application/pdf', controlGroup = u'M',
                logMessage = u'Added PDF datastream.')
                datastream = book_object['PDF']
                datastream.setContent(pdf_file_handle)
                logging.info('Added PDF datastream to:' + book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding PDF datastream to:' + book_pid + '\n')
            pdf_file_handle.close()
            
            #add mrc xml datastream
	    mrcxml_file = book_name + '_MRC.xml'
	    mrcxml_file_path = os.path.join(metadata_directory, mrcxml_file)
	    mrcxml_file_handle = open(mrcxml_file_path)
	    mrcxml_contents = mrcxml_file_handle.read()
	    mrcxml_file_handle.close()

	    try:
	        book_object.addDataStream(u'MRC-XML', unicode(mrcxml_contents, encoding = 'UTF-8'), label=u'MRC-XML',
		mimeType = u'application/xml', controlGroup=u'X',
		logMessage = u'Added basic mrc xml.')
		logging.info('Added mrc xml datastream to:' + book_pid)
	    except FedoraConnectionException:
	        logging.error('Error in adding mrc xml datastream to:' + book_pid + '\n')
 
            #add mrc ds
            marc_file = book_name + '.mrc'
            marc_file_path = os.path.join(metadata_directory, marc_file)
            marc_file_handle = open(marc_file_path, 'rb')

            try:
                book_object.addDataStream(u'MRC', u'aTmpStr', label=u'MRC',
                mimeType = u'application/mrc', controlGroup = u'M',
                logMessage = u'Added MRC datastream.')
                datastream = book_object['MRC']
                datastream.setContent(marc_file_handle)
                logging.info('Added MRC datastream to:' + book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding MRC datastream to:' + book_pid + '\n')
            marc_file_handle.close()               

            #add fits ds
            fits_pdf_file = book_name + '-FITS.xml'
            fits_pdf_file_path = os.path.join(metadata_directory, fits_pdf_file)
            fits_pdf_file_handle = open(fits_pdf_file_path)
            fits_pdf_contents = fits_pdf_file_handle.read()
            fits_pdf_file_handle.close()

            try:
                book_object.addDataStream(u'FITS', unicode(fits_pdf_contents), label=u'FITS',
                mimeType = u'text/xml', controlGroup=u'M',
                logMessage = u'Added basic FITS meta data.')
                logging.info('Added FITS datastream to:' + book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding FITS datastream to:' + book_pid + '\n')
            
            #add relationships
            objRelsExt = fedora_relationships.rels_ext(book_object, fedora_model_namespace)
            objRelsExt.addRelationship('isMemberOf', collection_pid)
            objRelsExt.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'),'islandora:bookCModel')
            objRelsExt.update()
            
            #get the book page datastructures
            book_page_tif_page_files = list()
            for tif_page_file in tif_page_files:
                if tif_page_file[:tif_page_file.find('-')] == book_name:
                    book_page_tif_page_files.append(tif_page_file)

            book_page_jp2_page_files = list()
            for jp2_page_file in jp2_page_files:
                if jp2_page_file[:jp2_page_file.find('-')] == book_name:
                    book_page_jp2_page_files.append(jp2_page_file)

            book_page_tn_page_files = list()
            for tn_page_file in tn_page_files:
                if tn_page_file[:tn_page_file.find('-')] == book_name:
                    book_page_tn_page_files.append(tn_page_file)

            book_page_jpg_page_files = list()
            for jpg_page_file in jpg_page_files:
                if jpg_page_file[:jpg_page_file.find('-')] == book_name:
                    book_page_jpg_page_files.append(jpg_page_file)
                    
            #loop through the jp2 files that are associated with the mods
            for tif_page_file in book_page_tif_page_files:
                #create an object for each
                page_name = tif_page_file[tif_page_file.find('-') + 1:tif_page_file.find('.')]
                #page_pid = fedora.getNextPID(name_space)
                page_label = book_label + '-' + page_name
                page_pid = name_space + book_pid[book_pid.find(':'):] + '-' + page_name
                page_label = unicode(page_label)
                page_object = fedora.createObject(page_pid, label = page_label)
                tif_page_file_path = os.path.join(source_directory, 'tif', tif_page_file)
		jp2_page_file_path = os.path.join(source_directory, 'jp2', jp2_page_file) 
                tn_page_file_path = os.path.join(source_directory, 'tn', tn_page_file) 
                jpg_page_file_path = os.path.join(source_directory, 'jpg', jpg_page_file)   
 
                #add tn ds
                if page_name == '00000':
                    tn_page_file_handle = open(tn_page_file_path, 'rb')
                    try:
                        book_object.addDataStream(u'TN', u'aTmpStr', label = u'TN',
                        mimeType = u'image/jpg', controlGroup = u'M',
                        logMessage = u'Added TN datastream.')
                        datastream = book_object['TN']
                        datastream.setContent(tn_page_file_handle)
                        logging.info('Added TN datastream to:' + book_pid)
                    except FedoraConnectionException:
                        logging.error('Error in adding TN datastream to:' + book_pid + '\n')
                    tn_page_file_handle.close()

                #add tn ds
                jpg_page_file_handle = open(jpg_page_file_path, 'rb')
                try:
                    book_object.addDataStream(u'IMG-JPG', u'aTmpStr', label = u'IMT-JPG',
                    mimeType = u'image/jpg', controlGroup = u'M',
                    logMessage = u'Added TN datastream.')
                    datastream = page_object['IMT-JPG']
                    datastream.setContent(jpg_page_file_handle)
                    logging.info('Added TN datastream to:' + page_pid)
                except FedoraConnectionException:
                    logging.error('Error in adding TN datastream to:' + page_pid + '\n')
                jpg_page_file_handle.close()
                
                #add jp2 ds
                jp2_page_file_handle = open(jp2_page_file_path, 'rb')
                try:
                    page_object.addDataStream(u'JP2', u'aTmpStr', label=u'JP2',
                    mimeType = u'image/jp2', controlGroup = u'M',
                    logMessage = u'Added JP2 datastream.')
                    datastream = page_object['JP2']
                    datastream.setContent(jp2_page_file_handle)
                    logging.info('Added JP2 datastream to:' + page_pid)
                except FedoraConnectionException:
                    logging.error('Error in adding JP2 datastream to:' + page_pid + '\n')
                jp2_page_file_handle.close()
                
                #add jp2 ds
                tif_page_file_handle = open(tif_page_file_path, 'rb')
                try:
                    page_object.addDataStream(u'OBJ', u'aTmpStr', label=u'OBJ',
                    mimeType = u'image/tif', controlGroup = u'M',
                    logMessage = u'Added TIF datastream.')
                    datastream = page_object['OBJ']
                    datastream.setContent(tif_page_file_handle)
                    logging.info('Added TIF datastream to:' + page_pid)
                except FedoraConnectionException:
                    logging.error('Error in adding TIF datastream to:' + page_pid + '\n')
                tif_page_file_handle.close()

                #add relationships
                objRelsExt=fedora_relationships.rels_ext(page_object, fedora_model_namespace)
                objRelsExt.addRelationship('isMemberOf', book_pid)
                objRelsExt.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'),'islandora:pageCModel')
                objRelsExt.update()
    sys.exit()
