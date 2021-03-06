'''
Created on Feb. 01, 2012
This file handles the batch ingest of digitized books for McMaster University Libray
@adapted from Will Panting's Hamilton College ingest script (https://github.com/DGI-Hamilton-College/Hamilton_ingest)
@author: Nick Ruest
'''
import logging, sys, os, ConfigParser, time, subprocess#, shutil
from fcrepo.connection import Connection, FedoraConnectionException
from fcrepo.client import FedoraClient
from islandoraUtils.metadata import fedora_relationships
from lxml import etree
#IMPORT BAGIT.PY

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
    
    # Need to set a variable that will grab the name of the root folder of book, then the underlying folders (Images, Metadata, OCR, PDF)
    # need ingest data streams for: pages, ocr text files, ocr position files, pdf, mods, mets, marc, dc, job, individual mix metadata

    # DATASTREAMS:
    # Individual page files
    # Individual mix xml
    # Individual OCR position files
    # Individual OCR text files
    # $ID_BatchProcess.xml
    # $ID_BookMetadata.xml
    # $ID_DC.xml
    # $ID_Manifest.xml
    # $ID_METS.xml
    # $ID_MissingPages.log
    # $ID_MODS.xml
    # $ID.mrc
    # $ID_MRC.xml
    # $ID_PlaceHolderForFoldout.log
    # $ID_ProcJob.xml
    # $ID_RefNum.xml
    # $ID_ScanJob.xml
    # OCRJob.xml
    # pdf file

    ## FEATURE ##
    # Once a book is ingested, it should be bagged. The bag metadata should be pulled from the MODS or MRC xml file.
    # Once the book is ingested and bagged, it should be moved to the the 'preservation' directory on the storage array

    ## OTHER TODOS ##
    # Grab folder name from book directory - should only grab the number
    # i'm not concerned with multivolume works right now.
    # sub-folder check
    #   - > make sure there are four sub-folders (images, ocr, metadata, pdf)
    #   - > can't worry about case sensitivity because the machine and people are not consistent
    # grab and ingest ALL of the datastreams
    # after a book is ingested, throw it in a bag.
    #   - > grab mods title and identifier fields for external description
    # $$$ PROFIT

    #configure logging
    log_directory = os.path.join(source_directory,'logs')
    if not os.path.isdir(log_directory):
        os.mkdir(log_directory)
    logFile = os.path.join(log_directory,'/big2/dc/Digital-Collections/archival-objects/books' + time.strftime('%y_%m_%d') + '.log')
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

    #get bookID - TODO grab the folder name which should be the oclc number for a book - need to look for multi-volume books
    
    #setup the directories
    books_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/books')
    if not os.path.isdir(books_directory):
       logging.error('books_directory invalid \n')
       sys.exit()
    
		def listdir_nohidden(path):
			""" List directory contents, omitting hidden files """
			for f in os.listdir(path):
				if not f.startswith('.'):
					yield f

		# Check that correct subfolders PDF, METADATA, and IMAGES exist in each object folder
		subfolders = listdir_nohidden(books_directory)
		subfolders = [books_directory + os.path.sep + subfolder for subfolder in subfolders]
		for folder in subfolders:
			expected_folders = ['PDF', 'METADATA', 'IMAGES']
				if not set(listdir_nohidden(folder)) & set(expected_folders):
					logging.error('asdf')
					sys.exit()

    mods_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/books/$bookID/Metadata')
    if not os.path.isdir(mods_directory):
        logging.error('MODS directory invalid \n')
        sys.exit()
    
    pdf_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/ITM/pdf')
    if not os.path.isdir(pdf_directory):
        logging.error('PDF directory invalid \n')
        sys.exit()

    images_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/ITM/images')
    if not os.path.isdir(images_directory):
        logging.error('IMAGES medium directory invalid \n')
        sys.exit()

    ocr_directory = os.path.join(source_directory, '/big2/dc/Digital-Collections/archival-objects/ITM/ocr')
    if not os.path.isdir(ocr_directory):
        logging.error('OCR thumbnail directory invalid \n')
        sys.exit()    
    
    #prep data structures (files)
    metadata_files = os.listdir(metadata_directory)
    pdf_files = os.listdir(pdf_directory)
    images_files = os.listdir(images_directory)
    ocr_files = os.listdir(ocr_directory)
    
    name_space = u'macrepo'
    
    '''
    do ingest
    '''
    #put in the book object
    try:
        collection_label = u'192'
        collection_pid = unicode(name_space + ':' + collection_label)
        collection_policy = u'<collection_policy xmlns="http://www.islandora.ca" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" name="" xsi:schemaLocation="http://www.islandora.ca http://syn.lib.umanitoba.ca/collection_policy.xsd"> <content_models> <content_model dsid="ISLANDORACM" name="Islandora Collection Model ~ islandora:collectionCModel" namespace="islandora:1" pid="islandora:collectionCModel"/> <content_model dsid="ISLANDORACM" name="Islandora large image content model" namespace="macrepo:1" pid="islandora:sp_large_image_cmodel"/> </content_models> <search_terms/> <staging_area/> <relationship>isMemberOfCollection</relationship> </collection_policy> '
        fedora.getObject(collection_pid)
    except FedoraConnectionException, object_fetch_exception:
        if object_fetch_exception.httpcode in [404]:
            logging.info(name_space + ':book missing, creating object.\n')
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
           
            #get book_label from mods title
            mods_tree = etree.parse(mods_file_path)
            book_label = mods_tree.xpath("*[local-name() = 'titleInfo']/*[local-name() = 'title']/text()")
            book_label = map_label[0]
            if len(book_label) > 255:
            collection_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'),'islandora:collectionCModel')
            collection_object_RELS_EXT.update()

    #loop through the mods folder
    for mods_file in mods_files:
        if mods_file.endswith('-MODS.xml'):
            #get mods file contents
            mods_file_path = os.path.join(source_directory, 'mods', mods_file)
            mods_file_handle = open(mods_file_path)
            mods_contents = mods_file_handle.read()
           
            #get book_label from mods title
            mods_tree = etree.parse(mods_file_path)
            book_label = mods_tree.xpath("*[local-name() = 'titleInfo']/*[local-name() = 'title']/text()")
            book_label = map_label[0]
            if len(book_label) > 255:
                book_label = map_label[0:250] + '...'
            #print(book_label)
            book_label = unicode(map_label)
            book_name = mods_tree.xpath("*[local-name() = 'identifier']/text()")[0].strip("\t\n\r")           
 
            #create a book object
            book_pid = fedora.getNextPID(name_space)
	    book_object = fedora.createObject(map_pid, label = map_label)
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

            #add tif datastream - individual book pages
           
            tif_file = book_name + '.tif'
            tif_file_path = os.path.join(source_directory, 'tif', tif_file)
            tif_file_handle = open(tif_file_path, 'rb')
            
            try:
                book_object.addDataStream(u'OBJ', u'aTmpStr', label=u'OBJ',
                mimeType = u'image/tif', controlGroup = u'M',
                logMessage = u'Added TIFF datastream.')
                datastream = book_object['OBJ']
                datastream.setContent(tif_file_handle)
                logging.info('Added TIFF datastream to:' + book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding TIFF datastream to:' + book_pid + '\n')
            tif_file_handle.close()
            
            #add BatchProcess xml
            
            batchProcess_file = book_name + '_BatchProcess.xml'
            batchProcess_file_path = os.path.join(source_directory, 'MetaData', )
            batchProcess_file_handle = open(batchProcess_file_path)
            batchProcess_contents = batchProcess_file_handle.read()

            try:
                book_object.addDataStream(u'BATCHPROCESS', unicode(batchProcess_contents), label = u'BATCHPROCESS',
                mimeType = u'text/xml', controlGroup = u'X',
                logMessage = u'Added Batch Process metadata.')
                logging.info('Added Batch Process datastream to:' + book_pid)
                book_object.addDataStream(u'MANIFEST', unicode(manifest_contents), label = u'MANIFEST',
                mimeType = u'text/xml', controlGroup = u'X',
                logMessage = u'Added manifest.')
                logging.info('Added manifest datastream to:' + book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding manifest datastream to:' + book_pid + '\n')
            manifest_file_handle.close()

            #add MARC xml

            mrcXML_file = book_name + '_MRC.xml'
            mrcXML_file_path = os.path.join(source_directory, 'Metadata', )
            mrcXML_file_handle = open(mrcXML_file_path)
            mrcXML_contents = manifest_file_handle.read()

            try:
                book_object.addDataStream(u'MRCXML', unicode(mrcXML_contents), label = u'MRCXML',
                mimeType = u'text/xml', controlGroup = u'X',
                logMessage = u'Added Marc XML.')
                logging.info('Added Marc XML datastream to:' + book_pid)
           except FedoraConnectionException:
                logging.error('Error in adding Marc XML datastream to:' + book_pid + '\n')
           mrcXML_file_handle.close()

           #add ProcJob xml

           procJob_file = book_name + '_ProcJob.xml'
           procJob_file_path = os.path.join(source_directory, 'Metadata', )
           procJob_file_handle = open(procJob_file_path)
           procJob_contents = procJob_file_handle.read()

           try:
               book_object.addDataStream(u'PROCJOB', unicode(procJob_contents), label = u'PROCJOB',
               mimeType = u'text/xml', controlGroup = u'X',
               logMessage = u'Added ProcJob xml.')
               logging.info('Added ProcJob xml datastream to:' + book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding ProcJob datastream to:' + book_pid + '\n')
            procJob_file_handle.close()
            

            #add ScanJob xml

            scanJob_file = book_name + '_ScanJob.xml'
            scanJob_file_path = os.path.join(source_directory, 'Metadata', )
            scanJob_file_handle open(scanJob_file_path)
            scanJob_contents = scanJob_file_handle.read()

            try:
                book_object.addDataStream(u'SCANJOB', unicode(scanJob_contents), label = u'SCANJOB',
                mimeType = u'text/xml', controlGroup = u'X',
                logMessage = u'Added ScanJob xml.')
                logging.info('Added ScanJob xml datasream to:' + book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding ProcJob datastream to:' + book_pid + '\n')
            scanJob_file_handle.close()

            # add Mets xml

            mets_file = book_name + '_METS.xml'
            mets_file_path = os.path.join(source_directory, 'Metadata', )
            mets_file_handle = open(mets_file_path)
            mets_contents = mets_file_handle.read()

            try:
                book_object.addDataStream(u'METS', unicode(mets_contents), label = u'METS',
                mimeType = u'text/xml', controlGroup = u'X',
                logMessage = u'Added METS xml.')
                logging.info('Added METS xml datastream to:' + book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding METS datastream to:' book_pid + '\n')
            mets_contents = mets_file_handle.close()

            # add RefNum xml

            refNum_file = book_name + '_RefNum.xml'
            refNum_file_path = os.path.join(source_directory, 'Metadata', )
            refNum_file_handle = open(refNum_file_path)
                mimeType = u'text/xml', controlGroup = u'X',
                logMessage = u'Added ScanJob xml.')
                logging.info('Added ScanJob xml datasream to:' + book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding ProcJob datastream to:' + book_pid + '\n')
            scanJob_file_handle.close()

            # add Mets xml

            mets_file = book_name + '_METS.xml'
            mets_file_path = os.path.join(source_directory, 'Metadata', )
            mets_file_handle = open(mets_file_path)
            mets_contents = mets_file_handle.read()

            try:
                book_object.addDataStream(u'METS', unicode(mets_contents), label = u'METS',
                mimeType = u'text/xml', controlGroup = u'X',
                logMessage = u'Added METS xml.')
                logging.info('Added METS xml datastream to:' + book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding METS datastream to:' book_pid + '\n')
            mets_contents = mets_file_handle.close()

            # add RefNum xml

            refNum_file = book_name + '_RefNum.xml'
            refNum_file_path = os.path.join(source_directory, 'Metadata', )
            refNum_file_handle = open(refNum_file_path)
            refNum_contents = refNum_file_handle.read()

            try:
                book_object.addDataStream(u'REFNUM', unicode(refNum_contents), label = u'REFNUM',
                mimeType = u'text/xml', controlGroup = u'X',
                logMessage = u'Added refNum xml.')
                logging.info('Added refNum xml datastream to:' + book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding refNum datastream to:' book_pid + '\n')
            refNum_contents = refNum_file_handle.close()

            # add OCRJob.xml

            ocrJob_file = 'OCRJob.xml'
            ocrJob_file_path = os.path.join(source_directory, 'Metadata', )
            ocrJob_file_handle = open(ocrJob_file_path)
            ocrJob_contents = ocrJob_file_handle.read()

            try:
                book_object.addDataStream(u'OCRJOB', unicode(ocrJob_contetns, label = u'OCRJOB',
                mimeType = u'text/xml', controlGroup = u'X',
                logMessage = u'Added OCRJob xml.')
                logging.info('Added OCRJob xml datastream to: ' + book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding OCRJob datastream to:' book_pid + '\n')
            ocrJob_contents = ocrJob_file_handle.close()

            # add MIX xml

            # add OCR position files

            # add OCR text files

            # add missing pages log

            # add mrc record

            # add foldout log

            # add pdf file

      #add relationships
            objRelsExt = fedora_relationships.rels_ext(map_object, fedora_model_namespace)
            objRelsExt.addRelationship('isMemberOfCollection', collection_pid)
            objRelsExt.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'),'islandora:sp_macbooks_cmodel')
            objRelsExt.update()

      sys.exit()
