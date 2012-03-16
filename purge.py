#!/usr/bin/env python
#fcrepo purge utility written by Matt McCollow & Nick Ruest
#usage: 'python purge.py START_PID END_PID'

import sys
import logging, sys, os, ConfigParser, time, subprocess
from fcrepo.connection import Connection, FedoraConnectionException
from fcrepo.client import FedoraClient

#get config
config = ConfigParser.ConfigParser()
config.read('mcmaster.cfg')
fedoraUrl = config.get('Fedora','url')
fedoraUserName = config.get('Fedora', 'username')
fedoraPassword = config.get('Fedora','password')

START_PID = int(sys.argv[1])
END_PID = int(sys.argv[2])

c = Connection(fedoraUrl, fedoraUserName, fedoraPassword)
fc = FedoraClient(c)

for i in range((START_PID), (END_PID)+1):
	pids = []
	term = u'pid~macrepo:' + unicode(i) + '-*'
	results = fc.searchObjects(term, ['pid'])
	for r in results:	
		pids.append(r['pid'])
	for pid in pids:
		fc.deleteObject(pid[0])
		print(pid)[0]
c.close()
