import os
import subprocess
import log

PANDOC_PATH = '/usr/bin/pandoc'

def pandoc_convert(content):
	""" pandoc text conversion """
	informat = 'html'
	outformat = 'plain'
	converter  = [PANDOC_PATH, '--from=%s' % informat, '--to=%s' % outformat]
	#converter += ['--ascii', '-s', '--toc'] # some extra options
	log.msg('Converting page content: %s' % converter)
	os.environ['PYTHONIOENCODING'] = 'utf-8'
	p = subprocess.Popen(
		converter,
		stdin=subprocess.PIPE,
		stdout=subprocess.PIPE
	)
	content = content.encode('utf-8')
	return p.communicate(content)[0].decode('utf-8')
