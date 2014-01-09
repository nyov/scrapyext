import os
import subprocess

PDFTOTXT = '/usr/bin/pdftotext'

def pdf_convert(filestream, *params):
	"""PDF text extraction"""
	# pdftotext -layout -eol unix file.pdf -
	# converts from stdin pdf to stdout text
	converter  = [PDFTOTXT]
	for param in params:
		converter.extend([param])
	converter.extend(['-', '-'])
	os.environ['PYTHONIOENCODING'] = 'utf-8'
	proc = subprocess.Popen(
		converter,
		shell=False,
		stdin=subprocess.PIPE,
		stdout=subprocess.PIPE,
		stderr=subprocess.PIPE
	)
	output = proc.communicate(filestream)[0]
	exit_code = proc.wait()
	return exit_code, output
