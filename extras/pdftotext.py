import os
import subprocess

PDFTOTXT = '/usr/bin/pdftotext' # `apt-get install poppler-utils` on debian

def pdf_convert(filestream, *params):
    """PDF text extraction"""
    # pdftotext -layout -eol unix file.pdf -
    # converts from stdin pdf to stdout text
    cmd = [PDFTOTXT]
    for param in params:
        cmd += [param]
    cmd += ['-', '-']
    _env = os.environ.get('PYTHONIOENCODING', '')
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    proc = subprocess.Popen(
        cmd,
        shell=False,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    output = proc.communicate(filestream)[0]
    exit_code = proc.wait()
    os.environ['PYTHONIOENCODING'] = _env
    return exit_code, output

if __name__ == '__main__':
    import sys

    filename, = sys.argv[1:2] or [None]
    text = b''
    if filename:
        with open(filename, 'rb') as f:
            exit_code, text = pdf_convert(f.read())
            if not exit_code:
                text = text.decode('utf-8')
                print (text)
