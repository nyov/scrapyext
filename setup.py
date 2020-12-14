try:
	from setuptools import setup
except ImportError:
	from distutils.core import setup

install_requires = [
	# dependencies of this codebase
	'itemloaders>=1.0.1',
]

setup(
	name = 'scrapyext',
	version = '0.0',
	license = 'BSD',
	description = 'Scrapy extensions',
	author = 'Scrapy developers',
	url = 'https://github.com/nyov/scrapyext',
	packages = ['scrapyext', 'scrapyext.tests'],
	platforms = ['Any'],
	classifiers = [
		'License :: OSI Approved :: BSD License',
		'Operating System :: OS Independent',
		'Programming Language :: Python'
	],
	install_requires=install_requires,
)
