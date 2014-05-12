scrapyext
=========

scrapy-extras is a collection of code samples and modules for the Scrapy framework.

Quality of code may differ vastly between modules and anything here might not be
fit for (any) use. No warranties.

Manifest
--------
[scrapyext][scrapyext.git] is meant as a source for additional modules and code
fragments, which may be useful and reuseable, but don't necessarily meet the
requirements for inclusion in a 'official' code library.

[Scrapy][scrapy.git] and [scrapylib][scrapylib.git] take in well-written and
well-tested code fitting the projects, but additions which don't (because of
missing tests, being simple hacks, lacking quality, or simply too removed in
functionality to fit the library without bloating it unnecessarily) might be
lost to others who could still find them useful for ideas or would take the
time to improve them.

As such `scrapyext` should be seen as a namespace to find scrapy modules,
hacks and related code, that are outside the scope of scrapy/lib,  without
hunting down gists and snippets all over the web.

Code in this repo
-----------------
* doesn't require (but welcomes) unittests,
* may not follow best practices,
* may be deprecated,
* may not work with current scrapy versions,
* isn't officially endorsed or supported by anyone,
* must be generic enough to be useful across projects,
* should be BSD licensed to allow merging with scrapy's codebase.

License
-------
Code is licensed (3-clause) BSD, unless noted otherwise.

----
[scrapy.git]:    https://github.com/scrapy/scrapy
[scrapyd.git]:   https://github.com/scrapy/scrapyd
[scrapylib.git]: https://github.com/scrapinghub/scrapylib
[scrapyext.git]: https://github.com/nyov/scrapyext
