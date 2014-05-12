Contributions
=============

Contributions welcome, only a few rules should be obeyed:

* reusability  
  Code is written with re-useability in mind.
  (no hardcoded local paths, settings, local database tables)
* relevance  
  Code should not be too generic, and actually tie into scrapy.
  (no formatter script for your output json files,
   but maybe if it's generic and a scrapy pipeline/extension)
* prefer existing code  
  Adding another e.g. mysql adbapi pipeline is instead of generalizing
  an existing one is undesirable. If it's using a different library
  (e.g. sqlalchemy) instead, to do the same thing, it's welcome.

About code formatting
---------------------
I don't believe in living by the pep8 bible. My preference are Tabs,
and I save 3 bytes on every indent versus PEP-8 style.

If you choose Tabs for indent (and spaces for alignment, if necessary),
`expand -t4`/`unexpand -t4` or a quality editor can convert to your
preferred indentation level locally and reversibly.

If you choose Spaces, follow PEP-8 (4 spaces).
Both choices are acceptable, but never mix styles in a file and don't
convert others existing files without good reason (major rewrite).

Breaking lines at exactly 72 [CPL][1] is not so useful either
if legibility is worse for it. Use best judgement.

----
[1]: https://en.wikipedia.org/wiki/Characters_per_line
