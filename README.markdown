logdb [![Build Status][travis-badge]][travis] [![Report Card][goreport-badge]][goreport] [![Coverage Status][coveralls-badge]][coveralls] [![GoDoc][godoc-badge]][godoc]
=====

LogDB is a Go library for efficient log-structured databases.  A log-structured
database is a very simple data store where writes are only ever appended to the
database, there are no random-access writes at all.  To prevent the database
from growing indefinitely, a contiguous chunk of entries can be removed from
either the beginning or the end.

This library is efficient and provides ACID consistency guarantees: an entry is
either stored or it is not, even in the event of power loss during execution
the database cannot be left in an inconsistent state.

The godoc is [available online][godoc].

[travis]: <https://travis-ci.org/barrucadu/logdb>
[travis-badge]: <https://travis-ci.org/barrucadu/logdb.svg?branch=master>
[goreport]: <https://goreportcard.com/report/github.com/barrucadu/logdb>
[goreport-badge]: <https://img.shields.io/badge/go_report-A-brightgreen.svg?style=flat>
[coveralls]: <https://coveralls.io/r/barrucadu/logdb?branch=master>
[coveralls-badge]: <https://coveralls.io/repos/barrucadu/logdb/badge.svg?branch=master>
[godoc]: <https://godoc.org/github.com/barrucadu/logdb>
[godoc-badge]: <https://godoc.org/github.com/barrucadu/logdb?status.svg>


Project Status
--------------

Very early days.  The API is unstable, and everything is in flux.


Contributing
------------

Bug reports, pull requests, and comments are very welcome!

Feel free to contact me on GitHub, through IRC (on freenode), or email
(mike@barrucadu.co.uk).
