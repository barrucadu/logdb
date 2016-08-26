logdb [![Build Status][travis-badge]][travis] [![Report Card][goreport-badge]][goreport] [![Coverage Status][coveralls-badge]][coveralls] [![GoDoc][godoc-badge]][godoc]
=====

LogDB is a Go library for efficient log-structured databases.  A log-structured
database is a very simple data store where writes are only ever appended to the
database, there are no random-access writes at all.  To prevent the database
from growing indefinitely, a contiguous chunk of entries can be removed from
either the beginning or the end.

This library is efficient and provides consistency guarantees: an entry is
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


Data Consistency
----------------

The guiding principle for consistency and correctness is that, no
matter where the program is interrupted, there should never be silent
corruption in the database. In particular, this means:

- If an `Append` is interrupted, or a `Sync` following an `Append` is
  interrupted, the entry will either be there or not: if the entry is
  accessible, it will have the expected contents.

- If an `AppendEntries` is interrupted, or a `Sync` following an
  `AppendEntries` is interrupted, some of the entries may not be
  there, but there will be no gaps: if any messages are there, it will
  be an initial portion of the appended ones, with the expected
  contents (as with `Append`).

- If a `Forget`, `Rollback`, or `Truncate` is interrupted, or a `Sync`
  following one of those is interrupted, some of the entries may
  remain: but there will be no gaps; it won't be possible to access
  both entry `x` and entry `y`, unless all entries between `x` and `y`
  are also accessible.

As the database is so simple, ensuring this data consistency isn't the
great challenge it is in more fully-featured database systems. Care is
taken to sync chunk data files before writing out chunk metadata
files, and metadata files are implemented as an append-only log. A
sensible default can be recovered for the one non-append-only piece of
metadata (the ID of the oldest visible entry in the database (which,
due to a `Forget` may be newer than the ID of the oldest entry in the
database)) if it is corrupted or lost.

If it is impossible to unambiguously and safely open a database, an
error is returned. Otherwise, automatic recovery is performed. If an
error occurs, please file a bug report including the error message and
a description of what was being done to the database when the process
terminated, as this shouldn't happen without external tampering.


Contributing
------------

Bug reports, pull requests, and comments are very welcome!

Feel free to contact me on GitHub, through IRC (on freenode), or email
(mike@barrucadu.co.uk).
