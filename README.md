README
this file implements a utility function (and a utility program) that
makes a copy of an SQLite database while simultaneously zeroing out all
deleted content, drop out free-list pages and defrag the database pages.

It's faster than "VACUUM" command.
