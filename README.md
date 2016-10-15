README
this file implements a utility function (and a utility program) that
makes a copy of an SQLite database while simultaneously zeroing out all
deleted content, drop out free-list pages and defrag the database pages.

It's faster than "VACUUM" command.

 If compiled with -DDEFRAG_STANDALONE then a main() procedure is added and
 this file becomes a standalone program that can be run as follows:

      gcc defrag.c -lsqlite3 -O2 -DDEFRAG_STANDALONE -o sqlite3defrag
      ./sqlite3defrag SOURCE DEST

this utility based on "scrub" tool find in official SQLite: 
    http://www.sqlite.org/src/artifact/1c5bfb8b0cd18b60

