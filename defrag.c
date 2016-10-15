/*
** 2016-10-15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This file implements a utility function (and a utility program) that
** makes a copy of an SQLite database while simultaneously zeroing out all
** deleted content, drop out free-list pages and defrag the database pages.
**
** Normally (when PRAGMA secure_delete=OFF, which is the default) when SQLite
** deletes content, it does not overwrite the deleted content but rather marks
** the region of the file that held that content as being reusable.  This can
** cause deleted content to recoverable from the database file.  This stale
** content is removed by the VACUUM command, but VACUUM can be expensive for
** large databases.  When in PRAGMA secure_delete=ON mode, the deleted content
** is zeroed, but secure_delete=ON has overhead as well.
**
** This utility attempts to make a copy of a complete SQLite database where
** all of the deleted content is zeroed out, all the data pages is rearranged 
** in the copy, and attempts to do so while being faster than running VACUUM.
**
** This utility set autovacuum to off on destination database.
**
** Usage:
**
**   int sqlite3_scrub_and_defrag(
**       const char *zSourceFile,   // Source database filename
**       const char *zDestFile,     // Destination database filename
**       char **pzErrMsg            // Write error message here
**   );
**
** Simply call the API above specifying the filename of the source database
** and the name of the backup copy.  The source database must already exist
** and can be in active use. (A read lock is held during the backup.)  The
** destination file should not previously exist.  If the pzErrMsg parameter
** is non-NULL and if an error occurs, then an error message might be written
** into memory obtained from sqlite3_malloc() and *pzErrMsg made to point to
** that error message.  But if the error is an OOM, the error might not be
** reported.  The routine always returns non-zero if there is an error.
**
** If compiled with -DDEFRAG_STANDALONE then a main() procedure is added and
** this file becomes a standalone program that can be run as follows:
**
**      ./sqlite3defrag SOURCE DEST
**
*/
#include "sqlite3.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>

typedef struct ScrubDefragState ScrubDefragState;
typedef unsigned char u8;
typedef unsigned short u16;
typedef unsigned int u32;


/* State information for a scrub-and-defrag operation */
struct ScrubDefragState {
  const char *zSrcFile;    /* Name of the source file */
  const char *zDestFile;   /* Name of the destination file */
  int rcErr;               /* Error code */
  char *zErr;              /* Error message text */
  sqlite3 *dbSrc;          /* Source database connection */
  sqlite3_file *pSrc;      /* Source file handle */
  sqlite3 *dbDest;         /* Destination database connection */
  sqlite3_file *pDest;     /* Destination file handle */
  u32 szPage;              /* Page size */
  u32 szUsable;            /* Usable bytes on each page */
  u32 nDestPage;           /* # pages of destination database */
  u32 nSrcPage;            /* # pages of source database*/
  u32 nFreePage;           /* Number of freelist pages */
  u8 *page1;               /* Content of page 1 */
  u32 iDestPageNo;         /* Current Destination database page no */
  u32 iLock;               /* Lock page number */
};

static void scrubDefragIncDestPageNo(ScrubDefragState *p){
  p->iDestPageNo++;
  if(p->iDestPageNo == p->iLock) p->iDestPageNo++;
}
/* Store an error message */
static void scrubDefragErr(ScrubDefragState *p, const char *zFormat, ...){
  va_list ap;
  sqlite3_free(p->zErr);
  va_start(ap, zFormat);
  p->zErr = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
  if( p->rcErr==0 ) p->rcErr = SQLITE_ERROR;
}

/* Allocate memory to hold a single page of content */
static u8 *scrubDefragAllocPage(ScrubDefragState *p){
  u8 *pPage;
  if( p->rcErr ) return 0;
  pPage = sqlite3_malloc( p->szPage );
  if( pPage==0 ) p->rcErr = SQLITE_NOMEM;
  return pPage;
}

/* Read a page from the source database into memory.  Use the memory
** provided by pBuf if not NULL or allocate a new page if pBuf==NULL.
*/
static u8 *scrubDefragRead(ScrubDefragState *p, int pgno, u8 *pBuf){
  int rc;
  sqlite3_int64 iOff;
  u8 *pOut = pBuf;
  if( p->rcErr ) return 0;
  if( pOut==0 ){
    pOut = scrubDefragAllocPage(p);
    if( pOut==0 ) return 0;
  }
  iOff = (pgno-1)*(sqlite3_int64)p->szPage;
  rc = p->pSrc->pMethods->xRead(p->pSrc, pOut, p->szPage, iOff);
  if( rc!=SQLITE_OK ){
    if( pBuf==0 ) sqlite3_free(pOut);
    pOut = 0;
    scrubDefragErr(p, "read failed for page %d", pgno);
    p->rcErr = SQLITE_IOERR;
  }
  return pOut;  
}

/* Write a page to the destination database */
static void scrubDefragWrite(ScrubDefragState *p, int pgno, const u8 *pData){
  int rc;
  sqlite3_int64 iOff;
  if( p->rcErr ) return;
  if( pgno > p->nDestPage ){
    scrubDefragErr(p, "internal logic error or database is corrupt, "
                   "please run 'pragma integrity_check' on database: %s",
                   p->zSrcFile);
    p->rcErr = SQLITE_CORRUPT;
    return;
  }
  iOff = (pgno-1)*(sqlite3_int64)p->szPage;
  rc = p->pDest->pMethods->xWrite(p->pDest, pData, p->szPage, iOff);
  if( rc!=SQLITE_OK ){
    scrubDefragErr(p, "write failed for page %d", pgno);
    p->rcErr = SQLITE_IOERR;
  }
}

/* Prepare a statement against the "db" database. */
static sqlite3_stmt *scrubDefragPrepare(
  ScrubDefragState *p,      /* Backup context */
  sqlite3 *db,        /* Database to prepare against */
  const char *zSql    /* SQL statement */
){
  sqlite3_stmt *pStmt;
  if( p->rcErr ) return 0;
  p->rcErr = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( p->rcErr ){
    scrubDefragErr(p, "SQL error \"%s\" on \"%s\"",
                   sqlite3_errmsg(db), zSql);
    sqlite3_finalize(pStmt);
    return 0;
  }
  return pStmt;
}

/* Execute  SQL statement and return an int value */
static void scrubDefragDbInt(
  ScrubDefragState *p,
  const char* zSql,
  int * pRes,
  const char* zErr
){
  int rc;
  sqlite3_stmt *pStmt;
  if( p->rcErr ) return;
  pStmt = scrubDefragPrepare(p, p->dbSrc, zSql);
  if( pStmt==0)  return; 
  rc = sqlite3_step(pStmt);
  if( rc==SQLITE_ROW ){
     *pRes = sqlite3_column_int(pStmt , 0);
  }
  p->rcErr = sqlite3_finalize(pStmt);
  if( p->rcErr ) {
    scrubDefragErr(p, zErr);
  }
}
/* Open the source database file */
static void scrubDefragOpenSrc(ScrubDefragState *p){
  sqlite3_stmt *pStmt;
  int rc;
  /* Open the source database file */
  p->rcErr = sqlite3_open_v2(p->zSrcFile, &p->dbSrc,
                 SQLITE_OPEN_READWRITE |
                 SQLITE_OPEN_URI | SQLITE_OPEN_PRIVATECACHE, 0);
  if( p->rcErr ){
    scrubDefragErr(p, "cannot open source database: %s",
                      sqlite3_errmsg(p->dbSrc));
    return;
  }
  p->rcErr = sqlite3_exec(p->dbSrc, "SELECT 1 FROM sqlite_master; BEGIN;",
                          0, 0, 0);
  if( p->rcErr ){
    scrubDefragErr(p,
       "cannot start a read transaction on the source database: %s",
       sqlite3_errmsg(p->dbSrc));
    return;
  }
  rc = sqlite3_wal_checkpoint_v2(p->dbSrc, "main", SQLITE_CHECKPOINT_FULL,
                                 0, 0);
  if( rc ){
    scrubDefragErr(p, "cannot checkpoint the source database");
    return;
  }
  scrubDefragDbInt(p, "PRAGMA page_size", &p->szPage, 
                      "unable to determine the page size");
  if( p->rcErr ) return;
  scrubDefragDbInt(p, "PRAGMA page_count", &p->nSrcPage, 
                      "unable to determine the size of the source database");
  if( p->rcErr ) return;

  scrubDefragDbInt(p, "PRAGMA freelist_count", &p->nFreePage, 
                      "unable to determine the free-list size of the source database");
  if( p->rcErr ) return;

  sqlite3_file_control(p->dbSrc, "main", SQLITE_FCNTL_FILE_POINTER, &p->pSrc);
  if( p->pSrc==0 || p->pSrc->pMethods==0 ){
    scrubDefragErr(p, "cannot get the source file handle");
    p->rcErr = SQLITE_ERROR;
  }
}

/* Create and open the destination file */
static void scrubDefragOpenDest(ScrubDefragState *p){
  sqlite3_stmt *pStmt;
  int rc;
  char *zSql;
  if( p->rcErr ) return;
  p->rcErr = sqlite3_open_v2(p->zDestFile, &p->dbDest,
                 SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                 SQLITE_OPEN_URI | SQLITE_OPEN_PRIVATECACHE, 0);
  if( p->rcErr ){
    scrubDefragErr(p, "cannot open destination database: %s",
                      sqlite3_errmsg(p->dbDest));
    return;
  }
  zSql = sqlite3_mprintf("PRAGMA page_size(%u);", p->szPage);
  if( zSql==0 ){
    p->rcErr = SQLITE_NOMEM;
    return;
  }
  p->rcErr = sqlite3_exec(p->dbDest, zSql, 0, 0, 0);
  sqlite3_free(zSql);
  if( p->rcErr ){
    scrubDefragErr(p,
       "cannot set the page size on the destination database: %s",
       sqlite3_errmsg(p->dbDest));
    return;
  }
  sqlite3_exec(p->dbDest, "PRAGMA journal_mode=OFF;", 0, 0, 0);
  p->rcErr = sqlite3_exec(p->dbDest, "BEGIN EXCLUSIVE;", 0, 0, 0);
  if( p->rcErr ){
    scrubDefragErr(p,
       "cannot start a write transaction on the destination database: %s",
       sqlite3_errmsg(p->dbDest));
    return;
  }
  pStmt = scrubDefragPrepare(p, p->dbDest, "PRAGMA page_count;");
  if( pStmt==0 ) return;
  rc = sqlite3_step(pStmt);
  if( rc!=SQLITE_ROW ){
    scrubDefragErr(p, "cannot measure the size of the destination");
  }else if( sqlite3_column_int(pStmt, 0)>1 ){
    scrubDefragErr(p, "destination database is not empty - holds %d pages",
                   sqlite3_column_int(pStmt, 0));
  }
  sqlite3_finalize(pStmt);
  sqlite3_file_control(p->dbDest, "main", SQLITE_FCNTL_FILE_POINTER, &p->pDest);
  if( p->pDest==0 || p->pDest->pMethods==0 ){
    scrubDefragErr(p, "cannot get the destination file handle");
    p->rcErr = SQLITE_ERROR;
  }
}

/* Read a 32-bit big-endian integer */
static u32 scrubDefragInt32(const u8 *a){
  u32 v = a[3];
  v += ((u32)a[2])<<8;
  v += ((u32)a[1])<<16;
  v += ((u32)a[0])<<24;
  return v;
}

static void scrubDefragWriteInt32(u8 *a, const u32 v){
  a[0] = (v >> 24) & 0xff;
  a[1] = (v >> 16) & 0xff;
  a[2] = (v >> 8 ) & 0xff;
  a[3] = v & 0xff;
}

/* Read a 16-bit big-endian integer */
static u32 scrubDefragInt16(const u8 *a){
  return (a[0]<<8) + a[1];
}

/*
** Read a varint.  Put the value in *pVal and return the number of bytes.
*/
static int scrubDefragVarint(const u8 *z, sqlite3_int64 *pVal){
  sqlite3_int64 v = 0;
  int i;
  for(i=0; i<8; i++){
    v = (v<<7) + (z[i]&0x7f);
    if( (z[i]&0x80)==0 ){ *pVal = v; return i+1; }
  }
  v = (v<<8) + (z[i]&0xff);
  *pVal = v;
  return 9;
}

/*
** Return the number of bytes in a varint.
*/
static int scrubDefragVarintSize(const u8 *z){
  int i;
  for(i=0; i<8; i++){
    if( (z[i]&0x80)==0 ){ return i+1; }
  }
  return 9;
}

/*
** Copy an overflow chain from source to destination.  Zero out any
** unused tail at the end of the overflow chain.
*/
static void scrubDefragOverflow(ScrubDefragState *p, int pgno, u32 nByte){
  u8 *a, *aBuf;
  u32 iCurrentPageNo;

  aBuf = scrubDefragAllocPage(p);
  if( aBuf==0 ) return;
  while( nByte>0 && pgno!=0 ){
    a = scrubDefragRead(p, pgno, aBuf);
    if( a==0 ) break;
    if( nByte >= (p->szUsable)-4 ){
      nByte -= (p->szUsable) - 4;
    }else{
      u32 x = (p->szUsable - 4) - nByte;
      u32 i = p->szUsable - x;
      memset(&a[i], 0, x);
      nByte = 0;
    }
    pgno = scrubDefragInt32(a);
    iCurrentPageNo = p->iDestPageNo;
    if(pgno !=0) {
      scrubDefragIncDestPageNo(p);
      scrubDefragWriteInt32(a, p->iDestPageNo);
    }
    scrubDefragWrite(p, iCurrentPageNo, a);
  }
  sqlite3_free(aBuf);      
}
   

/*
** Copy B-Tree page pgno, and all of its children, from source to destination.
** Zero out deleted content during the copy.
*/
static void scrubDefragBtree(ScrubDefragState *p, int pgno, int iDepth, int bRoot){
  u8 *a;
  u32 i, n, pc;
  u32 nCell;
  u32 nPrefix;
  u32 szHdr;
  u32 iChild;
  u8 *aTop;
  u8 *aCell;
  u32 x, y;
  int ln = 0;
  u32 iCurrentPageNo = p->iDestPageNo;
  
  if( p->rcErr ) return;
  if( iDepth>50 ){
    scrubDefragErr(p, "corrupt: b-tree too deep at page %d", pgno);
    return;
  }
  if( pgno==1 ){
    a = p->page1;
  }else{
    a = scrubDefragRead(p, pgno, 0);
    if( a==0 )  return;
  }
  nPrefix = pgno==1 ? 100 : 0;
  aTop = &a[nPrefix];
  szHdr = 8 + 4*(aTop[0]==0x02 || aTop[0]==0x05);
  aCell = aTop + szHdr;
  nCell = scrubDefragInt16(&aTop[3]);

  /* Zero out the gap between the cell index and the start of the
  ** cell content area */
  x = scrubDefragInt16(&aTop[5]);  /* First byte of cell content area */
  if( x>p->szUsable ){ ln=__LINE__; goto btree_corrupt; }
  y = szHdr + nPrefix + nCell*2;
  if( y>x ){ ln=__LINE__; goto btree_corrupt; }
  if( y<x ) memset(a+y, 0, x-y);  /* Zero the gap */

  /* Zero out all the free blocks */  
  pc = scrubDefragInt16(&aTop[1]);
  if( pc>0 && pc<x ){ ln=__LINE__; goto btree_corrupt; }
  while( pc ){
    if( pc>(p->szUsable)-4 ){ ln=__LINE__; goto btree_corrupt; }
    n = scrubDefragInt16(&a[pc+2]);
    if( pc+n>(p->szUsable) ){ ln=__LINE__; goto btree_corrupt; }
    if( n>4 ) memset(&a[pc+4], 0, n-4);
    x = scrubDefragInt16(&a[pc]);
    if( x<pc+4 && x>0 ){ ln=__LINE__; goto btree_corrupt; }
    pc = x;
  }

  /* Walk the tree and process child pages */
  for(i=0; i<nCell; i++){
    u32 X, M, K, nLocal;
    sqlite3_int64 P;
    pc = scrubDefragInt16(&aCell[i*2]);
    if( pc <= szHdr ){ ln=__LINE__; goto btree_corrupt; }
    if( pc > p->szUsable-3 ){ ln=__LINE__; goto btree_corrupt; }
    if( aTop[0]==0x05 || aTop[0]==0x02 ){
      if( pc+4 > p->szUsable ){ ln=__LINE__; goto btree_corrupt; }
      iChild = scrubDefragInt32(&a[pc]);
      assert(iChild); 
      scrubDefragIncDestPageNo(p);
      scrubDefragWriteInt32(&a[pc], p->iDestPageNo);
      pc += 4;
      scrubDefragBtree(p, iChild, iDepth+1, 0);
      if( aTop[0]==0x05 ) continue;
    }
    pc += scrubDefragVarint(&a[pc], &P);
    if( pc >= p->szUsable ){ ln=__LINE__; goto btree_corrupt; }
    if( aTop[0]==0x0d ){
      X = p->szUsable - 35;
    }else{
      X = ((p->szUsable - 12)*64/255) - 23;
    }
    if( P<=X ){
      /* All content is local.  No overflow */
      continue;
    }
    M = ((p->szUsable - 12)*32/255)-23;
    K = M + ((P-M)%(p->szUsable-4));
    if( aTop[0]==0x0d ){
      pc += scrubDefragVarintSize(&a[pc]);
      if( pc > (p->szUsable-4) ){ ln=__LINE__; goto btree_corrupt; }
    }
    nLocal = K<=X ? K : M;
    if( pc+nLocal > p->szUsable-4 ){ ln=__LINE__; goto btree_corrupt; }
    iChild = scrubDefragInt32(&a[pc+nLocal]);
    assert(iChild); 
    scrubDefragIncDestPageNo(p);
    scrubDefragWriteInt32(&a[pc+nLocal], p->iDestPageNo);
    scrubDefragOverflow(p, iChild, P-nLocal);
  }

  /* Walk the right-most tree */
  if( aTop[0]==0x05 || aTop[0]==0x02 ){
    iChild = scrubDefragInt32(&aTop[8]);
    scrubDefragIncDestPageNo(p);
    scrubDefragWriteInt32(&aTop[8], p->iDestPageNo);
    scrubDefragBtree(p, iChild, iDepth+1, 0);
  }
  if(bRoot) {
      scrubDefragIncDestPageNo(p);
  }

  /* Write this one page */
  scrubDefragWrite(p, iCurrentPageNo, a);

  /* All done */
  if( pgno>1 ) sqlite3_free(a);
  return;

btree_corrupt:
  scrubDefragErr(p, "corruption on page %d of source database (errid=%d)",
                 pgno, ln);
  if( pgno>1 ) sqlite3_free(a);  
}

int sqlite3_scrub_and_defrag(
  const char *zSrcFile,    /* Source file */
  const char *zDestFile,   /* Destination file */
  char **pzErr             /* Write error here if non-NULL */
){
  ScrubDefragState s;
  u32 n, i;
  sqlite3_stmt *pStmt;
  char* errmsg=0;
  char* zSql = sqlite3_mprintf("%s","BEGIN EXCLUSIVE;\nPRAGMA writable_schema=on;");

  memset(&s, 0, sizeof(s));
  s.zSrcFile = zSrcFile;
  s.zDestFile = zDestFile;
  s.iDestPageNo = 1;

  /* Open both source and destination databases */
  scrubDefragOpenSrc(&s);
  scrubDefragOpenDest(&s);
  if (s.rcErr) goto scrub_abort;

  s.iLock = (1073742335/s.szPage)+1;
  /* Read in page 1 */
  s.page1 = scrubDefragRead(&s, 1, 0);
  if( s.page1==0 ) goto scrub_abort;

  s.nDestPage = s.nSrcPage - s.nFreePage;
  if(s.nSrcPage >= s.iLock && s.nDestPage < s.iLock){
    s.nDestPage--; 
  }
  scrubDefragWriteInt32(&s.page1[28], s.nDestPage);
  /* First freelist trunk page */
  scrubDefragWriteInt32(&s.page1[32], 0);
  /* freelist count */
  scrubDefragWriteInt32(&s.page1[36], 0);
  /* autovacuum */
  scrubDefragWriteInt32(&s.page1[52], 0);

  s.szUsable = s.szPage - s.page1[20];

  /* Copy all of the btrees */
  scrubDefragBtree(&s, 1, 0, 1);
  pStmt = scrubDefragPrepare(&s, s.dbSrc,
      "SELECT rootpage,name,type FROM sqlite_master WHERE coalesce(rootpage,0)>0"
      "   ORDER BY CASE type WHEN 'table' THEN 2 "
      "                      WHEN 'index' THEN 1 "
      "                      ELSE 0 END, rootpage");
  if( pStmt==0 ) goto scrub_abort;
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    i = (u32)sqlite3_column_int(pStmt, 0);
    zSql = sqlite3_mprintf("%z\nUPDATE SQLITE_MASTER SET rootpage=%d "
                           "  WHERE rootpage=%d AND name=%Q AND type=%Q;",
                           zSql, 
                           s.iDestPageNo, 
                           sqlite3_column_int(pStmt, 0),
                           sqlite3_column_text(pStmt, 1), 
                           sqlite3_column_text(pStmt, 2));
    scrubDefragBtree(&s, i, 0, 1);
  }
  s.rcErr = sqlite3_finalize(pStmt);
  if( s.rcErr ) goto scrub_abort;

  zSql = sqlite3_mprintf("%z\nCOMMIT;\nPRAGMA writable_schema=off;", zSql);
  if(zSql == 0){
     s.rcErr = SQLITE_NOMEM;
  }else{
    sqlite3_close(s.dbDest);
    /* reopen the destination database and update the root pages */
    s.rcErr = sqlite3_open_v2(s.zDestFile, &s.dbDest, 
                     SQLITE_OPEN_READWRITE |
                     SQLITE_OPEN_URI | SQLITE_OPEN_PRIVATECACHE, 0);
    if( s.rcErr ){ 
      scrubDefragErr(&s, "Error occurred while reopen destination database:%s",
                         sqlite3_errmsg(s.dbDest));
    }else if( s.rcErr = sqlite3_exec(s.dbDest, zSql, 0, 0, &errmsg) ){
        scrubDefragErr(&s, "Error occurred while update root page: %z",errmsg);
    }
  }
  sqlite3_free(zSql);

scrub_abort:    
  /* Close the destination database without closing the transaction. If we
  ** commit, page zero will be overwritten. */
  sqlite3_close(s.dbDest);
  /* But do close out the read-transaction on the source database */
  sqlite3_exec(s.dbSrc, "COMMIT;", 0, 0, 0);
  sqlite3_close(s.dbSrc);
  sqlite3_free(s.page1);
  if( pzErr ){
    *pzErr = s.zErr;
  }else{
    sqlite3_free(s.zErr);
  }
  return s.rcErr;
}   

#ifdef DEFRAG_STANDALONE
/* Error and warning log */
static void errorLogCallback(void *pNotUsed, int iErr, const char *zMsg){
  const char *zType;
  switch( iErr&0xff ){
    case SQLITE_WARNING: zType = "WARNING";  break;
    case SQLITE_NOTICE:  zType = "NOTICE";   break;
    default:             zType = "ERROR";    break;
  }
  fprintf(stderr, "%s: %s\n", zType, zMsg);
}

/* The main() routine when this utility is run as a stand-alone program */
int main(int argc, char **argv){
  char *zErr = 0;
  int rc;
  if( argc!=3 ){
    fprintf(stderr,"Usage: %s SOURCE DESTINATION\n", argv[0]);
    exit(1);
  }
  sqlite3_config(SQLITE_CONFIG_LOG, errorLogCallback, 0);
  rc = sqlite3_scrub_and_defrag(argv[1], argv[2], &zErr);
  if( rc==SQLITE_NOMEM ){
    fprintf(stderr, "%s: out of memory\n", argv[0]);
    exit(1);
  }
  if( zErr ){
    fprintf(stderr, "%s: %s\n", argv[0], zErr);
    sqlite3_free(zErr);
    exit(1);
  }
  return 0;
}
#endif
