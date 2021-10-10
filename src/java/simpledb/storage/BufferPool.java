package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int maxNumPages;
    private ConcurrentHashMap<PageId, Page> pageTable;
    private LockManager _lockManager;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.maxNumPages = numPages;
        pageTable = new ConcurrentHashMap<>();
        _lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        _lockManager.lockPage(tid, pid, perm);

        Page page;
        int tableid = pid.getTableId();
        Catalog catalog = Database.getCatalog();
        DbFile dbFile = catalog.getDatabaseFile(tableid);
        page = pageTable.get(pid);
        if (page != null) {
            // do nothing
        } else {
            // unlock the current page
//            Debug.log(-1, "before evict page tid: %d, page: %d unlock, table size: %d, maxNumPages: %d",
//                                        tid.getId(), pid.getPageNumber(), pageTable.size(), maxNumPages);
            _lockManager.unlockPage(tid, pid);
            if (pageTable.size() >= maxNumPages) {
                evictPage(tid);
            }
            assert(pageTable.size() <= maxNumPages);
            // lock it again
            _lockManager.lockPage(tid, pid, perm);
            page = dbFile.readPage(pid);
            pageTable.put(pid, page);
        }
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        _lockManager.unlockPage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return _lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            try {
                flushPages(tid);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            // rollback
            Set changedPageIds = _lockManager.txnPages(tid);
            if (changedPageIds != null) {
                Iterator<PageId> iter = changedPageIds.iterator();
                while (iter.hasNext()) {
                    PageId pid = iter.next();
                    Page page = pageTable.get(pid);
                    if (page != null && page.isDirty() == tid) {
                        discardPage(pid);
                    } else if (page != null) {
                        assert(page.isDirty() == null);
                    }
                }
            }
        }
        _lockManager.unlockAllPages(tid);
        _lockManager.removeDeps(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbfile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = dbfile.insertTuple(tid, t);
        for (int i = 0; i < pages.size(); i++) {
            Page page = pages.get(i);
            page.markDirty(true, tid);
            pageTable.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbfile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> dirtyPages = dbfile.deleteTuple(tid, t);
        for (int i = 0; i < dirtyPages.size(); i++) {
            Page page = dirtyPages.get(i);
            page.markDirty(true, tid);
            pageTable.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Iterator it = this.pageTable.entrySet().iterator();
        while(it.hasNext()) {
            HashMap.Entry pair = (HashMap.Entry)it.next();
            PageId pageid = (PageId)pair.getKey();
            flushPage(pageid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageTable.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pageTable.get(pid);
        if (page != null && page.isDirty() != null) {
            DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            dbfile.writePage(page);
            page.markDirty(false, null);
        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Set changedPids = _lockManager.txnPages(tid);;
        if (changedPids != null) {
            Iterator<PageId> iter = changedPids.iterator();
            while (iter.hasNext()) {
                PageId pid = iter.next();
                flushPage(pid);
            }
        }

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage(TransactionId tid) throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // evict the first page scanned
        Iterator it = this.pageTable.entrySet().iterator();
        PageId candidate = null;
        while (it.hasNext()) {
            HashMap.Entry pair = (HashMap.Entry)it.next();
            PageId pageid = (PageId)pair.getKey();
            Page page = (Page)pair.getValue();
            TransactionId xid = page.isDirty();
            if (xid != null) {
                // do not evict dirty page
//                Debug.log(-1, "evict page %d dirty", pageid.getPageNumber());
            } else {
                if (!_lockManager.isLocked(pageid)) {
                    candidate = pageid;
                    Debug.log(-1, "evict page %d candidate found", pageid.getPageNumber());
                    break;
                } else if (_lockManager.holdsLock(tid, pageid)) {
                    // I hold the lock myself, consider it as a candidate
                    candidate = pageid;
                    break;
                } else {
                    Debug.log(-1, "evict page %d locked", pageid.getPageNumber());
                }
            }
        }

        if (candidate == null) {
            throw new DbException("no clean page");
        }

        if (!_lockManager.holdsLock(tid, candidate)) {
            // I do not hold lock on this page, just lock it and remove it from page table
            _lockManager.lockPage(tid, candidate, Permissions.READ_WRITE);
            pageTable.remove(candidate);
            _lockManager.unlockPage(tid, candidate);
        } else {
            pageTable.remove(candidate);
        }
    }
}
