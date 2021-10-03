package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;

import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;

public class LockManager {
    class PageLock {
        StampedLock sl;
        long stamp;

        public void lock(Permissions perm) {
            if (perm == Permissions.READ_ONLY) {
                stamp = sl.readLock();
            } else if (perm == Permissions.READ_WRITE) {
                stamp = sl.writeLock();
            }
        }

        public void unlock() {
            sl.unlock(stamp);
        }
    }

    private ConcurrentHashMap<PageId, PageLock> _pageLocksTable;
    private ConcurrentHashMap<TransactionId, HashSet<PageId>> _txnPagesTable;

    public LockManager() {
        _pageLocksTable = new ConcurrentHashMap<>();
        _txnPagesTable = new ConcurrentHashMap<>();
    }

    public void lockPage(TransactionId tid, PageId pid, Permissions perm) {
        PageLock pageLock = _pageLocksTable.get(pid);
        if (null == pageLock) {
            pageLock = new PageLock();
            pageLock.lock(perm);
            _pageLocksTable.putIfAbsent(pid, pageLock);
        } else {
            pageLock.lock(perm);
        }

        HashSet txnpages = _txnPagesTable.get(tid);
        if (null == txnpages) {
            txnpages = new HashSet();
            txnpages.add(pid);
            _txnPagesTable.put(tid, txnpages);
        } else {
            txnpages.add(pid);
        }
    }

    public void unlockPage(TransactionId tid, PageId pid) {
        PageLock pageLock = _pageLocksTable.get(pid);
        if (null == pageLock) {
            // there is no page lock for this page, do nothing
        } else {
            pageLock.unlock();
        }

        HashSet txnpages = _txnPagesTable.get(tid);
        if (null == txnpages) {
            // there is no txnPages for this txn, do nothing
        } else {
            txnpages.remove(pid);
        }
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        HashSet txnpages = _txnPagesTable.get(tid);
        if (null == txnpages) {
            return false;
        } else {
            return txnpages.contains(pid);
        }
    }

    public void unlockAllPages(TransactionId tid) {
        HashSet txnpages = _txnPagesTable.get(tid);
        if (null == txnpages) {
            // this txn does not hold any locks
        } else {
            Iterator<PageId> pageIdIterator = txnpages.iterator();
            while (pageIdIterator.hasNext()) {
                PageId pid = pageIdIterator.next();
                PageLock pageLock = _pageLocksTable.get(pid);
                if (null == pageLock) {
                    // this should not happen
                    assert(false);
                } else {
                    pageLock.unlock();
                }
            }
        }
    };

}
