package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Debug;
import simpledb.common.LockMode;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockManager {
    class PageLock {
        final private PageId _pid;
        final private ReentrantLock _latch;
        final private Condition _noSharedCond;
        final private Condition _noExclusiveCond;
        LockMode _mode;
        HashSet<TransactionId> _writers;
        HashSet<TransactionId> _readers;

        public PageLock(PageId pid) {
            _pid = pid;
            _latch = new ReentrantLock();
            _noSharedCond = _latch.newCondition();
            _noExclusiveCond = _latch.newCondition();
            _mode = LockMode.None;
            _writers = new HashSet();
            _readers = new HashSet();
        }

        public void lock(TransactionId tid, Permissions perm) {
            _latch.lock();
            try {
                while (true) {
                    if (_mode == LockMode.SHARED) {
                        assert(_readers.size() > 0);
                        assert(_writers.size() == 0);

                        if (perm == Permissions.READ_ONLY) {
                            _readers.add(tid);
                            break;
                        } else if (perm == Permissions.READ_WRITE) {
                            if (_readers.size() == 1 && _readers.contains(tid)) {
                                // upgrade
                                _mode = LockMode.EXCLUSIVE;
                                _readers.remove(tid);
                                _writers.add(tid);
                                break;
                            } else {
                                _noSharedCond.await();
                                continue;
                            }
                        }
                    } else if (_mode == LockMode.EXCLUSIVE) {
                        assert(_writers.size() == 1) : "_writers.size() == " + _writers.size();
                        assert(_readers.size() == 0);

                        if (_writers.contains(tid)) {
                            // myself
                            break;
                        } else {
                            _noExclusiveCond.await();
                            continue;
                        }
                    } else if (_mode == LockMode.None) {
                        assert(_writers.size() == 0);
                        assert(_readers.size() == 0);
                        if (perm == Permissions.READ_ONLY) {
                            _mode = LockMode.SHARED;
                            _readers.add(tid);
                            break;
                        } else if (perm == Permissions.READ_WRITE) {
                            _mode = LockMode.EXCLUSIVE;
                            _writers.add(tid);
                            break;
                        }
                    } else {
                        Debug.log(-1, "impossible");
                        assert(false);
                    }
                }
            } catch (Exception e) {
                Debug.log(-1, "exception");
                e.printStackTrace();
            } finally {
//                Debug.log(-1, "tid: %d, page: %d, locked", tid.getId(), _pid.getPageNumber());
                _latch.unlock();
            }
        }

        public void unlock(TransactionId tid) {
            _latch.lock();
            try {
                if (_mode == LockMode.SHARED) {
                    assert(_writers.size() == 0);
                    _readers.remove(tid);
                    if (_readers.size() == 0) {
                        _mode = LockMode.None;
                        _noSharedCond.signalAll();
                    }
                } else if (_mode == LockMode.EXCLUSIVE) {
                    assert(_writers.size() == 1);
                    _writers.remove(tid);
                    assert(_writers.size() == 0);
                    _mode = LockMode.None;
                    _noExclusiveCond.signalAll();
                }
            } finally {
//                Debug.log(-1, "tid: %d, page: %d, unlocked", tid.getId(), _pid.getPageNumber());
                _latch.unlock();
            }
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
            pageLock = new PageLock(pid);
            pageLock.lock(tid, perm);
            PageLock prevLock = _pageLocksTable.putIfAbsent(pid, pageLock);
            if (prevLock != null) {
                prevLock.lock(tid, perm);
            }
        } else {
            pageLock.lock(tid, perm);
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
            assert(false);
        } else {
            pageLock.unlock(tid);
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

    public boolean isLocked(PageId pid) {
        PageLock latch = _pageLocksTable.get(pid);
        if (latch == null) {
            return false;
        } else {
            if (latch._mode == LockMode.None) {
                return false;
            } else {
//                Debug.log(-1, "mode: %s, readers.size(): %d, reader tid: %s",
//                        latch._mode,
//                        latch._readers.size(),
//                        latch._readers.iterator().next().getId());
                return true;
            }
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
                    pageLock.unlock(tid);
                }
            }
        }
    }

    public Set<PageId> txnPages(TransactionId tid) {
        return _txnPagesTable.get(tid);
    }

}
