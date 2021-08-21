package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static java.nio.file.StandardOpenOption.READ;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    class HeapFileIterator implements DbFileIterator {

        private TransactionId xid;
        private HeapFile hf;
        private int numPage;
        private int pageIdx;
        private HeapPage curPage;
        private Iterator<Tuple> tupleIterator;
        private boolean opened;

        public HeapFileIterator(HeapFile hf, TransactionId xid) {
            this.xid = xid;
            this.hf = hf;
            this.numPage = hf.numPages();
            this.pageIdx = 0;
            this.opened = false;

        }

        /**
         * Opens the iterator
         *
         * @throws DbException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            if (this.opened) {
                throw new DbException("already opened");
            }

            HeapPageId pid = new HeapPageId(this.hf.getId(), this.pageIdx);
            curPage = (HeapPage)Database.getBufferPool().getPage(this.xid, pid, Permissions.READ_ONLY);
            tupleIterator = curPage.iterator();
            this.opened = true;
//            Debug.log(-1, "heap file numPage: %d", this.numPage);
        }

        /**
         * @return true if there are more tuples available, false if no more tuples or iterator isn't open.
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!this.opened) {
                return false;
            }

            if (tupleIterator == null) {
                return false;
            }

            if (tupleIterator.hasNext()) {
                return true;
            }

            while (pageIdx < numPage - 1) {
                pageIdx++;
                HeapPageId pid = new HeapPageId(this.hf.getId(), pageIdx);
                curPage = (HeapPage)Database.getBufferPool().getPage(xid, pid, Permissions.READ_ONLY);
                tupleIterator = curPage.iterator();
                if (tupleIterator.hasNext()) {
                    return true;
                } else {
                    // continue next page
                }
            }

            // pageIdx == numPage - 1
            // the last page already has been scanned
            return false;
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
//            Debug.log(-1, "file iterator pageIdx: %d", pageIdx);
            if (!this.opened) {
                throw new NoSuchElementException("iterator not opened");
            }

            if (tupleIterator == null) {
                throw new NoSuchElementException("curPage null");
            }

            return tupleIterator.next();
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException When rewind is unsupported.
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.pageIdx = 0;
            HeapPageId pid = new HeapPageId(this.hf.getId(), this.pageIdx);
            curPage = (HeapPage)Database.getBufferPool().getPage(this.xid, pid, Permissions.READ_ONLY);
            tupleIterator = curPage.iterator();
        }

        /**
         * Closes the iterator.
         */
        @Override
        public void close() {
            curPage = null;
            tupleIterator = null;
            this.pageIdx = 0;
            this.opened = false;
        }
    }

    private File file;

    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            SeekableByteChannel sbc = Files.newByteChannel(this.file.toPath());
            sbc.position(pid.getPageNumber() * BufferPool.getPageSize());
            ByteBuffer buf = ByteBuffer.allocate(BufferPool.getPageSize());
            int nbytes = sbc.read(buf);
            assert(nbytes == BufferPool.getPageSize());
            HeapPageId heapPageId = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(heapPageId, buf.array());
        } catch (IOException e) {
            Debug.log(-1, "IOException", e);
        }
        return null;
    }

    // see DbFile.java for javadocs
    @Override
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        SeekableByteChannel sbc = Files.newByteChannel(this.file.toPath());
        sbc.position(page.getId().getPageNumber() * BufferPool.getPageSize());
        ByteBuffer buffer = ByteBuffer.wrap(page.getPageData());
        sbc.write(buffer);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        try {
            long size = Files.size(this.file.toPath());
            return (int)size / BufferPool.getPageSize();
        } catch (IOException e) {
            Debug.log(-1, "IOException", e);
        }
        return 0;
    }

    // see DbFile.java for javadocs
    @Override
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> retPages = new ArrayList<Page>();
        // some code goes here
        int curNumPages = numPages();
        for (int i = 0 ; i < curNumPages; i++) {
            PageId pageId = new HeapPageId(this.getId(), i);
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                retPages.add(page);
                return retPages;
            }
        }

        HeapPageId pageId = new HeapPageId(this.getId(), curNumPages);
        HeapPage page = new HeapPage(pageId, HeapPage.createEmptyPageData());
        page.insertTuple(t);
        // file size will be extended here
        Files.write(this.file.toPath(), page.getPageData(), StandardOpenOption.APPEND);
        page = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        retPages.add(page);
        return retPages;
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> retPages = new ArrayList<Page>();
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        retPages.add(page);
        return retPages;
    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

}

