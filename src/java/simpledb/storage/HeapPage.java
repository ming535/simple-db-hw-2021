package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    class PageIterator implements Iterator<Tuple> {
        final HeapPage page;
        int curIdx;

        public PageIterator(HeapPage page) {
            this.page = page;
            this.curIdx = 0;
//            Debug.log(-1, "pageNo: %d, validSlots: %d", this.idx, this.validSlots);
        }

        @Override
        public boolean hasNext() {
            if (curIdx == page.numSlots) {
                return false;
            }

            if (page.tuples[curIdx] != null) {
                return true;
            }

            while (curIdx < page.numSlots - 1) {
                curIdx++;
                if (page.tuples[curIdx] != null) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Tuple next() {
            Tuple t = page.tuples[curIdx];
            curIdx++;
            return t;
        }
    }

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header;
    final Tuple[] tuples;
    final int numSlots;


    private boolean isDirty;
    private TransactionId dirtyXid;
    byte[] oldData;
    private final Byte oldDataLock= (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        this.isDirty = false;
        this.dirtyXid = null;
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        this.header = new byte[getHeaderSize()];
        for (int i=0; i< this.header.length; i++) {
            this.header[i] = dis.readByte();
        }
        
        this.tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i< this.tuples.length; i++) {
                this.tuples[i] = readNextTuple(dis, i);
            }
        } catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
//        Debug.log(-1,"pageNo: %d, numSlots %d", id.getPageNumber(), this.numSlots);
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {
        // some code goes here
        int tupleBits = this.td.getSize() * 8 + 1;
        int pageSizeBits = BufferPool.getPageSize() * 8;
        return pageSizeBits / tupleBits;
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {        
        // some code goes here
        int nheaderbytes = getNumTuples() / 8;
        if (nheaderbytes * 8 < getNumTuples()) {
            nheaderbytes++;
        }
        return nheaderbytes;
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    @Override
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    @Override
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    @Override
    public HeapPageId getId() {
        // some code goes here
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    @Override
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if (!t.getTupleDesc().equals(td)) {
            throw new DbException("tuple description not match");
        }

        if (!t.getRecordId().getPageId().equals(pid)) {
            throw new DbException(String.format("tuple not belongs to this page, tuple pid %d, page id: %d", t.getRecordId().getPageId().getPageNumber(), pid.getPageNumber()));
        }

        int slot = t.getRecordId().getTupleNumber();
        if (!isSlotUsed(slot)) {
            throw new DbException(String.format("slot not used: pageId: %d, tupleNumber: %d",
                                                t.getRecordId().getPageId().getPageNumber(),
                                                t.getRecordId().getTupleNumber()));
        }

        tuples[slot] = null;
        markSlotUsed(slot, false);
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if (!t.getTupleDesc().equals(td)) {
            throw new DbException("tuple description not match");
        }

        for (int i = 0; i < numSlots; i++) {
            if (!isSlotUsed(i)) {
                t.setRecordId(new RecordId(pid, i));
                tuples[i] = t;
                markSlotUsed(i, true);
                return;
            }
        }

        throw new DbException("no empty slot");
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    @Override
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	// not necessary for lab1
        this.isDirty = dirty;
        if (dirty) {
            this.dirtyXid = tid;
        } else {
            this.dirtyXid = null;
        }
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    @Override
    public TransactionId isDirty() {
        // some code goes here
	// Not necessary for lab1
        if (this.isDirty) {
            return this.dirtyXid;
        } else {
            return null;
        }
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int emptySlots = 0;
        for (int i = 0; i < numSlots; i++) {
            if (!isSlotUsed(i)) {
                emptySlots++;
            }
        }
        return emptySlots;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        byte headerByte = header[i/8];
        headerByte &= (1 << (i % 8));
        return headerByte != 0;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
        if (value) {
            header[i/8] = (byte) (header[i/8] | (1 << (i % 8)));
        } else {
            header[i/8] = (byte) (header[i/8] & (~(1 << (i % 8))));
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        return new PageIterator(this);
    }

}

