package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId xid;
    private OpIterator child;
    private TupleDesc resultTd;
    private boolean deleted;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.xid = t;
        this.child = child;
        this.resultTd = new TupleDesc(new Type[]{Type.INT_TYPE});
        this.deleted = false;
    }

    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.resultTd;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.open();
        super.open();
    }

    @Override
    public void close() {
        // some code goes here
        this.child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.deleted) {
            return null;
        }

        int numTuple = 0;

        while (this.child.hasNext()) {
            Tuple t = this.child.next();
            try {
                Database.getBufferPool().deleteTuple(this.xid, t);
                numTuple++;
            } catch (IOException e) {
                throw new DbException("io exception");
            }
        }
        TupleDesc td = this.getTupleDesc();
        Tuple result = new Tuple(td);
        result.setField(0, new IntField(numTuple));
        this.deleted = true;
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children.length == 1) {
            this.child = children[0];
        }
    }

}
