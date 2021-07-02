package simpledb.execution;

import com.sun.scenario.effect.Merge;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    class MergedTupleContainer {
        Tuple t;
        int mergedNum;

        public MergedTupleContainer(Tuple t, int mergedNum) {
            this.t = t;
            this.mergedNum = mergedNum;
        }

    }

    class IntegerAggregatorIterator implements OpIterator {
        private IntegerAggregator agg;
        private boolean opened;
        private Iterator it;

        IntegerAggregatorIterator(IntegerAggregator aggregator) {
            this.agg = aggregator;
        }

        /**
         * Opens the iterator. This must be called before any of the other methods.
         *
         * @throws DbException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.opened = true;
            this.it = this.agg.tupleByfield.entrySet().iterator();
        }

        /**
         * Returns true if the iterator has more tuples.
         *
         * @return true f the iterator has more tuples.
         * @throws IllegalStateException If the iterator has not been opened
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!this.opened) {
                throw new IllegalStateException("not opened");
            }
            return this.it.hasNext();
        }

        /**
         * Returns the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return the next tuple in the iteration.
         * @throws NoSuchElementException if there are no more tuples.
         * @throws IllegalStateException  If the iterator has not been opened
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!opened) {
                throw new IllegalStateException("not opened");
            }
            HashMap.Entry pair = (HashMap.Entry)this.it.next();
            MergedTupleContainer c = (MergedTupleContainer) pair.getValue();
            IntField newF;
            Tuple newtup = new Tuple(getTupleDesc());
            if (this.agg.what == Op.AVG) {
                IntField f;
                if (this.agg.gbfield == Aggregator.NO_GROUPING) {
                    f = (IntField) c.t.getField(0);
                    newF = new IntField(f.getValue() / c.mergedNum);
                    newtup.setField(0, newF);
                } else {
                    f = (IntField) c.t.getField(1);
                    newF = new IntField(f.getValue() / c.mergedNum);
                    newtup.setField(0, c.t.getField(0));
                    newtup.setField(1, newF);
                }
                return newtup;
            }
            return c.t;
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException           when rewind is unsupported.
         * @throws IllegalStateException If the iterator has not been opened
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (!opened) {
                throw new IllegalStateException("not opened");
            }
            this.it = this.agg.tupleByfield.entrySet().iterator();
        }

        /**
         * Returns the TupleDesc associated with this OpIterator.
         *
         * @return the TupleDesc associated with this OpIterator.
         */
        @Override
        public TupleDesc getTupleDesc() {
            return this.agg.getTupleDesc();
        }

        /**
         * Closes the iterator. When the iterator is closed, calling next(),
         * hasNext(), or rewind() should fail by throwing IllegalStateException.
         */
        @Override
        public void close() {
            this.opened = false;
        }
    }

    private int gbfield;
    private Type gbFieldType;
    private int afield;
    private Op what;
    private HashMap<Field, MergedTupleContainer> tupleByfield;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbFieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.tupleByfield = new HashMap<>();
    }

    public TupleDesc getTupleDesc() {
        if (this.gbfield == Aggregator.NO_GROUPING) {
            return new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            return new TupleDesc(new Type[]{this.gbFieldType, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        TupleDesc mergedTd = getTupleDesc();
        Tuple mergedTup;
        MergedTupleContainer mergedTupContainer;
        Field newField = tup.getField(this.afield);

        if (this.gbfield == Aggregator.NO_GROUPING) {
            Field gbfield = new IntField(0);
            mergedTupContainer = this.tupleByfield.get(gbfield);

            if (null == mergedTupContainer) {
                mergedTup = new Tuple(mergedTd);
                mergedTup.setField(0, newField);
                mergedTupContainer = new MergedTupleContainer(mergedTup, 1);
            } else {
                mergeTup(mergedTupContainer, 0, newField);
            }
            this.tupleByfield.put(gbfield, mergedTupContainer);
        } else {
            Field gbfield = tup.getField(this.gbfield);
            mergedTupContainer = this.tupleByfield.get(gbfield);

            if (null == mergedTupContainer) {
                mergedTup = new Tuple(mergedTd);
                mergedTup.setField(0, gbfield);
                mergedTup.setField(1, newField);
                mergedTupContainer = new MergedTupleContainer(mergedTup, 1);
            } else {
                mergeTup(mergedTupContainer, 1, newField);
            }
            this.tupleByfield.put(gbfield, mergedTupContainer);
        }
    }

    private void mergeTup(MergedTupleContainer container, int aggfieldIdx, Field newField) {
        Field oldAgField = container.t.getField(aggfieldIdx);
        switch (this.what) {
            case MAX:
                if (newField.compare(Predicate.Op.GREATER_THAN, oldAgField)) {
                    container.t.setField(aggfieldIdx, newField);
                    container.mergedNum++;
                }
                break;
            case MIN:
                if (newField.compare(Predicate.Op.LESS_THAN, oldAgField)) {
                    container.t.setField(aggfieldIdx, newField);
                    container.mergedNum++;
                }
                break;
            case SUM:
            case AVG:
                int oldV = ((IntField)oldAgField).getValue();
                int newV = ((IntField)newField).getValue();
                container.t.setField(aggfieldIdx, new IntField(oldV + newV));
                container.mergedNum++;
                break;
            case COUNT:
                container.mergedNum++;
                break;
            default:
                break;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    @Override
    public OpIterator iterator() {
        // some code goes here
        return new IntegerAggregatorIterator(this);
    }

}
