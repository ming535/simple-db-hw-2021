package simpledb.optimizer;

import simpledb.common.Debug;
import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int numBuckets;
    private int[] bucketHeights;
    private int numTuples;
    final private int min;
    final private int max;
    final private int bucketWidth;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.numBuckets = buckets;
        this.bucketHeights = new int[buckets];
        this.min = min;
        this.max = max;
        this.bucketWidth = (int)Math.ceil(((double)max - (double)min + 1) / (double)buckets);
        this.numTuples = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        if (v >= this.min && v <= this.max) {
            this.bucketHeights[bucketIdx(v)] += 1;
            this.numTuples += 1;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        switch (op) {
            case EQUALS:
                return equalSelectivity(v);
            case GREATER_THAN:
                return greaterThanSelectivity(v);
            case GREATER_THAN_OR_EQ:
                return equalSelectivity(v) + greaterThanSelectivity(v);
            case LESS_THAN:
                return 1 - (equalSelectivity(v) + greaterThanSelectivity(v));
            case LESS_THAN_OR_EQ:
                return 1 - greaterThanSelectivity(v);
            case NOT_EQUALS:
                return 1 - equalSelectivity(v);
            default:
                return 1;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        // some code goes here
        return null;
    }

    private int bucketIdx(int v) {
        // elementsPerBucket * bucketIdx + min = value
        int idx = (int)(((long)v - (long)this.min) / this.bucketWidth);
//        Debug.log(-1, "bucketIdx: %d, v: %d, min: %d, bucketWidth: %d", idx, v, this.min, this.bucketWidth);
        assert(idx < this.numBuckets);
        assert(idx >= 0);
        return idx;
    }

    private double equalSelectivity(int v) {
        if (v < this.min) {
            return 0;
        }

        if (v > this.max) {
            return 0;
        }

        int idx = bucketIdx(v);
        double selectivity = ((double)this.bucketHeights[idx] / (double)this.bucketWidth) / (double)this.numTuples;
        Debug.log(-1, "idx: %d, height: %d, buketWidth: %d, numTuples: %d, selectivity: %f", idx, this.bucketHeights[idx], this.bucketWidth, this.numTuples, selectivity);
        return selectivity;
    }

    private double greaterThanSelectivity(int v) {
        if (v < this.min) {
            return 1.0;
        }

        if (v > this.max) {
            return 0;
        }

        int idx = bucketIdx(v);
        int bktMax = this.bucketWidth * (idx + 1) + this.min - 1;
        double selectivity = (bktMax - v) / this.bucketWidth;
        Debug.log(-1, "GT_initial v: %d, idx: %d, bkMax: %d, min: %d, selectivity: %f", v, idx, bktMax, this.min, selectivity);
        for (int i = idx + 1; i < this.numBuckets; i++) {
            selectivity += (double)this.bucketHeights[i] / this.numTuples;
        }
        Debug.log(-1, "GT_Final selectivity: %f", selectivity);
        return selectivity;
    }
}
