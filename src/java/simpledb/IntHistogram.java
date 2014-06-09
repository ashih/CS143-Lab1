package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {


    private int m_numbuckets;
    private int m_min;
    private int m_max;
    private int[] m_buckets;
    private int m_bucketsize;
    private int m_range;
    private int OVER = -1;
    private int UNDER = -2;
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
        m_numbuckets = buckets;
        m_min = min;
        m_max = max;
        m_range = max-min;
        if (m_numbuckets > m_range)
            m_bucketsize = 1;
        else
            m_bucketsize = (int) Math.ceil( (double) m_range / (double) buckets);
        m_buckets = new int[buckets];
        for (int i = 0; i < m_numbuckets; i++)
            m_buckets[i] = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        assert (v >= m_min);
        assert (v <= m_max);
        int b = getIndex(v);
        /*if (b >= m_numbuckets) {
            System.out.println(b);
            System.out.println(v);
            //System.out.println(m_min);
            //System.out.println(m_max);
            //System.out.println(m_bucketsize);
        }*/
        m_buckets[b]++;
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
    private int getIndex (int val)
    {
        if (val < m_min) return UNDER;
        if (val > m_max) return OVER;
        if ((val == m_max) && (val % m_bucketsize == 0)) val--;        
        return (val-m_min)/m_bucketsize;
    }

    private int allValues()
    {
        int sumOfValues = 0; //totalValues
        for (int i = 0; i < m_numbuckets; i++)
            sumOfValues += m_buckets[i];
        return sumOfValues;
    }

    private int gt (int b)
    {
        if (b == OVER) return 0;
        if (b == UNDER) return allValues();
        int sum = 0;
        for (int i = b + 1; i < m_numbuckets; i++)
            sum += m_buckets[i];
        return sum;
    }

    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        int b = getIndex(v);
        double numberOfValues = 0;
        if (b != UNDER && b != OVER) 
            numberOfValues = m_buckets[b]; 
        double tValues = allValues();
        switch (op) {
            case EQUALS:
                return numberOfValues/tValues;
            case GREATER_THAN:
                return gt(b)/tValues;
            case GREATER_THAN_OR_EQ:
                double temp = gt(b) + numberOfValues;
                return temp/tValues;
            case LESS_THAN_OR_EQ:
                return (tValues - gt(b)) / tValues;
            case LESS_THAN:
                return (tValues - gt(b) - numberOfValues)/tValues;
            case NOT_EQUALS:
                return (tValues - numberOfValues) / tValues;
            default:
                break;
        }
        return 1;
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
    public String toString() {

        // some code goes here
        return null;
    }
}
