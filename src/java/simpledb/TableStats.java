package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    private int m_tableid;
    private int m_cost;
    private HeapFile m_tablefile;
    private int m_ntuples;
    private TupleDesc m_td;

    private HashMap<String, IntHistogram> intmap = new HashMap<String, IntHistogram>();
    private HashMap<String, StringHistogram> strmap = new HashMap<String, StringHistogram>();

    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here

        m_tableid = tableid;
        m_cost = ioCostPerPage;
        m_tablefile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);


        //Create Iterator
        Transaction t = new Transaction();
        DbFileIterator iter = m_tablefile.iterator(t.getId());
        Tuple first = null;
        HashMap<String, Integer> minmap = new HashMap<String, Integer>();
        HashMap<String, Integer> maxmap = new HashMap<String, Integer>();
        try {
            iter.open();
            if (iter.hasNext())
            {
                first = iter.next();
                m_ntuples++;
            }
            m_td = first.getTupleDesc();
            int nfields = m_td.numFields();

            //initialize the maps
            for (int i = 0; i < nfields; i++)
            {
                if (m_td.getFieldType(i) == Type.INT_TYPE)
                {
                    minmap.put(m_td.getFieldName(i), ((IntField)first.getField(i)).getValue());
                    maxmap.put(m_td.getFieldName(i), ((IntField)first.getField(i)).getValue());
                }
            }

            //update maps
            while(iter.hasNext())
            {
                Tuple tup = iter.next();
                m_ntuples++;
                for(int i = 0; i < nfields; i++)
                {
                    if(m_td.getFieldType(i) == Type.INT_TYPE)
                    {
                        int val = ((IntField)tup.getField(i)).getValue();
                        if (minmap.get(m_td.getFieldName(i)) > val)
                            minmap.put(m_td.getFieldName(i), val);
                        else if (maxmap.get(m_td.getFieldName(i)) < val)
                            maxmap.put(m_td.getFieldName(i), val);
                    }
                }
            }


            //creating histograms

            for(int i = 0; i < nfields; i++)
            {
                if(m_td.getFieldType(i) == Type.INT_TYPE)
                {
                    String fieldname = m_td.getFieldName(i);
                    intmap.put(fieldname, new IntHistogram(NUM_HIST_BINS, minmap.get(fieldname), maxmap.get(fieldname)));
                } else if (m_td.getFieldType(i) == Type.STRING_TYPE)  {
                    String fieldname = m_td.getFieldName(i);
                    strmap.put(fieldname, new StringHistogram(NUM_HIST_BINS));  
                }
            }

            //rewind
            iter.rewind();

            //try again
            while(iter.hasNext())
            {
                Tuple tup = iter.next();
                for(int i = 0; i < nfields; i++)
                {
                    if(m_td.getFieldType(i) == Type.INT_TYPE)
                    {
                        intmap.get(m_td.getFieldName(i)).addValue(((IntField)(tup.getField(i))).getValue());
                    }
                    else if (m_td.getFieldType(i) == Type.STRING_TYPE)
                    {
                        strmap.get(m_td.getFieldName(i)).addValue(((StringField)(tup.getField(i))).getValue());
                    }
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } 
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return m_tablefile.numPages() * m_cost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (m_ntuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (m_td.getFieldType(field) == Type.INT_TYPE)
            return intmap.get(m_td.getFieldName(field)).estimateSelectivity(op, ((IntField) constant).getValue());
        else if (m_td.getFieldType(field) == Type.STRING_TYPE)
            return strmap.get(m_td.getFieldName(field)).estimateSelectivity(op, ((StringField) constant).getValue());
        else
            return 0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return m_ntuples;
    }

}
