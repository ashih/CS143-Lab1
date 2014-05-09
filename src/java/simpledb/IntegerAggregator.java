package simpledb;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

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

    int m_gbfield;
    Type m_gbfieldtype;
    int m_afield;
    Op m_what;
    HashMap<Object,Integer> m_aggregate;
    HashMap<Object,Integer> m_count;
    String m_keyName;


    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        m_gbfield = gbfield;
        m_gbfieldtype = gbfieldtype;
        m_afield = afield;
        m_what = what;
        m_aggregate = new HashMap<Object,Integer>();
        m_count = new HashMap<Object,Integer>();
        m_keyName = null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Object key;
        if (m_gbfield == NO_GROUPING) {
            key = (Integer) NO_GROUPING;
        }
        else {
            if (m_keyName == null)
                m_keyName = tup.getTupleDesc().getFieldName(m_gbfield);
            
            if (tup.getField(m_gbfield).getType() == Type.INT_TYPE)
                key = ((IntField) tup.getField(m_gbfield)).getValue();
            else 
                key = ((StringField) tup.getField(m_gbfield)).getValue();
        }
        int value = ((IntField) tup.getField(m_afield)).getValue();
        int aggregate;
        int count;
        
        if (!m_aggregate.containsKey(key)){
            switch (m_what) {
                case MIN: {
                    aggregate = Integer.MAX_VALUE;
                    break;
                }
                case MAX: {
                    aggregate = Integer.MIN_VALUE;
                    break;
                }
                case SUM:
                case AVG:                    
                case COUNT: 
                    aggregate = 0;
            }

            m_aggregate.put(key,value);
            m_count.put(key,1);
        }
        else {
            aggregate = m_aggregate.get(key);
            count = m_count.get(key);
            count++;
            
            switch (m_what) {
                case MIN: {                    
                    if (value < aggregate)
                        aggregate = value;
                    break;
                }
                case MAX: {                    
                    if (value > aggregate)
                        aggregate = value;
                    break;
                }
                case SUM:
                case AVG: {                
                    aggregate += value;
                    break;
                }                    
                case COUNT:
                    aggregate++;
            }

            m_aggregate.put(key,aggregate);
            m_count.put(key,count);
        }        
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        TupleDesc tupleDesc;
        if (m_gbfield == NO_GROUPING) {
            Type[] types = new Type[1];
            types[0] = Type.INT_TYPE;
            tupleDesc = new TupleDesc(types);            
        }
        else {
            Type[] types = new Type[2];
            types[0] = m_gbfieldtype;
            types[1] = Type.INT_TYPE;
            tupleDesc = new TupleDesc(types);

            String[] names = new String[2];
            names[0] = m_keyName;
            names[1] = m_what.toString();
        }
        for(Map.Entry<Object,Integer> entry : m_aggregate.entrySet()) {
            Tuple tup = new Tuple(tupleDesc);
            Object key = entry.getKey();
            int value = entry.getValue();
            if (m_what == Aggregator.Op.AVG) {
                value = value / m_count.get(key);
            }
            else if (m_what == Aggregator.Op.COUNT) {
                value = m_count.get(key);
            }

            Field aggregatefield = new IntField(value);            
            if (m_gbfield == NO_GROUPING) {
                tup.setField(0, aggregatefield);
            }
            else {
                Field gbfield;
                if (m_gbfieldtype == Type.INT_TYPE) {             
                    gbfield = new IntField( (Integer) key);
                }
                else {
                    gbfield = new StringField( (String) key, m_gbfieldtype.getLen());
                }
                tup.setField(0, gbfield);
                tup.setField(1, aggregatefield);
            }
            tuples.add(tup);
        }
        return new TupleIterator(tupleDesc,tuples);
    }

}
