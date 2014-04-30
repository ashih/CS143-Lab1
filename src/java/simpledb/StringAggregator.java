package simpledb;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    int m_gbfield;
    Type m_gbfieldtype;
    int m_afield;
    Op m_what;
    HashMap<Object,Integer> m_count;
    String m_keyName;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        m_gbfield = gbfield;
        m_gbfieldtype = gbfieldtype;
        m_afield = afield;
        m_what = what;
        m_count = new HashMap<Object,Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (m_keyName == null)
            m_keyName = tup.getTupleDesc().getFieldName(m_gbfield);
        Object key;
        if (tup.getField(m_gbfield).getType() == Type.INT_TYPE)
            key = ((IntField) tup.getField(m_gbfield)).getValue();
        else 
            key = ((StringField) tup.getField(m_gbfield)).getValue();

        int count;
        
        if (!m_count.containsKey(key)){            
            m_count.put(key,1);
        }
        else {
            count = m_count.get(key);
            count++;
            m_count.put(key,count);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
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
        for(Map.Entry<Object,Integer> entry : m_count.entrySet()) {
            Tuple tup = new Tuple(tupleDesc);
            Object key = entry.getKey();
            int count = entry.getValue();
            
            Field countfield = new IntField(count);            
            if (m_gbfield == NO_GROUPING) {
                tup.setField(0, countfield);
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
                tup.setField(1, countfield);
            }
            tuples.add(tup);
        }
        return new TupleIterator(tupleDesc,tuples);
    }

}
