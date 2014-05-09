package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */

    private TransactionId m_tid;
    private DbIterator m_child;
    private int m_tableid;
    private boolean m_inserted;
    private TupleDesc m_td;

    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
        m_tid = t;
        m_child = child;
        m_tableid = tableid;
        m_inserted = false;

        Type[] temp1 = new Type[1];
        temp1[0] = Type.INT_TYPE;
        String[] temp2 = new String[1];
        temp2[0] = "InsertCount";
        m_td = new TupleDesc (temp1,temp2);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return m_td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        m_child.open();
        m_inserted = false;
        super.open();
    }

    public void close() {
        // some code goes here
        m_child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        m_child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (m_inserted) return null;
        
        BufferPool pool = Database.getBufferPool();
        
        int count = 0;
        try {
            while (m_child.hasNext()) {
                Tuple next = m_child.next();
                count++;
                pool.insertTuple(m_tid, m_tableid, next);
            }
        } catch (IOException e) {
            System.out.println("Error inserting tuple");
            e.printStackTrace();
            System.out.println(e.getMessage());
            System.exit(1);
        }
        
        Tuple result = new Tuple(m_td);
        IntField intField = new IntField(count);
        result.setField(0, intField);
        m_inserted = true;
        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] children = new DbIterator[1];
        children[0] = m_child;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        m_child = children[0];
    }
}
