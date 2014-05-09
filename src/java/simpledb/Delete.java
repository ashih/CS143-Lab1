package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */

    private TransactionId m_tid;
    private DbIterator m_child;
    private boolean m_deleted;
    private TupleDesc m_td;

    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        m_tid = t;
        m_child = child;

        Type[] temp1 = new Type[1];
        temp1[0] = Type.INT_TYPE;
        String[] temp2 = new String[1];
        temp2[0] = "DeleteCount";
        m_td = new TupleDesc (temp1,temp2);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return m_td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        m_child.open();
        m_deleted = false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (m_deleted) return null;
        
        BufferPool pool = Database.getBufferPool();
        
        int count = 0;
        try{
            while (m_child.hasNext()) {
                Tuple next = m_child.next();
                count++;
                pool.deleteTuple(m_tid, next);
            }
        } catch (IOException e) {
            System.out.println("Error deleting tuple");
            e.printStackTrace();
            System.out.println(e.getMessage());
            System.exit(1);
        }

        Tuple result = new Tuple(m_td);
        IntField intField = new IntField(count);
        result.setField(0, intField);
        m_deleted = true;
        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] children = new DbIterator[1];
        children[1] = m_child;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        m_child = children[0];
    }

}
