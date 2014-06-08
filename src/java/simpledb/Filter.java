package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    
    Predicate m_p;
    DbIterator m_child;
    
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
        m_p = p;
        m_child = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return m_p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return m_child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        m_child.open();
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
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while (m_child.hasNext()) {
            Tuple next = m_child.next();
            if (m_p.filter(next))
                return next;
        }
        return null;
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
