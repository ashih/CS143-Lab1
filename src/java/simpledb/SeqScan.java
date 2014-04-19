package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name m_tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            m_tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            m_tableAlias.null, or null.null).
     */
    private TransactionId m_tid;
    private int m_tableid;
    private String m_tableAlias;
    private DbFileIterator m_iter;
    private DbFile m_f;

    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        m_tid = tid;
        m_tableid = tableid;
        m_tableAlias = tableAlias;
        m_f = Database.getCatalog().getDatabaseFile(m_tableid);
        m_iter = m_f.iterator(m_tid);
    }

    /**
     * @returnø
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(m_tableid);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        // some code goes here
        return m_tableAlias;
    }

    /**
     * Reset the m_tableid, and m_tableAlias of this operator.
     * @param m_tableid
     *            the table to scan.
     * @param m_tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name m_tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            m_tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            m_tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        m_tableid = tableid;
        m_tableAlias = tableAlias;
        m_f = Database.getCatalog().getDatabaseFile(tableid);
        m_iter = m_f.iterator(m_tid);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        m_iter.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the m_tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the m_tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc temp = m_f.getTupleDesc();
        int len = temp.numFields();
        Type[] types = new Type[len];
        String[] names = new String[len];
        for (int i = 0; i < len; i++)
        {
            types[i] = temp.getFieldType(i);
            names[i] = m_tableAlias + "," + temp.getFieldName(i);
        }
        return new TupleDesc(types,names);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return m_iter.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return m_iter.next();
    }

    public void close() {
        // some code goes here
        m_iter.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        m_iter.rewind();
    }
}
