package simpledb;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {


    protected File m_f;
    protected TupleDesc m_td;    

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        m_f = f;
        m_td = td;           
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return m_f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return m_f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return m_td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try { 

            RandomAccessFile raf = new RandomAccessFile(m_f,"r");
            byte[] buffer = new byte[BufferPool.PAGE_SIZE];
            int offset = BufferPool.PAGE_SIZE * pid.pageNumber();
            raf.seek(offset);
            raf.read(buffer, 0, BufferPool.PAGE_SIZE);
            raf.close();
            HeapPageId hpid = (HeapPageId) pid;
            return new HeapPage(hpid, buffer);
        } catch (IOException e) {
            System.err.println("IO error when reading page");
            System.exit(1);
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil( (double) (m_f.length()/BufferPool.PAGE_SIZE));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        class HeapFileIterator implements DbFileIterator {

            protected TransactionId tid;
            protected HeapFile hf;
            protected int tableid;
            protected int currpid;
            protected HeapPage currp;
            protected int numPages;
            protected Iterator<Tuple> tIterator;

            public HeapFileIterator(TransactionId tid, HeapFile hf) {
                this.tid = tid;
                this.hf = hf;
                tableid = hf.getId();
                currpid = 0;
                numPages = hf.numPages();
            }

            public void open() throws TransactionAbortedException, DbException {                
                HeapPageId hpid = new HeapPageId(tableid,currpid);
                currp = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
                tIterator = currp.iterator();
            }

            public boolean hasNext() throws TransactionAbortedException, DbException {
                if (tIterator == null) return false;
                if (tIterator.hasNext()) return true;
                while (currpid < numPages-1) {
                    currpid++;
                    HeapPageId hpid = new HeapPageId(tableid,currpid);
                    currp = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
                    tIterator = currp.iterator();
                    if (tIterator.hasNext()) return true;
                }                
                return false;
            }

            public Tuple next() {
                if (tIterator == null)
                    throw new NoSuchElementException();
                return tIterator.next();
            }

            public void rewind() throws DbException, TransactionAbortedException {                
                close();
                open();
            }

            public void close() {
                currpid = 0;
                tIterator = null;
            }
        }
        //DbFileIterator it = new HeapFileIterator();
        //return it;
        HeapFileIterator it = new HeapFileIterator(tid, this);
        return it;
    }

}

