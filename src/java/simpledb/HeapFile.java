package simpledb;

import java.io.*;
import java.nio.*;
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

    protected File f;
    protected TupleDesc td;
    protected FileChannel rafch;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        RandomAccessFile raf = new RandomAccessFile(f,"rw");
        rafch = raf.getChannel();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
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
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageNumber = pid.pageNumber();
        int offset = BufferPool.PAGE_SIZE * pageNumber;
        ByteBuffer buff = new ByteBuffer.allocate(BufferPool.PAGE_SIZE); 
        rafch.read(buff, offset);
        HeapPageId hpid = (HeapPageId) pid;
        return new HeapPage(hpid, buff.array());    
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
        int numPages = (int) Math.floor(rafch.size() / BufferPool.PAGE_SIZE);
        return numPages;
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
            protected int currpid;
            protected Page currp;
            protected int numPages;
            protected Iterator<Tuple> tIterator;

            public HeapFileIterator(TransactionId tid, HeapFile hf) {
                this.tid = tid;
                this.hf = hf;
                currpid = 0;
                numPages = hf;
            }

            public void open() {
                currp = readPage(currpid);
                tIterator = currp.iterator();
            }

            public boolean hasNext() {
                if (tIterator == null) return false;
                if (tIterator.hasNext()) return true;
                while (currpid <= (numPages-1)) {
                    currp = readPage(currpid+1);
                    tIterator = currp.iterator();
                    if (tIterator.hasNext()) return true;
                }
                return false;

            }

            public Tuple next() {
                return tIterator.next();
            }

            public void rewind() throws DbException, TransactionAbortedException {
                open();
                close();
            }

            public void close() {
                currpid = 0;
                tIterator = null;
            }
        }
        //DbFileIterator it = new HeapFileIterator();
        //return it;
        HeapFileIterator it = new HeapFileIterator(tid, hf);
        return it;
    }

}

