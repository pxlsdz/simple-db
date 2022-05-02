package util;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.storage.BufferPool;
import simpledb.storage.DbFileIterator;
import simpledb.storage.HeapFile;
import simpledb.storage.HeapPage;
import simpledb.storage.HeapPageId;
import simpledb.storage.Page;
import simpledb.storage.PageId;
import simpledb.storage.Tuple;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
    private int heapFileId;
    private int numPages;
    private TransactionId tid;
    private int curPage  = 0;
    private HeapPage curHeapPage;
    private Iterator<Tuple> it;
    private boolean isOpen = false;

    public HeapFileIterator(int numPages, int id,  TransactionId tid){
        this.numPages = numPages;
        this.heapFileId = id;
        this.tid = tid;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        isOpen = true;
        curPage = 0;
        curHeapPage = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(heapFileId, curPage), Permissions.READ_ONLY);
        it = curHeapPage.iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if(it != null && it.hasNext()){
            return true;
        }
        curPage++;
        if(curPage >= numPages) {
            return false;
        }
        curHeapPage = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(heapFileId, curPage), Permissions.READ_ONLY);
        it = curHeapPage.iterator();
        return curPage < numPages;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if(!isOpen || it == null) {
            throw new NoSuchElementException();
        }
        return it.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        open();
    }

    @Override
    public void close() {
        isOpen = false;
        curPage = 0;
    }
}
