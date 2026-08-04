package com.xxdb.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class DdbByteArrayInputStream extends InputStream {
    protected List<byte[]> bufList_ = new ArrayList<>();

    public DdbByteArrayInputStream(List<byte[]> list) {
        this.bufList_.addAll(list);
        this.pos_ = 0;
        this.count_=0;
        this.bufIndex_=0;
        this.bufPos_=0;
        list.forEach(one->{
            this.count_+=one.length;
        });
    }
    protected long count_,pos_;
    protected int bufIndex_,bufPos_;
    protected long mark_ = 0;
    protected int markBufIndex_=0,markBufPos_=0;

    public synchronized int read() {
        if(pos_>=count_)
            return -1;
        pos_++;
        byte[] bytes=bufList_.get(bufIndex_);
        int ret = bytes[bufPos_++] & 0xff;
        if(bufPos_>=bytes.length) {
            bufIndex_++;
            bufPos_ = 0;
        }
        return ret;
    }

    public synchronized void append(byte[] buf){
        bufList_.add(buf);
        count_ += buf.length;
    }

    /**
     * Bulk read across the internal block list. {@link InputStream} never declares this
     * overload with {@code int} off/len as abstract, so without this override callers such as
     * {@link java.io.BufferedInputStream#read(byte[], int, int)} fall back to the default
     * {@link InputStream#read(byte[], int, int)} implementation, which repeatedly calls the
     * single-byte {@link #read()} above. For large decompressed payloads that turns every
     * {@code readFully} into millions of synchronized single-byte calls. All backing bytes are
     * already resident in {@code bufList_}, so a plain {@code System.arraycopy} across the
     * relevant blocks is both correct and non-blocking.
     */
    @Override
    public synchronized int read(byte[] b, int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return 0;
        }
        if (pos_ >= count_) {
            return -1;
        }
        int remaining = len;
        int totalRead = 0;
        while (remaining > 0 && pos_ < count_) {
            byte[] bytes = bufList_.get(bufIndex_);
            int available = bytes.length - bufPos_;
            int n = Math.min(available, remaining);
            System.arraycopy(bytes, bufPos_, b, off + totalRead, n);
            bufPos_ += n;
            pos_ += n;
            totalRead += n;
            remaining -= n;
            if (bufPos_ >= bytes.length) {
                bufIndex_++;
                bufPos_ = 0;
            }
        }
        return totalRead;
    }

    /**
     * Retained for binary compatibility with clients compiled against the historical,
     * non-overriding overload. Use {@link #read(byte[], int, int)} instead.
     *
     * @deprecated Deprecated since 3.00.6.0. This overload has never been a valid override of
     *             {@link InputStream#read(byte[], int, int)} ({@code off}/{@code len} must be
     *             {@code int}); calls still throw {@link RuntimeException}. Use
     *             {@link #read(byte[], int, int)} instead.
     */
    @Deprecated
    public synchronized int read(byte[] b, long off, long len) {
        throw new RuntimeException("This method is not support yet");
    }

    public synchronized long skip(long n) {
        long ret=n;
        pos_ += n;
        while (n > 0){
            byte[] bytes = bufList_.get(bufIndex_);
            if (n >= bytes.length - bufPos_){
                n = n - (bytes.length - bufPos_);
                bufPos_ = 0;
                bufIndex_ ++;
            }else {
                bufPos_ += n;
                n = 0;
            }
        }
        return ret;
    }

    public synchronized int available() {
        long len=count_ - pos_;
        if(len>Integer.MAX_VALUE)
            return Integer.MAX_VALUE;
        else
            return (int)len;
    }

    public boolean markSupported() {
        return true;
    }

    public void mark(int readAheadLimit) {
        mark_ = pos_;
        markBufIndex_=bufIndex_;
        markBufPos_=bufPos_;
    }

    public synchronized void reset() {
        pos_ = mark_;
        bufIndex_=markBufIndex_;
        bufPos_=markBufPos_;
    }

    public void close() throws IOException {
    }


}
