package com.xxdb.io;

import java.io.ByteArrayInputStream;
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

    public synchronized int read(byte b[], long off, long len) {
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
                n = 0;
                bufPos_ += n;
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
