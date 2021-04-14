package com.xxdb.compression;

public class DecoderFactory {

    private DecoderFactory() {}
    
    public static Decoder get(int method){
    	if(method == 1)
    		return new LZ4Decoder();
    	else if(method == 2)
    		return new DeltaOfDeltaDecoder();
    	else
    		throw new RuntimeException("Invalid compression method " + method);
    }
}
