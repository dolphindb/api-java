package com.xxdb;

import javax.crypto.Cipher;
import java.io.IOException;
import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

public class RSAUtils {
    public static PublicKey getPublicKey(String keyCode) throws IOException {
    	try{
	        byte[] keyBytes = decodeOpenSSLPublicKey(keyCode);
	        X509EncodedKeySpec keySpec=new X509EncodedKeySpec(keyBytes);
	        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
	        return keyFactory.generatePublic(keySpec);
    	}
    	catch(Exception ex){
    		throw new IOException(ex.getMessage());
    	}
    }

    public static byte[] decodeOpenSSLPublicKey(String instr) {
        String pempubheader = "-----BEGIN PUBLIC KEY-----";
        String pempubfooter = "-----END PUBLIC KEY-----";
        String pemstr = instr.trim();
        byte[] binkey;
        pemstr = pemstr.replace(pempubheader, "" ).replace(pempubfooter, "" ).replaceAll("\\s","");
        String pubstr = pemstr.trim();
        binkey = Base64.getDecoder().decode(pubstr);
        return binkey;
    }

    public static byte[] encryptByPublicKey(byte[] data,PublicKey pk) throws IOException{
    	try{
	        Cipher cipher=Cipher.getInstance("RSA/ECB/PKCS1Padding");
	        cipher.init(Cipher.ENCRYPT_MODE, pk);
	        return cipher.doFinal(data);
    	}
    	catch(Exception ex){
    		throw new IOException(ex.getMessage());
    	}
    }

    public static byte[] encrypt(byte[] content, PublicKey publicKey) throws IOException{
    	try{
	        Cipher cipher=Cipher.getInstance("RSA/ECB/PKCS1Padding");
	        cipher.init(Cipher.ENCRYPT_MODE, publicKey);
	        return cipher.doFinal(content);
    	}
    	catch(Exception ex){
    		throw new IOException(ex.getMessage());
    	}
    }

    public static byte[] decrypt(byte[] content, PrivateKey privateKey) throws IOException{
    	try{
	        Cipher cipher=Cipher.getInstance("RSA");
	        cipher.init(Cipher.DECRYPT_MODE, privateKey);
	        return cipher.doFinal(content);
    	}
    	catch(Exception ex){
    		throw new IOException(ex.getMessage());
    	}
    }
}
