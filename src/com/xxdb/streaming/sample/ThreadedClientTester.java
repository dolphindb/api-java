package com.xxdb.streaming.sample;

import java.io.IOException;
import java.net.SocketException;

import com.xxdb.streaming.client.ThreadedClient;

public class ThreadedClientTester {

	
	public static void stopPublish() {

	}
	public static void main(String args[]) throws SocketException {
		
		 ThreadedClient client = new ThreadedClient(8997);
		/*
		 
		//============ publish create table =============
        n=20000000
        t=table(n:0,`time`count`qty`price`exch,[TIMESTAMP,INT,INT,DOUBLE,INT])
        share t as trades
        setStream(trades,true)
        t=NULL;
        //===========insert data
        rows = 1
        timev = take(now(), rows)
        count = take(1, rows)
        qtyv = take(112, rows)
        pricev = take(53.75, rows)
        exchv = take(2, rows)
        for(x in 0:20000000){
        insert into trades values(timev, count, qtyv, pricev, exchv)
        }
        insert into trades values(timev, count, take(-1, rows), pricev, exchv);
		client.GetLocalIP();
		 */

        try {
			//client.subscribe("192.168.1.45", 8904, "trades2", new TwoSigmaMessageHandler(), 0);
			client.subscribe("192.168.1.42", 8904, "trades1", new TwoSigmaMessageHandler(), 0);
			//client.subscribe("192.168.1.14", 8082, "trades1", new TwoSigmaMessageHandler(), 0);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
