package com.learninghadoop2.hive.client;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.hadoop.hive.service.ThriftHive.Client;
import java.util.List;

public class HiveThriftClient {     
     private static Client getClient(String host, Integer port) {
        int TIMEOUT = 99999999;
          
        Client client=null;
        try {
             TSocket transport = new TSocket(host, port);
             transport.setTimeout(TIMEOUT);
             transport.open();
             TBinaryProtocol protocol = new TBinaryProtocol(transport);
             client = new Client(protocol);
             return client;
        }
        catch (Exception e) {
           e.printStackTrace();
           return null;
        }
     }
     
     public static void main(String[] args) throws Exception {
          Client client = getClient("localhost", 11000);
          client.execute("show tables");

          List<String> results = client.fetchAll();
          for (String result : results) {
            System.out.println(result);
          }
     }
}
