package de.tum.i13.shared;


import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVServerInfo;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.TreeMap;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;

public class Metadata implements Serializable {

    private TreeMap<String, KVServerInfo> serverMap;
    private KVServerInfo serverInfo;

    public Metadata(KVServerInfo serverInfo) {
        this.serverInfo = serverInfo;
        this.serverMap = new TreeMap<>();
    }

    public TreeMap<String, KVServerInfo> getServerMap() {
        return serverMap;
    }
    
    public boolean checkServerResposible(String keyHash){

        String key = serverMap.ceilingKey(calculateHash(keyHash));

        if(key.isEmpty()){
            key = serverMap.firstKey();
        }

        return key.equals(serverInfo.getServerKeyHash()) ? true : false;
    }


    public String getServerHashRange(){
        return "<"+ serverInfo.getStartIndex() + ">, <" + serverInfo.getEndIndex() + ">, <" + serverInfo.getAddress() + ":" + serverInfo.getPort() + ">;\r\n";
    }

    public static String calculateHash(String str) {

        try {
            MessageDigest msgDigest = MessageDigest.getInstance("MD5");
            byte[] message = msgDigest.digest(str.getBytes(TELNET_ENCODING));
            return message.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return null;
    }

}

