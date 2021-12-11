package de.tum.i13.shared;


import de.tum.i13.server.kv.KVServerInfo;

import java.io.Serializable;
import java.util.TreeMap;


public class Metadata implements Serializable {

    // <hash of server, server info object>
    private TreeMap<String, KVServerInfo> serverMap;
    // only needed if instantiated in KVServer
    private KVServerInfo serverInfo;

    public Metadata() {
        this.serverInfo = null;
        this.serverMap = new TreeMap<>();
    }

    public TreeMap<String, KVServerInfo> getServerMap() {
        return serverMap;
    }
    
    public boolean checkServerResponsible(String keyHash){
        String key = serverMap.ceilingKey(calculateHash(keyHash));

        if(key.isEmpty())
            key = serverMap.firstKey();

        return key.equals(serverInfo.getServerKeyHash());
    }


    public String getServerHashRange(){
        return "<"+ serverInfo.getStartIndex() + ">, <" + serverInfo.getEndIndex() + ">, <" + serverInfo.getAddress() + ":" + serverInfo.getPort() + ">;\r\n";
    }

    public static String calculateHash(String str) {
        return Util.calculateHash(str);
    }

}

