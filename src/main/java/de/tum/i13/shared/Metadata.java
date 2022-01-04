package de.tum.i13.shared;


import de.tum.i13.client.TestStore;
import de.tum.i13.server.kv.KVServerInfo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.logging.Logger;


public class Metadata implements Serializable {

    // <hash of server, server info object>
    private TreeMap<String, KVServerInfo> serverMap;
    // only needed if instantiated in KVServer
    private KVServerInfo serverInfo;
    private static final Logger LOGGER = Logger.getLogger(TestStore.class.getName());

    public Metadata(KVServerInfo info) {
        this.serverInfo = info;
        this.serverMap = new TreeMap<>();
    }

    public Metadata(KVServerInfo info, String toParse) {
        this.serverInfo = info;
        this.serverMap = new TreeMap<>();
        String[] parse = toParse.split(";");
        for (String s : parse) {
            KVServerInfo i = new KVServerInfo(s);
            serverMap.put(i.getServerKeyHash(), i);
        }
    }

    public Metadata(String addressPort, String toParse) {
        this.serverMap = new TreeMap<>();
        String[] parse = toParse.split(";");
        for (String s : parse) {
            KVServerInfo i = new KVServerInfo(s);
            serverMap.put(i.getServerKeyHash(), i);
        }
        String[] addressInfo = addressPort.split(":");
        this.serverInfo = serverMap.get(Util.calculateHash(addressInfo[0], Integer.parseInt(addressInfo[1])));
    }

    public void updateClientMetadata(String toParse){
        String[] parse = toParse.split(";");
        this.serverMap.clear();
        for (String s : parse) {
            String[] serverInfo = s.split(",");
            KVServerInfo i = new KVServerInfo(serverInfo[0], serverInfo[1], serverInfo[2]);
            serverMap.put(i.getServerKeyHash(), i);
        }
    }
    public void updateMetadata(String toParse) {
        String[] parse = toParse.split(";");
        this.serverMap.clear();
        for (String s : parse) {
            KVServerInfo i = new KVServerInfo(s);
            serverMap.put(i.getServerKeyHash(), i);
        }
    }

    /**
     * This function return the ip and the port of the server
     * that is responsible for the file identified with the hash passed
     * as parameter
     *
     * @param index the hash of the file that we want to access
     * @return A pair <IP, port>
     */
    public Pair<String, Integer> getServerResponsible(String index) throws NoSuchElementException, NullPointerException{
        if(serverMap == null){
            throw new NoSuchElementException();
        }
        Map.Entry<String, KVServerInfo> newMap = null;
        try {
            newMap = serverMap.ceilingEntry(index);
        } catch(NullPointerException e) {
            throw new NullPointerException();
        }

        KVServerInfo kvInfo = (newMap == null) ? serverMap.firstEntry().getValue(): newMap.getValue();

        return new Pair<>(kvInfo.getAddress(), kvInfo.getPort());
    }

    public TreeMap<String, KVServerInfo> getServerMap() {
        return serverMap;
    }

    public boolean checkServerResponsible(String keyHash){

        String key = serverMap.ceilingKey(calculateHash(keyHash));

        if(key == null || key.isEmpty())
            key = serverMap.firstKey();

        return key.equals(serverInfo.getServerKeyHash());
    }


    public String getServerHashRange(){

        //<range_from>,<range_to>,<ip:port>;
        String message = "";
        for(String s : serverMap.keySet()){
            KVServerInfo serverInfo = serverMap.get(s);
            message += serverInfo.getStartIndex() + "," + serverInfo.getEndIndex() + "," + serverInfo.getAddress() + ":" + serverInfo.getPort() + ";";
        }
        return message;
    }

    public String getServerHashRangeWithReplicas(){

        //<range_from>,<range_to>,<ip:port>;<range_from>,<range_to><rep1ip:rep1port>;<range_from>,<range_to><rep2ip:rep2port>;
        String message = "";
        for(String s : serverMap.keySet()){
            KVServerInfo serverInfo = serverMap.get(s);
            message += serverInfo.getStartIndex() + "," + serverInfo.getEndIndex() + "," + serverInfo.getAddress() + ":" + serverInfo.getPort() + ";";

            if(serverMap.size() > 2){
            ArrayList<String> replicaServers = getReplicaServers(s);

                for(int i=0; i<replicaServers.size(); i++){

                    KVServerInfo replicaInfo = serverMap.get(replicaServers.get(i));
                    message += serverInfo.getStartIndex() + "," + serverInfo.getEndIndex() + "," + replicaInfo.getAddress() + ":" + replicaInfo.getPort() +";";
                    //message +=  "," + serverInfo.getAddress() + ":" + serverInfo.getPort();
                }
            }

        }

        return message;
    }

    public String getRangeHash(String index) throws NoSuchElementException{
        if(serverMap == null){
            throw new NoSuchElementException();
        }
        Map.Entry<String, KVServerInfo> m = null;
        try {
            m = serverMap.ceilingEntry(index);
        } catch(NullPointerException e) {
            throw new NullPointerException();
        }

        String hash = (m == null) ? serverMap.firstEntry().getKey(): m.getKey();

        return hash;
    }

    public ArrayList<String> getReplicasHash(String serverInfo) {
        ArrayList<String> replicas = new ArrayList<>();
        String currentHash = calculateHash(serverInfo);
        String successorHash = null;
        if (serverMap.size() > 2) {
            for (int i = 0; i < 2; i++) {

                //get successor of the current server
                Map.Entry<String, KVServerInfo> successorServer;
                successorServer = serverMap.higherEntry(currentHash);
                if (successorServer == null)
                    successorServer = serverMap.firstEntry();

                successorHash = successorServer.getKey();
                replicas.add(successorHash);

                //do it again for the successor again
                currentHash = successorHash;
            }
        }

        return replicas;
    }

    public ArrayList<String> getReplicaServers(String serverInfo) {
        ArrayList<String> replicas = new ArrayList<>();
        String currentHash = serverInfo;
        String successorHash = null;
        if (serverMap.size() > 2) {
            for (int i = 0; i < 2; i++) {

                //get successor of the current server
                Map.Entry<String, KVServerInfo> successorServer;
                successorServer = serverMap.higherEntry(currentHash);
                if (successorServer == null)
                    successorServer = serverMap.firstEntry();

                successorHash = successorServer.getKey();
                replicas.add(successorHash);

                //do it again for the successor again
                currentHash = successorHash;
            }
        }

        return replicas;
    }


    public boolean isRoleReplica(String keyHash) {
        String responsibleServer =  serverMap.ceilingKey(calculateHash(keyHash));
        if (responsibleServer == null) {
            responsibleServer = serverMap.firstKey();
        }
        if(serverMap.size() > 2) {
            LOGGER.info("serverMap size is > than 2 so checking replica servers");
            return getReplicasHash(responsibleServer).contains(serverInfo.getServerKeyHash());
        }else{
            LOGGER.info("serverMap size is < than 2, returning true as deafult..");
            return true;
        }
    }

    public static String calculateHash(String str) {
        return Util.calculateHash(str);
    }

    @Override
    public String toString() {
        String s = "";
        for (KVServerInfo info : serverMap.values())
            s += info.toString() + ";";

        return s.substring(0, s.length() - 1);
    }

    public void setServerMap(TreeMap<String, KVServerInfo> serverMap) {
        this.serverMap = serverMap;
    }

    public boolean isEmpty(){
        return serverMap.isEmpty();
    }

    public void removeEntry(String serverHash){
        serverMap.remove(serverHash);
    }
}

