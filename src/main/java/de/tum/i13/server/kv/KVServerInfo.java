package de.tum.i13.server.kv;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;

public class KVServerInfo {

    private String address;
    private int port;
    private int intraPort;
    private String startIndex;
    private String endIndex;
    private String serverKeyHash;

    public KVServerInfo(String address, int port, String startIndex, String endIndex, int intraPort){
        this.address = address;
        this.port = port;
        this.intraPort = intraPort;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.serverKeyHash = calculateHash(this.address, this.port);
    }

    public String getServerKeyHash() {
        return serverKeyHash;
    }

    public void setServerKeyHash(String serverKeyHash) {
        this.serverKeyHash = serverKeyHash;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public int getIntraPort() { return intraPort; }

    public String getStartIndex() {
        return startIndex;
    }

    public String getEndIndex() {
        return endIndex;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setIntraPort(int intraPort) { this.intraPort = intraPort; }

    public void setStartIndex(String startIndex) {
        this.startIndex = startIndex;
    }

    public void setEndIndex(String endIndex) {
        this.endIndex = endIndex;
    }

    private static String calculateHash(String address, int port) {

        try {
            String serverKey = address + port;
            MessageDigest msgDigest = MessageDigest.getInstance("MD5");
            byte[] message = msgDigest.digest(serverKey.getBytes(TELNET_ENCODING));
            return message.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return null;
    }

}
