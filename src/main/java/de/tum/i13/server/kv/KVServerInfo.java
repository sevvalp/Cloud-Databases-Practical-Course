package de.tum.i13.server.kv;

import de.tum.i13.shared.Util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;

public class KVServerInfo {

    private UUID uuid;
    private String address;
    private int port;
    private int intraPort;
    private String startIndex;
    private String endIndex;
    private String serverKeyHash;

    public KVServerInfo(UUID uuid, String address, int port, String startIndex, String endIndex, int intraPort){
        this.uuid = uuid;
        this.address = address;
        this.port = port;
        this.intraPort = intraPort;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.serverKeyHash = calculateHash(this.address, this.port);
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
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
        return Util.calculateHash(address, port);
    }

}
