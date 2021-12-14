package de.tum.i13.server.kv;

import de.tum.i13.shared.Util;

import java.io.UnsupportedEncodingException;
import java.nio.channels.SelectionKey;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;

public class KVServerInfo {

    private String address;
    private int port;
    private int intraPort;
    private String startIndex;
    private String endIndex;
    private String serverKeyHash;
    private SelectionKey selectionKey;

    public KVServerInfo(String address, int port, String startIndex, String endIndex, int intraPort, SelectionKey selectionKey){
        this.address = address;
        this.port = port;
        this.intraPort = intraPort;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.serverKeyHash = calculateHash(this.address, this.port);
        this.selectionKey = selectionKey;
    }

    public KVServerInfo(String address, int port, String startIndex, String endIndex, int intraPort){
        this.address = address;
        this.port = port;
        this.intraPort = intraPort;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.serverKeyHash = calculateHash(this.address, this.port);
        this.selectionKey = null;
    }
    public KVServerInfo(String toParse) {
        String[] parse = toParse.split(",");
        for (String attribute : parse) {
            String[] att = attribute.split("=");
            if (att.length != 2)
                continue;
            Matcher m = Pattern.compile("(?<=\\\")(.*?)(?=\\\")").matcher(att[0]);
            String key = m.find() ? m.group(0) : "";
            m = Pattern.compile("(?<=\\\")(.*?)(?=\\\")").matcher(att[1]);
            String value = m.find() ? m.group(0) : att[1];
            setAttribute(key, value);
        }
    }

    public KVServerInfo(String startIndex, String endIndex, String addressPort){
        String firstElement = addressPort.split(":")[0];
        String secondElement = addressPort.split(":")[1];
        this.address = addressPort.split(":")[0].replace("<", "").trim();
        this.port = Integer.parseInt(addressPort.split(":")[1].substring(0,secondElement.length()-1));
        String sIndex = startIndex.replace("<", "").replace(">", "");
        String eIndex = endIndex.replace("<", "").replace(">", "").trim();
        this.startIndex = sIndex;
        this.endIndex = eIndex;
        this.intraPort = this.port;
        this.serverKeyHash = calculateHash(this.address, this.port);
    }

    private void setAttribute(String key, String value) {
        switch(key) {
            case "address":
                this.address = value;
                break;
            case "port":
                this.port = Integer.parseInt(value);
                break;
            case "intraPort":
                this.intraPort = Integer.parseInt(value);
                break;
            case "startIndex":
                this.startIndex = value;
                break;
            case "endIndex":
                this.endIndex = value;
                break;
            case "serverKeyHash":
                this.serverKeyHash = value;
                break;
        }
    }

    public SelectionKey getSelectionKey() {
        return selectionKey;
    }

    public void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
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

    public static String calculateHash(String address, int port) {
        return Util.calculateHash(address, port);
    }

    @Override
    public String toString() {
        return String.format("{" +
                "\"address\"=\"%s\", " +
                "\"port\"=%d, " +
                "\"intraPort\"=%d, " +
                "\"startIndex\"=\"%s\", " +
                "\"endIndex\"=\"%s\", " +
                "\"serverKeyHash\"=\"%s\"" +
                "}", address, port, intraPort, startIndex, endIndex, serverKeyHash);
    }

}
