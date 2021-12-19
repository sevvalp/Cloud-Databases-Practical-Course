package de.tum.i13.shared;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Util {

    private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

    public static int getFreePort() {

        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * Converts a byte array to hex.
     * Copied from https://stackoverflow.com/questions/9655181/how-to-convert-a-byte-array-to-a-hex-string-in-java
     * @param bytes byte array to convert
     * @return hex representation of byte array.
     */
    public static String bytesToHex(byte[] bytes) {
        byte[] hexChars = new byte[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars, StandardCharsets.UTF_8);
    }

    /**
     * Calculates MD5 Hash for a server, will use address:port as input.
     * @param address
     * @param port
     * @return
     */
    public static String calculateHash(String address, int port) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
//            md.update((address + "+" + port).getBytes());
            md.update((address + port).getBytes());
            return bytesToHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            // we will never reach here as MD5 is a supported Algorithm
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Calculates MD5 Hash of a string.
     * @param str String to hash
     * @return
     */
    public static String calculateHash(String str) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(str.getBytes());
            return bytesToHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            // we will never reach here as MD5 is a supported Algorithm
            e.printStackTrace();
        }
        return "";
    }
}
