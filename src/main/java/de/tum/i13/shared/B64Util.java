package de.tum.i13.shared;

import java.util.Base64;

/**
 * Util class to encode/decode Base64 Strings.
 *
 * @version 0.1
 * @since   2021-11-12
 */
public class B64Util {

    /**
     * Encodes a string to Base64.
     * @param s String to encode.
     * @return Base64 encoded String.
     */
    public static String b64encode(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes());
    }

    /**
     * Decodes a Base64 string.
     * @param s Base64 String to decode.
     * @return Decoded String.″
     */
    public static String b64decode(String s) {
        return new String(Base64.getDecoder().decode(s));
    }
}
