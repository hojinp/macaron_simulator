package edu.cmu.pdl.macaronsimulator.common;

import java.util.HashMap;
import java.util.Map;

/* Code from https://stackoverflow.com/questions/561486/how-to-convert-an-integer-to-the-shortest-url-safe-string-in-python */
public class StrToIntDecoder {
    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    private final Map<Character, Integer> ALPHABET_REVERSE;
    private final int BASE = ALPHABET.length();
    private final char SIGN_CHARACTER = '$';
    private final int maxDecodedVal;

    public StrToIntDecoder(int maxDecodedVal) {
        this.maxDecodedVal = maxDecodedVal;
        ALPHABET_REVERSE = new HashMap<>();
        for (int i = 0; i < ALPHABET.length(); i++) {
            ALPHABET_REVERSE.put(ALPHABET.charAt(i), i);
        }
    }

    public int numDecode(String s) {
        if (s.charAt(0) == SIGN_CHARACTER) return -numDecode(s.substring(1));
        int n = 0;
        for (int i = 0; i < s.length(); i++) n = n * BASE + ALPHABET_REVERSE.get(s.charAt(i));
        // Assume n is integer range. TODO: Support n greater than Integer.MAX_VALUE
        assert n < maxDecodedVal : "decoded number (" + n + ") must be less than " + maxDecodedVal;
        return n;
    }

    public String strEncode(final int val) {
        int n = val;
        assert n >= 0;
        String ret = "";
        while (true) {
            int r = n % BASE;
            n = n / BASE;
            ret = ALPHABET.charAt(r) + ret;
            if (n == 0)
                break;
        }
        /* The following line is for sanity check. Please commnent the following line for the speed. */
        // if (numDecode(ret) != n) {
        //     Logger.getGlobal().info("Error: ret != n : " + ret + " != " + String.valueOf(val));
        // }
        // assert numDecode(ret) == val : "Error: ret != n : " + ret + " != " + String.valueOf(val);
        return ret;
    }
}
