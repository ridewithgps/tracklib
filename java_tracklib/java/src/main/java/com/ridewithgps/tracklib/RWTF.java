package com.ridewithgps.tracklib;

import java.util.*;

public class RWTF {
    private static native List<Map<String, DataField>> parse_rwtf(byte[] input);
    private List<Map<String, DataField>> data;

    static {
        System.loadLibrary("tracklib");
    }

    public RWTF(byte[] input) {
        this.data = parse_rwtf(input);
    }

    public Iterator<Map<String, DataField>> iterator() {
        return data.iterator();
    }

    public String toString() {
        return this.data.toString();
    }
}
