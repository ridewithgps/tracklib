package com.ridewithgps.tracklib;

import java.util.*;

public class DataField {
    private long numberValue;
    private double longFloatValue;
    private double shortFloatValue;
    private String base64Value;
    private String stringValue;
    private boolean boolValue;
    private List<Long> idsValue;
    private Type type;

    public void setNumberValue(long v) {
        this.numberValue = v;
        this.type = Type.Number;
    }

    public void setLongFloatValue(double v) {
        this.longFloatValue = v;
        this.type = Type.LongFloat;
    }

    public void setShortFloatValue(double v) {
        this.shortFloatValue = v;
        this.type = Type.ShortFloat;
    }

    public void setBase64Value(String v) {
        this.base64Value = v;
        this.type = Type.Base64;
    }

    public void setStringValue(String v) {
        this.stringValue = v;
        this.type = Type.String;
    }

    public void setBoolValue(boolean v) {
        this.boolValue = v;
        this.type = Type.Bool;
    }

    public void setIDsValue(List<Long> v) {
        this.idsValue = v;
        this.type = Type.IDs;
    }

    public boolean isNumber() {
        return Type.Number.equals(this.type);
    }

    public boolean isLongFloat() {
        return Type.LongFloat.equals(this.type);
    }

    public boolean isShortFloat() {
        return Type.ShortFloat.equals(this.type);
    }

    public boolean isBase64() {
        return Type.Base64.equals(this.type);
    }

    public boolean isString() {
        return Type.String.equals(this.type);
    }

    public boolean isBool() {
        return Type.Bool.equals(this.type);
    }

    public boolean isIDs() {
        return Type.IDs.equals(this.type);
    }

    public long asNumber() {
        return this.numberValue;
    }

    public double asLongFloat() {
        return this.longFloatValue;
    }

    public double asShortFloat() {
        return this.shortFloatValue;
    }

    public String asBase64() {
        return this.base64Value;
    }

    public String asString() {
        return this.stringValue;
    }

    public boolean asBool() {
        return this.boolValue;
    }

    public List<Long> asIDs() {
        return this.idsValue;
    }

    public String toString() {
        if (isNumber()) {
            return String.format("Number(%s)", this.numberValue);
        } else if (isLongFloat()) {
            return String.format("LongFloat(%s)", this.longFloatValue);
        } else if (isShortFloat()) {
            return String.format("ShortFloat(%s)", this.shortFloatValue);
        } else if (isBase64()) {
            return String.format("Base64(%s)", this.base64Value);
        } else if (isString()) {
            return String.format("String(%s)", this.stringValue);
        } else if (isBool()) {
            return String.format("Bool(%s)", this.boolValue);
        } else if (isIDs()) {
            return String.format("IDs(%s)", this.idsValue);
        } else {
            return "Null";
        }
    }

    private enum Type {
        Number,
        LongFloat,
        ShortFloat,
        Base64,
        String,
        Bool,
        IDs
    }
}
