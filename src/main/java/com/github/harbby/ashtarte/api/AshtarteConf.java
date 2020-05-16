package com.github.harbby.ashtarte.api;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class AshtarteConf implements Serializable {
    private final Map<String, String> props = new HashMap<>();

    public String getString(String key) {
        return getString(key, null);
    }

    public String getString(String key, String defaultValue) {
        return props.getOrDefault(key, defaultValue);
    }

    public int getInt(String key) {
        String value = props.get(key);
        if (value == null) {
            throw new NullPointerException();
        } else {
            return Integer.parseInt(value);
        }
    }

    public int getInt(String key, int defaultValue) {
        String value = props.get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return Integer.parseInt(value);
        }
    }
}
