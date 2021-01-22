/*
 * Copyright (C) 2018 The Astarte Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.harbby.astarte.core.api;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class AstarteConf
        implements Serializable
{
    private final Map<String, String> props = new HashMap<>();

    public void addConf(AstarteConf conf)
    {
        props.putAll(conf.props);
    }

    public void put(String key, String value)
    {
        props.put(key, value);
    }

    public String getString(String key)
    {
        return getString(key, null);
    }

    public String getString(String key, String defaultValue)
    {
        return props.getOrDefault(key, defaultValue);
    }

    public int getInt(String key)
    {
        String value = props.get(key);
        if (value == null) {
            throw new NullPointerException();
        }
        else {
            return Integer.parseInt(value);
        }
    }

    public int getInt(String key, int defaultValue)
    {
        String value = props.get(key);
        if (value == null) {
            return defaultValue;
        }
        else {
            return Integer.parseInt(value);
        }
    }
}
