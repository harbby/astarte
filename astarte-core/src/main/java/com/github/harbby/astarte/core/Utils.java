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
package com.github.harbby.astarte.core;

import com.github.harbby.gadtry.base.ClassLoaders;
import com.github.harbby.gadtry.base.Platform;
import com.github.harbby.gadtry.base.Serializables;
import com.github.harbby.gadtry.base.Throwables;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;

public class Utils<E extends Serializable>
{
    private Utils() {}

    public static <T extends Serializable> T clear(T serializable)
    {
        if (serializable == null) {
            return serializable;
        }
        try {
            byte[] bytes = Serializables.serialize(serializable);
            return Serializables.byteToObject(bytes, serializable.getClass().getClassLoader());
        }
        catch (IOException | ClassNotFoundException e) {
            throw Throwables.throwsThrowable(e);
        }
    }

    /**
     * load other jar to system classLoader
     *
     * @param urls jars
     */
    public static void loadExtJarToSystemClassLoader(List<URL> urls)
    {
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        if (classLoader instanceof URLClassLoader) {
            try {
                Method addURLMethod = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
                addURLMethod.setAccessible(true);
                for (URL uri : urls) {
                    addURLMethod.invoke(classLoader, uri);
                }
                return;
            }
            catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw Throwables.throwsThrowable(e);
            }
        }
        //java11+
        try {
            Field field = classLoader.getClass().getDeclaredField("ucp");
            Platform.addOpenJavaModules(field.getType(), ClassLoaders.class);
            field.setAccessible(true);
            Object ucp = field.get(classLoader);
            Method addURLMethod = ucp.getClass().getMethod("addURL", URL.class);
            for (URL url : urls) {
                addURLMethod.invoke(ucp, url);
            }
        }
        catch (NoSuchMethodException | NoSuchFieldException | InvocationTargetException | IllegalAccessException e) {
            throw new UnsupportedOperationException("this jdk not support", e);
        }
    }

    /**
     * get system classLoader ucp jars
     *
     * @return system classLoader ucp jars
     */
    public static List<URL> getSystemClassLoaderJars()
    {
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        if (classLoader instanceof URLClassLoader) {
            return java.util.Arrays.asList(((URLClassLoader) classLoader).getURLs());
        }
        //java11+
        try {
            Field field = classLoader.getClass().getDeclaredField("ucp");
            Platform.addOpenJavaModules(field.getType(), ClassLoaders.class);
            field.setAccessible(true);
            Object ucp = field.get(classLoader);
            Method getURLs = ucp.getClass().getMethod("getURLs");
            URL[] urls = (URL[]) getURLs.invoke(ucp);
            return Arrays.asList(urls);
        }
        catch (NoSuchMethodException | NoSuchFieldException | InvocationTargetException | IllegalAccessException e) {
            throw new UnsupportedOperationException("this jdk not support", e);
        }
    }
}
