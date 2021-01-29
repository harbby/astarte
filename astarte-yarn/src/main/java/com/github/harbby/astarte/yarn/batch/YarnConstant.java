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
package com.github.harbby.astarte.yarn.batch;

public class YarnConstant
{
    private YarnConstant() {}

    public static final String APP_MAIN_CLASS = "YARN_APP_MAIN_CLASS";

    public static final String APP_CLASSPATH = "CLASSPATH";

    public static final String EXECUTOR_VCORES = "EXECUTOR_VCORES";

    public static final String EXECUTOR_MEMORY = "EXECUTOR_MEMORY_MB";

    public static final String EXECUTOR_NUMBERS = "EXECUTOR_NUMBERS";

    public static final String APP_CACHE_FILES = "APP_CACHE_FILES";
    public static final String APP_CACHE_DIR = "APP_CACHE_DIR";
}
