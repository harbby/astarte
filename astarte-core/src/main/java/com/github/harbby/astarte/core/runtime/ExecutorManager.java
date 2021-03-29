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
package com.github.harbby.astarte.core.runtime;

import java.net.SocketAddress;
import java.util.Optional;

public abstract class ExecutorManager
        implements Service
{
    private static Factory factory;
    private final int vcores;
    private final int executorMem;
    private final int executorNum;

    protected ExecutorManager(int vcores, int executorMem, int executorNum)
    {
        this.vcores = vcores;
        this.executorMem = executorMem;
        this.executorNum = executorNum;
    }

    public int getExecutorNum()
    {
        return executorNum;
    }

    public int getExecutorMem()
    {
        return executorMem;
    }

    public int getVcores()
    {
        return vcores;
    }

    static ExecutorManager createExecutorManager(int vcores, int memMb, int executorNum, SocketAddress driverManagerAddress)
    {
        return Optional.ofNullable(factory).orElse(ForkVmExecutorManager::new)
                .createExecutorManager(vcores, memMb, executorNum, driverManagerAddress);
    }

    public static void setFactory(Factory factory)
    {
        ExecutorManager.factory = factory;
    }

    public static interface Factory
    {
        public ExecutorManager createExecutorManager(int vcores, int memMb, int executorNum, SocketAddress driverManagerAddress);
    }
}
