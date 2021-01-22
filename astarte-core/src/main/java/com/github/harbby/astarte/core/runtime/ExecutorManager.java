package com.github.harbby.astarte.core.runtime;

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

    static ExecutorManager createExecutorManager(int vcores, int memMb, int executorNum)
    {
        return Optional.ofNullable(factory).orElse(ForkVmExecutorManager::new)
                .createExecutorManager(vcores, memMb, executorNum);
    }

    public static void setFactory(Factory factory)
    {
        ExecutorManager.factory = factory;
    }

    public static interface Factory
    {
        public ExecutorManager createExecutorManager(int vcores, int memMb, int executorNum);
    }
}
