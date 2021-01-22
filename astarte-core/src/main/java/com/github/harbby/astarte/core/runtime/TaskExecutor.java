package com.github.harbby.astarte.core.runtime;

public class TaskExecutor
{
    private TaskExecutor() {}

    public static void main(String[] args)
            throws Exception
    {
        int vcores = 2;
        Executor executor = new Executor(vcores);
        executor.join();
    }
}
