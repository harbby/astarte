package com.github.harbby.astarte.runtime;

public class TaskExecutor
{
    private TaskExecutor() {}

    public static void main(String[] args)
            throws Exception
    {
        int vcores = 2;
        Executor executor = new Executor(2);
        executor.join();
    }
}
