package com.github.harbby.ashtarte.runtime;

import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import com.github.harbby.gadtry.jvm.VmFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ForkVmExecutorManager
        implements ExecutorManager
{
    private final int vcores;
    private final int memMb;
    private final int executorNum;
    private List<VmFuture<Integer>> vms;
    private final ExecutorService pool;

    public ForkVmExecutorManager(int vcores, int memMb, int executorNum)
    {
        this.vcores = vcores;
        this.memMb = memMb;
        this.executorNum = executorNum;
        this.pool = Executors.newFixedThreadPool(executorNum);
        this.vms = new ArrayList<>(executorNum);
    }

    @Override
    public void start()
    {
        //启动所有Executor
        for (int i = 0; i < executorNum; i++) {
            JVMLauncher<Integer> launcher = JVMLaunchers.<Integer>newJvm()
                    .setName("ashtarte.Executor")
                    .setConsole(System.out::print)
                    //.addVmOps("-Dio.netty.leakDetectionLevel=advanced")
                    .setXmx(memMb + "m")
                    .build();
            VmFuture<Integer> vmFuture = runJvm(launcher, pool, vcores, executorNum);
            vms.add(vmFuture);
        }
    }

    private static VmFuture<Integer> runJvm(JVMLauncher<Integer> launcher,
            ExecutorService pool,
            int vcores, int executorNum)
    {
        return launcher.startAsync(pool, () -> {
            System.out.println("starting... Executor, vcores[" + vcores + "] mem[" + executorNum + "MB]");
            TaskManager.main(new String[0]);
            return 0;
        });
    }

    @Override
    public void stop()
    {
        vms.forEach(x -> x.cancel());
    }
}
