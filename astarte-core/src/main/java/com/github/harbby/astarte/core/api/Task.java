package com.github.harbby.astarte.core.api;

import com.github.harbby.astarte.core.TaskContext;

import java.io.Serializable;

public interface Task<R>
        extends Serializable
{
    public int getTaskId();

    public R runTask(TaskContext taskContext);

    public Stage getStage();
}
