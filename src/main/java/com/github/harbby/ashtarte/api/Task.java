package com.github.harbby.ashtarte.api;

import com.github.harbby.ashtarte.TaskContext;

import java.io.Serializable;

public interface Task<R> extends Serializable {
    public long getTaskId();

    public R runTask(TaskContext taskContext);
}
