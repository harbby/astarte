package com.github.harbby.ashtarte.runtime;

import com.github.harbby.ashtarte.api.Task;

public class TaskEvent {
    private final Object result;
    private final Class<? extends Task> taskType;

    public TaskEvent(Class<? extends Task> taskType, Object result) {
        this.result = result;
        this.taskType = taskType;
    }

    public Class<? extends Task> taskType() {
        return taskType;
    }

    public void getTaskResult() {

    }
}
