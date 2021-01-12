package com.github.harbby.ashtarte.runtime;

public interface ExecutorBackend {

    public void updateState(TaskEvent state);

    static ExecutorBackend getInstance() {
        return new ExecutorBackendImpl();
    }
}
