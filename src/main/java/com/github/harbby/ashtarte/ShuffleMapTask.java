package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.api.Task;
import com.github.harbby.ashtarte.utils.SerializableObj;

public class ShuffleMapTask<E> implements Task<MapTaskState> {

    private final Stage stage;
    private final Partition partition;

    public ShuffleMapTask(
            Stage stage,
            Partition partition) {
        this.stage = stage;
        this.partition = partition;
    }

    @Override
    public long getTaskId() {
        return 0;
    }

    @Override
    public MapTaskState runTask(TaskContext taskContext) {
        stage.compute(partition, taskContext);

        //throw new UnsupportedOperationException();
        return null;
    }
}
