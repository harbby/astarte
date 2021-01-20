package com.github.harbby.ashtarte.api;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.operator.Operator;

import java.io.Serializable;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.github.harbby.gadtry.base.MoreObjects.toStringHelper;

public abstract class Stage
        implements Serializable
{
    private final Operator<?> operator;
    private final int stageId;
    private final int jobId;

    private Map<Integer, Integer> deps = new HashMap<>();
    private Set<SocketAddress> shuffleServices;

    protected Stage(final Operator<?> operator, int jobId, int stageId)
    {
        this.operator = operator;
        this.jobId = jobId;
        this.stageId = stageId;
    }

    public void setDeps(Map<Integer, Integer> deps)
    {
        this.deps.putAll(deps);
    }

    public void setShuffleServices(Set<SocketAddress> shuffleServices)
    {
        this.shuffleServices = shuffleServices;
    }

    public Set<SocketAddress> getShuffleServices()
    {
        return shuffleServices;
    }

    public Map<Integer, Integer> getDeps()
    {
        return deps;
    }

    public Operator<?> getFinalOperator()
    {
        return operator;
    }

    public Partition[] getPartitions()
    {
        return operator.getPartitions();
    }

    public abstract void compute(Partition split, TaskContext taskContext);

    public int getJobId()
    {
        return jobId;
    }

    public int getStageId()
    {
        return stageId;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", stageId)
                .add("finalOperator", operator)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, stageId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        Stage other = (Stage) obj;
        return Objects.equals(this.operator, other.operator) &&
                Objects.equals(this.stageId, other.stageId);
    }

    public int getNumPartitions()
    {
        return operator.numPartitions();
    }
}
