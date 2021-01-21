package com.github.harbby.astarte.core.runtime;

public interface TaskEvent
        extends Event
{
    public static TaskEvent failed(int jobId, String error)
    {
        return new TaskFailed(jobId, error);
    }

    public static TaskEvent success(int taskId, Object result)
    {
        return new TaskSuccess(taskId, result);
    }

    public static class TaskFailed
            implements TaskEvent
    {
        private final int jobId;
        private final String error;

        public TaskFailed(int jobId, String error)
        {
            this.jobId = jobId;
            this.error = error;
        }

        public int getJobId()
        {
            return jobId;
        }

        public String getError()
        {
            return error;
        }
    }

    public static class TaskSuccess
            implements TaskEvent
    {
        private final int taskId;
        private final Object result;

        public TaskSuccess(int taskId, Object result)
        {
            this.taskId = taskId;
            //check result serializable
            this.result = result;
        }

        public int getTaskId()
        {
            return taskId;
        }

        public Object getTaskResult()
        {
            return result;
        }
    }
}
