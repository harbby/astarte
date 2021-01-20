package com.github.harbby.ashtarte.runtime;

public interface TaskEvent
        extends Event
{
    public static TaskEvent failed(int jobId, String error)
    {
        return new TaskFailed(jobId, error);
    }

    public static TaskEvent success(Object result)
    {
        return new TaskSuccess(result);
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
        private final Object result;

        public TaskSuccess(Object result)
        {
            //check result serializable
            this.result = result;
        }

        public Object getTaskResult()
        {
            return result;
        }
    }
}
