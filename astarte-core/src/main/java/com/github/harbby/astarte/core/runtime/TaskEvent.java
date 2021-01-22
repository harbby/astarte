/*
 * Copyright (C) 2018 The Astarte Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
