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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public interface TaskEvent
        extends Event
{
    public static TaskEvent failed(int jobId, int taskId, String error)
    {
        return new TaskFailed(jobId, taskId, error);
    }

    public static TaskEvent success(int jobId, int taskId, Object result)
    {
        return new TaskSuccess(jobId, taskId, result);
    }

    int getJobId();

    public int getTaskId();

    public static class TaskFailed
            implements TaskEvent
    {
        private int jobId;
        private int taskId;
        private String error;

        public TaskFailed(int jobId, int taskId, String error)
        {
            this.jobId = jobId;
            this.taskId = taskId;
            this.error = error;
        }

        public TaskFailed() {}

        @Override
        public int getJobId()
        {
            return jobId;
        }

        public String getError()
        {
            return error;
        }

        @Override
        public int getTaskId()
        {
            return taskId;
        }

        @Override
        public void writeExternal(ObjectOutput out)
                throws IOException
        {
            out.writeInt(jobId);
            out.writeInt(taskId);
            out.writeUTF(error);
        }

        @Override
        public void readExternal(ObjectInput in)
                throws IOException, ClassNotFoundException
        {
            this.jobId = in.readInt();
            this.taskId = in.readInt();
            this.error = in.readUTF();
        }
    }

    public static class TaskSuccess
            implements TaskEvent
    {
        private int jobId;
        private int taskId;
        private Object result;

        public TaskSuccess(int jobId, int taskId, Object result)
        {
            this.jobId = jobId;
            this.taskId = taskId;
            //check result serializable
            this.result = result;
        }

        public TaskSuccess() {}

        @Override
        public int getTaskId()
        {
            return taskId;
        }

        public Object getTaskResult()
        {
            return result;
        }

        @Override
        public int getJobId()
        {
            return jobId;
        }

        @Override
        public void writeExternal(ObjectOutput out)
                throws IOException
        {
            out.writeInt(jobId);
            out.writeInt(taskId);
            out.writeObject(result);
        }

        @Override
        public void readExternal(ObjectInput in)
                throws IOException, ClassNotFoundException
        {
            this.jobId = in.readInt();
            this.taskId = in.readInt();
            this.result = in.readObject();
        }
    }
}
