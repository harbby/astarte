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
package com.github.harbby.astarte.core;

import com.github.harbby.astarte.core.api.AstarteConf;
import com.github.harbby.astarte.core.api.Stage;
import com.github.harbby.astarte.core.api.function.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public abstract class JobScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(JobScheduler.class);
    private static JobScheduler.Factory factory;

    public abstract <E, R> List<R> runJob(int jobId,
            List<Stage> jobStages,
            Mapper<Iterator<E>, R> action,
            Map<Stage, Map<Integer, Integer>> stageMap);

    public void stop()
    {
    }

    public static void setFactory(JobScheduler.Factory factory)
    {
        JobScheduler.factory = factory;
    }

    public static interface Factory
    {
        public JobScheduler createJobScheduler(AstarteConf conf);
    }

    static JobScheduler createJobScheduler(AstarteConf conf)
    {
        checkState(factory != null, "not setting running mode");
        return factory.createJobScheduler(conf);
    }
}
