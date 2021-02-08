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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LocalNettyExecutorManager
        extends ExecutorManager
{
    private static final Logger logger = LoggerFactory.getLogger(LocalNettyExecutorManager.class);
    private final ExecutorService pool;

    public LocalNettyExecutorManager(int vcores, int executorMem, int executorNum)
    {
        super(vcores, executorMem, executorNum);
        this.pool = Executors.newFixedThreadPool(executorNum);
    }

    @Override
    public void start()
    {
        pool.submit(() -> {
            try (Executor executor = new Executor(getVcores())) {
                executor.join();
            }
            catch (InterruptedException e) {
                logger.info("local executor closing...");
            }
            catch (Exception e) {
                logger.error("executor running failed ", e);
            }
        });
    }

    @Override
    public void stop()
    {
        pool.shutdownNow();
    }
}
