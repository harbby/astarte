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
package com.github.harbby.astarte.yarn.batch;

import com.github.harbby.astarte.core.runtime.ExecutorManager;
import com.github.harbby.astarte.core.runtime.TaskExecutor;
import com.github.harbby.gadtry.base.Throwables;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class YarnExecutorManager
        extends ExecutorManager
{
    private static final Logger logger = LoggerFactory.getLogger(AstarteYarnAppDriver.class);
    private final AMRMClient<AMRMClient.ContainerRequest> rmClient;
    private NMClientAsync nmClient;

    public YarnExecutorManager(
            AMRMClient<AMRMClient.ContainerRequest> rmClient,
            int vcores,
            int memMb,
            int executorNum)
    {
        super(vcores, memMb, executorNum);
        this.rmClient = rmClient;
    }

    @Override
    public void start()
    {
        YarnHandler yarnHandler = new YarnHandler();
        this.nmClient = NMClientAsync.createNMClientAsync(yarnHandler);
        try {
            nmClient.init(rmClient.getConfig());
            nmClient.start();

            this.createYarnContainer();
        }
        catch (IOException | InterruptedException | YarnException e) {
            throw Throwables.throwsThrowable(e);
        }
    }

    @Override
    public void stop()
    {
        try {
            nmClient.close();
        }
        catch (IOException e) {
            logger.warn("", e);
        }
        try {
            rmClient.close();
        }
        catch (IOException e) {
            logger.warn("", e);
        }
    }

    private void createYarnContainer()
            throws IOException, YarnException, InterruptedException
    {
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        String javaLibPath = System.getProperty("java.library.path");
        final String amCommand = "/ideal/lib/jdk8/bin/java -cp ./:" +
                System.getProperty("java.class.path") +
                " -Djava.library.path=" + javaLibPath +
                " -Dlogback.configurationFile=/ideal/workspce/github/astarte/conf/logback.xml " +
                TaskExecutor.class.getName();
        logger.info("amCommand {}", amCommand);
        amContainer.setCommands(Collections.singletonList(amCommand));

        for (int i = 0; i < getExecutorNum(); i++) {
            Resource capability = Records.newRecord(Resource.class);
            capability.setMemorySize(getExecutorMem());
            capability.setVirtualCores(getVcores());
            AMRMClient.ContainerRequest request = AMRMClient.ContainerRequest.newBuilder()
                    .capability(capability)
                    .priority(Priority.newInstance(1))
                    .build();
            rmClient.addContainerRequest(request);
        }

        int allocate = 0;
        while (allocate < getExecutorNum()) {
            List<Container> containers = rmClient.allocate(0.1F).getAllocatedContainers();
            for (Container container : containers) {
                nmClient.startContainerAsync(container, amContainer);
                allocate++;
            }
        }
    }

    private static class YarnHandler
            extends NMClientAsync.AbstractCallbackHandler
    {
        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse)
        {
            logger.info("Container {} started", containerId);
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus)
        {
        }

        @Override
        public void onContainerStopped(ContainerId containerId)
        {
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t)
        {
        }

        @Override
        public void onContainerResourceIncreased(ContainerId containerId, Resource resource)
        {
        }

        @Override
        public void onContainerResourceUpdated(ContainerId containerId, Resource resource)
        {
        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable t)
        {
        }

        @Override
        public void onIncreaseContainerResourceError(ContainerId containerId, Throwable t)
        {
        }

        @Override
        public void onUpdateContainerResourceError(ContainerId containerId, Throwable t)
        {
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t)
        {
        }
    }
}
