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

import com.github.harbby.astarte.core.api.Constant;
import com.github.harbby.astarte.core.runtime.ExecutorManager;
import com.github.harbby.astarte.core.runtime.TaskExecutor;
import com.github.harbby.gadtry.base.Throwables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.harbby.astarte.yarn.batch.YarnConstant.APP_CACHE_DIR;
import static com.github.harbby.astarte.yarn.batch.YarnConstant.APP_CACHE_FILES;
import static com.github.harbby.astarte.yarn.batch.YarnConstant.APP_CLASSPATH;
import static com.github.harbby.astarte.yarn.batch.YarnDeployClient.ASTARTE_LIB_ARCHIVE;
import static com.github.harbby.astarte.yarn.batch.YarnDeployClient.ASTARTE_LIB_NAME;
import static java.util.Objects.requireNonNull;

public class YarnExecutorManager
        extends ExecutorManager
{
    private static final Logger logger = LoggerFactory.getLogger(YarnExecutorManager.class);
    private final AMRMClient<AMRMClient.ContainerRequest> rmClient;
    private NMClient nmClient;
    private final SocketAddress driverManagerAddress;

    public YarnExecutorManager(
            AMRMClient<AMRMClient.ContainerRequest> rmClient,
            int vcores,
            int memMb,
            int executorNum,
            SocketAddress driverManagerAddress)
    {
        super(vcores, memMb, executorNum);
        this.driverManagerAddress = driverManagerAddress;
        this.rmClient = rmClient;
    }

    @Override
    public void start()
    {
        this.nmClient = NMClient.createNMClient();
        try {
            nmClient.init(rmClient.getConfig());
            nmClient.start();
            this.createYarnContainer();
        }
        catch (IOException | InterruptedException | YarnException e) {
            throw Throwables.throwThrowable(e);
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
        final String amCommand = "$JAVA_HOME/bin/java" +
                " -Dlogback.configurationFile=file:logback.xml " +
                " -Dlog4j.configuration=file:log4j.properties " +
                TaskExecutor.class.getName();
        logger.info("executor Command {}", amCommand);
        amContainer.setCommands(Collections.singletonList(amCommand));
        final Map<String, String> executorEnv = new HashMap<>();
        executorEnv.put(APP_CLASSPATH, System.getProperty("java.class.path"));
        executorEnv.put(Constant.DRIVER_SCHEDULER_ADDRESS, driverManagerAddress.toString());
        amContainer.setEnvironment(executorEnv);
        amContainer.setLocalResources(propreLocalResources());

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
                nmClient.startContainer(container, amContainer);
                logger.info("Container {} started", container);
                allocate++;
            }
        }
    }

    private Map<String, LocalResource> propreLocalResources()
    {
        String cacheFiles = requireNonNull(System.getenv(APP_CACHE_FILES));
        String cacheDir = requireNonNull(System.getenv(APP_CACHE_DIR));
        Map<String, LocalResource> resources = new HashMap<>();
        for (String file : cacheFiles.split(File.pathSeparator)) {
            String[] split = file.split(",");
            Path cacheFile = new Path(cacheDir, split[0]);

            LocalResource localResource = Records.newRecord(LocalResource.class);
            localResource.setResource(URL.fromPath(cacheFile));
            localResource.setSize(Long.parseLong(split[1]));
            localResource.setTimestamp(Long.parseLong(split[2]));
            localResource.setType(LocalResourceType.valueOf(split[3]));
            localResource.setVisibility(LocalResourceVisibility.valueOf(split[4]));
            String name = cacheFile.getName().startsWith(ASTARTE_LIB_ARCHIVE) ? ASTARTE_LIB_NAME : cacheFile.getName();
            resources.put(name, localResource);
        }
        return resources;
    }
}
