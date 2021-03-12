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

import com.github.harbby.astarte.core.JobScheduler;
import com.github.harbby.astarte.core.runtime.ClusterScheduler;
import com.github.harbby.astarte.core.runtime.ExecutorManager;
import com.github.harbby.gadtry.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.github.harbby.astarte.yarn.batch.YarnConstant.APP_CACHE_DIR;
import static com.github.harbby.astarte.yarn.batch.YarnConstant.APP_MAIN_CLASS;
import static com.github.harbby.astarte.yarn.batch.YarnConstant.EXECUTOR_NUMBERS;
import static com.github.harbby.astarte.yarn.batch.YarnConstant.EXECUTOR_VCORES;

public class AstarteYarnApplication
{
    private static final Logger logger = LoggerFactory.getLogger(AstarteYarnApplication.class);

    private AstarteYarnApplication() {}

    public static void main(String[] args)
            throws Exception
    {
        logger.info("this is Astarte Yarn Driver");
        logger.info("properties: {}", System.getProperties());

        Map<String, String> envs = System.getenv();

        String containerIdString =
                envs.get(ApplicationConstants.Environment.CONTAINER_ID.key());
        if (containerIdString == null) {
            // container id should always be set in the env by the framework
            throw new IllegalArgumentException(
                    "ContainerId not set in the environment");
        }
        ContainerId containerId = ContainerId.fromString(containerIdString); // ConverterUtils.toContainerId(containerIdString)
        String hadoopConf = envs.get(ApplicationConstants.Environment.HADOOP_CONF_DIR.key());
        Configuration yarnConfiguration = getHadoopConf(hadoopConf);

        try (AMRMClient<AMRMClient.ContainerRequest> rmClient = AMRMClient.createAMRMClient()) {
            rmClient.init(yarnConfiguration);
            rmClient.start();

            rmClient.registerApplicationMaster("master", 7239, null); //注册driver的web页面
            //invoke user main class
            Class<?> mainClass = Class.forName(System.getenv(APP_MAIN_CLASS));
            Future<?> future = Executors.newSingleThreadExecutor().submit(() -> {
                while (true) {
                    rmClient.allocate(0);
                    logger.debug("yarn resource manager allocate(0)");
                    TimeUnit.SECONDS.sleep(10);
                }
            });
            ExecutorManager.setFactory((vcores, memMb, executorNum, driverManagerAddress) ->
                    new YarnExecutorManager(rmClient, vcores, memMb, executorNum, driverManagerAddress));
            try {
                logger.info("init... BatchContext");
                JobScheduler.setFactory((conf) -> new ClusterScheduler(conf,
                        Integer.parseInt(System.getenv(EXECUTOR_VCORES)),
                        Integer.parseInt(System.getenv(EXECUTOR_NUMBERS))
                ));
                logger.info("invoke... user mainClass {}", mainClass);
                mainClass.getMethod("main", String[].class)
                        .invoke(null, (Object) args);
                future.cancel(true);
                rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, null);
                clearCacheDir(new Path(URI.create(System.getenv(APP_CACHE_DIR))), yarnConfiguration);
                System.exit(0);
            }
            catch (Exception e) {
                String errorMsg = Throwables.getStackTraceAsString(e);
                future.cancel(true);
                rmClient.unregisterApplicationMaster(FinalApplicationStatus.FAILED, errorMsg, null);
                logger.error("", e);
                System.exit(-1);
            }
        }
    }

    private static void clearCacheDir(Path cacheDir, Configuration hadoopConf)
    {
        try {
            FileSystem fs = FileSystem.get(hadoopConf);
            fs.delete(cacheDir, true);
        }
        catch (IOException e) {
            logger.error("clear cache dir failed", e);
        }
    }

    private static Configuration getHadoopConf(String hadoopConfDir)
            throws NoSuchFileException
    {
        Configuration hadoopConf = new Configuration();
        //System.setProperty("HADOOP_USER_NAME", hdfsUser);
        System.setProperty("HADOOP_CONF_DIR", hadoopConfDir);
        //---create hadoop conf
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        for (String file : Arrays.asList("yarn-site.xml", "core-site.xml", "hdfs-site.xml")) {
            File site = new File(hadoopConfDir, file);
            if (site.exists() && site.isFile()) {
                hadoopConf.addResource(new org.apache.hadoop.fs.Path(site.toURI()));
            }
            else {
                throw new NoSuchFileException(site + " not exists");
            }
        }
        return hadoopConf;
    }
}
