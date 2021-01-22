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
package com.github.harbby.astarte.core.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AstarteYarnAppDriver
{
    private AstarteYarnAppDriver() {}

    private static final Logger logger = LoggerFactory.getLogger(AstarteYarnAppDriver.class);

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
        ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();
        String hadoopConf = envs.get(ApplicationConstants.Environment.HADOOP_CONF_DIR.key());
        Configuration yarnConfiguration = getHadoopConf(hadoopConf);

        try (AMRMClient<AMRMClient.ContainerRequest> resourceManagerClient = AMRMClient.createAMRMClient()) {
            resourceManagerClient.init(yarnConfiguration);
            resourceManagerClient.start();

            resourceManagerClient.registerApplicationMaster("master", 7239, null); //注册driver的web页面
            //invoke user main class

            for (int i = 0; i < 10; i++) {
                resourceManagerClient.allocate(0);
                System.out.println("TimeUnit.SECONDS.sleep(10) this time " + new Date());
                TimeUnit.SECONDS.sleep(10);
            }

            resourceManagerClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, null);
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
