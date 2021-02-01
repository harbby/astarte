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

import com.github.harbby.astarte.submit.JobArgsOptionParser;
import com.github.harbby.astarte.submit.JobDeployClient;
import com.github.harbby.gadtry.base.Files;
import com.github.harbby.gadtry.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.github.harbby.astarte.yarn.batch.YarnConstant.APP_CACHE_DIR;
import static com.github.harbby.astarte.yarn.batch.YarnConstant.APP_CACHE_FILES;
import static com.github.harbby.astarte.yarn.batch.YarnConstant.APP_CLASSPATH;
import static com.github.harbby.astarte.yarn.batch.YarnConstant.APP_MAIN_CLASS;
import static com.github.harbby.astarte.yarn.batch.YarnConstant.EXECUTOR_MEMORY;
import static com.github.harbby.astarte.yarn.batch.YarnConstant.EXECUTOR_NUMBERS;
import static com.github.harbby.astarte.yarn.batch.YarnConstant.EXECUTOR_VCORES;
import static java.util.Objects.requireNonNull;

public class YarnDeployClient
        implements JobDeployClient
{
    private static final Logger logger = LoggerFactory.getLogger(YarnDeployClient.class);
    public static final String YARN_APP_TYPE = "Astarte";
    private static final String YARN_CLUSTER_MODE = "yarn-cluster";
    public static final String ASTARTE_LIB_ARCHIVE = "__astarte_libs__";
    public static final String ASTARTE_LIB_NAME = "__jars__";
    public static final String ASTARTE_LIB_CLASSPATH = ASTARTE_LIB_NAME + "/*:" + ASTARTE_LIB_NAME + "/hadoop/*";

    @Override
    public String registerModeName()
    {
        return YARN_CLUSTER_MODE;
    }

    @Override
    public void deploy(JobArgsOptionParser jobConf)
            throws Exception
    {
        String[] args = jobConf.getUserArgs();
        Configuration configuration = loadHadoopConfig();

        try (YarnClient yarnClient = initYarnClient(configuration)) {
            FileSystem fs = FileSystem.get(configuration);
            final YarnClientApplication yarnApplication = yarnClient.createApplication();
            ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
            ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();

            Path workingDir = new Path(fs.getWorkingDirectory(), ".astarte/" + appContext.getApplicationId());
            //upload user other jars
            FileUploader fileUploader = new FileUploader(fs, workingDir);
            File jarsArchive = prepareArchive("jars");
            try {
                fileUploader.uploadArchive(ASTARTE_LIB_NAME, jarsArchive.toURI(), LocalResourceVisibility.PRIVATE);
            }
            finally {
                logger.info("clear archive files {} {}", jarsArchive, jarsArchive.delete());
            }
            fileUploader.upload(prepareConfFile());
            fileUploader.upload(Collections.singletonList(jobConf.getMainClassJar().toURI()));
            fileUploader.upload(jobConf.getJars());
            fileUploader.upload(jobConf.getFiles());

            Map<String, LocalResource> uploadResource = fileUploader.getResources();
            amContainer.setLocalResources(uploadResource);
            amContainer.setCommands(createAmCommand(args));

            // Setup CLASSPATH and environment variables for ApplicationMaster
            Map<String, String> appMasterEnv = createAmEnv(jobConf, uploadResource);
            appMasterEnv.put(APP_CACHE_DIR, workingDir.toUri().toString());
            amContainer.setEnvironment(appMasterEnv);

            Resource capability = Records.newRecord(Resource.class);
            capability.setMemorySize(jobConf.getDriverMemoryMb());
            capability.setVirtualCores(jobConf.getDriverVcores());

            appContext.setResource(capability);
            appContext.setApplicationType(YARN_APP_TYPE);
            appContext.setMaxAppAttempts(2);
            appContext.setApplicationName(jobConf.getJobName());
            appContext.setAMContainerSpec(amContainer);

            try {
                yarnClient.submitApplication(appContext);
                waitJobRunning(yarnClient, appContext.getApplicationId());
            }
            catch (Exception e) {
                clearCacheDir(fs, workingDir);
                throw e;
            }
        }
    }

    private static final class FileUploader
    {
        private final Map<String, LocalResource> resources = new HashMap<>();
        private final FileSystem fs;
        private final Path workingDir;

        public FileUploader(FileSystem fs, Path workingDir)
        {
            this.fs = fs;
            this.workingDir = workingDir;
        }

        public void upload(List<URI> files)
                throws IOException
        {
            for (URI uri : files) {
                this.upload(uri);
            }
        }

        public void upload(URI uri)
                throws IOException
        {
            this.upload(null, uri, LocalResourceType.FILE, LocalResourceVisibility.APPLICATION);
        }

        public void uploadArchive(String name, URI uri, LocalResourceVisibility visibility)
                throws IOException
        {
            upload(name, uri, LocalResourceType.ARCHIVE, visibility);
        }

        public void upload(
                String name,
                URI uri,
                LocalResourceType resourceType,
                LocalResourceVisibility visibility)
                throws IOException
        {
            if (uri.getScheme() == null || "file".equals(uri.getScheme())) {
                File file = new File(uri.getPath());
                Path dst = new Path(workingDir, file.getName());
                logger.info("upload {} to {}", file.getName(), dst);
                fs.copyFromLocalFile(new Path(file.getPath()), dst);
                LocalResource localResource = createLocalResource(fs, dst, resourceType, visibility);
                resources.put(Optional.ofNullable(name).orElse(file.getName()), localResource);
            }
            else if ("hdfs".equals(uri.getScheme())) {
                Path path = new Path(uri);
                LocalResource localResource = createLocalResource(fs, path, resourceType, visibility);
                resources.put(Optional.ofNullable(name).orElse(path.getName()), localResource);
            }
            else {
                throw new UnsupportedOperationException(uri.toString());
            }
        }

        public Map<String, LocalResource> getResources()
        {
            return new HashMap<>(resources);
        }
    }

    private static List<URI> prepareConfFile()
    {
        String astarteHome = requireNonNull(System.getenv("ASTARTE_HOME"));
        return Files.listFiles(new File(astarteHome, "conf"), false)
                .stream()
                .map(File::toURI)
                .collect(Collectors.toList());
    }

    private static File prepareArchive(String dir)
            throws IOException
    {
        String astarteHome = requireNonNull(System.getenv("ASTARTE_HOME"));
        File rootDir = new File(astarteHome, dir);
        if (!rootDir.exists()) {
            throw new NoSuchFileException("$ASTARTE_HOME/" + dir);
        }
        File jarsArchive = File.createTempFile(ASTARTE_LIB_ARCHIVE, ".zip", new File("/tmp"));
        try (ZipOutputStream jarsStream = new ZipOutputStream(new FileOutputStream(jarsArchive))) {
            jarsStream.setLevel(0);
            for (File file : Files.listFiles(rootDir, true)) {
                String name = file.getPath().substring(rootDir.getPath().length() + 1);
                jarsStream.putNextEntry(new ZipEntry(name));
                IOUtils.copyBytes(new FileInputStream(file), jarsStream, 4096);
                jarsStream.closeEntry();
            }
        }
        return jarsArchive;
    }

    private void clearCacheDir(FileSystem fs, Path workingDir)
    {
        try {
            fs.delete(workingDir, true);
        }
        catch (IOException e) {
            logger.error("clear cache dir failed", e);
        }
    }

    private void waitJobRunning(YarnClient yarnClient, ApplicationId applicationId)
            throws InterruptedException, IOException, YarnException
    {
        long startTime = System.currentTimeMillis();
        while (true) {
            ApplicationReport applicationReport = yarnClient.getApplicationReport(applicationId);
            YarnApplicationState state = applicationReport.getYarnApplicationState();

            logger.info("Application {} State: {}", applicationId, state);
            switch (state) {
                case RUNNING:
                    logger.info("YARN application {} deployed successfully.", applicationId);
                    return;
                case FAILED:
                    throw new YarnException("Application " + applicationId + " State FAILED: " +
                            applicationReport.getDiagnostics() + ", use yarn logs -applicationId " + applicationId);
                case KILLED:
                case FINISHED:
                    return;
                default:
                    if (System.currentTimeMillis() - startTime > 60_000) {
                        logger.info("Deployment took more than 60 seconds. Please check if the requested resources are available in the YARN cluster");
                    }
            }
            TimeUnit.MILLISECONDS.sleep(1000);
        }
    }

    private static Map<String, String> createAmEnv(JobArgsOptionParser jobConf, Map<String, LocalResource> uploadResource)
            throws URISyntaxException
    {
        Map<String, String> appMasterEnv = new HashMap<>();
        appMasterEnv.put(APP_MAIN_CLASS, jobConf.getMainClass().getName());
        appMasterEnv.put(APP_CLASSPATH, ASTARTE_LIB_CLASSPATH + ":" +
                uploadResource.keySet().stream().filter(x -> !ASTARTE_LIB_NAME.equals(x)).collect(Collectors.joining(File.pathSeparator)));

        appMasterEnv.put(EXECUTOR_VCORES, String.valueOf(jobConf.getExecutorVcores()));
        appMasterEnv.put(EXECUTOR_NUMBERS, String.valueOf(jobConf.getExecutorNumber()));
        appMasterEnv.put(EXECUTOR_MEMORY, String.valueOf(jobConf.getExecutorMemoryMb()));

        StringBuilder cacheFiles = new StringBuilder();
        for (Map.Entry<String, LocalResource> entry : uploadResource.entrySet()) {
            LocalResource localResource = entry.getValue();
            cacheFiles.append(File.pathSeparator).append(localResource.getResource().toPath().getName())
                    .append(",").append(localResource.getSize())
                    .append(",").append(localResource.getTimestamp())
                    .append(",").append(localResource.getType().name())
                    .append(",").append(localResource.getVisibility().name());
        }
        appMasterEnv.put(APP_CACHE_FILES, cacheFiles.substring(1));
        return appMasterEnv;
    }

    private static LocalResource createLocalResource(FileSystem fs, Path remoteDstPath, LocalResourceType resourceType, LocalResourceVisibility visibility)
            throws IOException
    {
        FileStatus fileStatus = fs.getFileStatus(remoteDstPath);
        LocalResource localResource = Records.newRecord(LocalResource.class);
        localResource.setResource(URL.fromURI(remoteDstPath.toUri()));
        localResource.setSize(fileStatus.getLen());
        localResource.setTimestamp(fileStatus.getModificationTime());
        localResource.setType(resourceType);
        localResource.setVisibility(visibility);
        return localResource;
    }

    private static List<String> createAmCommand(String[] args)
    {
        final String amCommand = "$JAVA_HOME/bin/java -server " +
                "-Djava.io.tmpdir=$PWD/tmp " +
                "-Dlogback.configurationFile=file:logback.xml " +
                "-Dlog4j.configuration=file:log4j.properties " +
                AstarteYarnApplication.class.getName() + " " + String.join(" ", args);

        logger.info("amCommand {}", amCommand);
        return Collections.singletonList(amCommand);
    }

    private static YarnClient initYarnClient(Configuration configuration)
    {
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(configuration);
        yarnClient.start();
        return yarnClient;
    }

    private static Configuration loadHadoopConfig()
    {
        YarnConfiguration hadoopConf = new YarnConfiguration();
        String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
        if (hadoopConfDir == null) {
            return hadoopConf;
        }
        //---create hadoop conf
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        Stream.of("yarn-site.xml", "core-site.xml", "hdfs-site.xml").forEach(file -> {
            File site = new File(hadoopConfDir, file);
            if (site.exists() && site.isFile()) {
                hadoopConf.addResource(new org.apache.hadoop.fs.Path(site.toURI()));
            }
        });
        return hadoopConf;
    }
}
