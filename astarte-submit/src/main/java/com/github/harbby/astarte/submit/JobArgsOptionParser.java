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
package com.github.harbby.astarte.submit;

import com.github.harbby.gadtry.base.Files;
import com.github.harbby.gadtry.base.Platform;
import com.github.harbby.gadtry.base.Strings;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

public class JobArgsOptionParser
{
    private static final Logger logger = LoggerFactory.getLogger(JobArgsOptionParser.class);
    private static final Options options = new Options();

    private final CommandLine commandLine;
    private final Class<?> mainClass;
    private final File mainJar;
    private final String[] userArgs;
    private final List<URI> userJars;
    private final List<URI> userFiles;

    static {
        options.addOption("h", "help", false, "Print this usage information");

        options.addOption(null, "name", true, "--name job name");
        options.addOption(null, "class", true, "--class main class not set");
        options.addOption(null, "mode", true, "local[?] or yarn-cluster or k8s");
        options.addOption(null, "vcores", true, "the cluster mode executor vcores(slots), default: 2");
        options.addOption(null, "executors", true, "the cluster mode executor number, default: 2");
        options.addOption("em", "executor_memory", true, "the cluster mode executor memory(MB), default: 1024MB");
        options.addOption("dm", "driver_memory", true, "the cluster mode driver memory(MB), default: 1024MB");

        options.addOption(null, "jars", true, "user other jars");
        options.addOption(null, "files", true, "user other files");
    }

    public JobArgsOptionParser(String[] args)
            throws Exception
    {
        logger.info("parser... job args: [{}] ", args);
        this.commandLine = new DefaultParser().parse(options, args, false);
        if (commandLine.hasOption('h')) {
            System.out.println("Help Astarte: ");
            options.getOptions().forEach(x -> System.out.println(x.toString()));
            System.exit(0);
        }
        checkState(commandLine.getArgs().length >= 1, "must set main class jar");
        List<String> fullArgs = commandLine.getArgList().stream()
                .filter(Strings::isNotBlank).collect(Collectors.toList());
        this.mainJar = new File(fullArgs.get(0));
        Platform.loadExtJarToSystemClassLoader(Collections.singletonList(mainJar.toURI().toURL()));

        String mainClassString = requireNonNull(commandLine.getOptionValue("class"), "Missing required option: --class");
        this.mainClass = Class.forName(mainClassString);
        logger.info("mainClass: {}", mainClass);

        checkState(mainJar.isFile() && mainJar.exists(), "not such file " + mainJar);
        this.userArgs = fullArgs.subList(1, fullArgs.size()).toArray(new String[0]);
        this.userJars = findFiles(commandLine.getOptionValue("jars"));
        this.userFiles = findFiles(commandLine.getOptionValue("files"));
    }

    public String getJobName()
    {
        return commandLine.getOptionValue("name", "astarte-job");
    }

    public String getMode()
    {
        return requireNonNull(commandLine.getOptionValue("mode"), "Missing required option: --mode");
    }

    /**
     * support hdfs://...jar
     *
     * @return user job other dep jars
     */
    public List<URI> getJars()
    {
        return this.userJars;
    }

    /**
     * support hdfs://...file
     *
     * @return user job other dep files
     */
    public List<URI> getFiles()
    {
        return this.userFiles;
    }

    public File getMainClassJar()
    {
        return mainJar;
    }

    public int getExecutorVcores()
    {
        return Integer.parseInt(commandLine.getOptionValue("vcores", "2"));
    }

    public int getExecutorMemoryMb()
    {
        return Integer.parseInt(commandLine.getOptionValue("executor_memory", "1024"));
    }

    public int getExecutorNumber()
    {
        return Integer.parseInt(commandLine.getOptionValue("executors", "2"));
    }

    public int getDriverMemoryMb()
    {
        return Integer.parseInt(commandLine.getOptionValue("driver_memory", "1024"));
    }

    public int getDriverVcores()
    {
        return Integer.parseInt(commandLine.getOptionValue("driver_vcores", "1"));
    }

    public Class<?> getMainClass()
    {
        return mainClass;
    }

    public String[] getUserArgs()
    {
        return userArgs;
    }

    private static List<URI> findFiles(String path)
            throws URISyntaxException
    {
        if (path == null) {
            return Collections.emptyList();
        }
        List<URI> jars = new ArrayList<>();
        for (String x : path.split(File.pathSeparator)) {
            if (Strings.isBlank(x)) {
                continue;
            }
            URI uri = new URI(x);
            if (uri.getScheme() == null || "file".equals(uri.getScheme())) {
                File file = new File(uri.getPath());
                checkState(file.exists(), "no such file " + file);
                for (File findFile : Files.listFiles(file, false)) {
                    jars.add(findFile.toURI());
                }
            }
            else {
                throw new UnsupportedOperationException();
            }
        }
        return jars;
    }
}
