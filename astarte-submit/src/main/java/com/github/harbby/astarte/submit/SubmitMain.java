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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

public class SubmitMain
{
    private static final Logger logger = LoggerFactory.getLogger(SubmitMain.class);

    private SubmitMain() {}

    public static void main(String[] args)
            throws Throwable
    {
        logger.info("Welcome to Astarte Submit");
        JobArgsOptionParser argsParser = new JobArgsOptionParser(args);
        Iterable<JobDeployClient> iterable = ServiceLoader.load(JobDeployClient.class);
        Map<String, JobDeployClient> clientMap = new HashMap<>();
        iterable.forEach(jobDeployClient -> {
            String name = jobDeployClient.registerModeName();
            logger.info("found deploy mode: {},{}", name, jobDeployClient.getClass());
            clientMap.put(name, jobDeployClient);
        });

        if (argsParser.getMode().startsWith("local")) {
            logger.info("mode: {}  run mainClass: {}, {}", argsParser.getMode(), argsParser.getMainClass(), argsParser.getUserArgs());
            try {
                argsParser.getMainClass().getMethod("main", String[].class)
                        .invoke(null, (Object) argsParser.getUserArgs());
            }
            catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }
        else {
            JobDeployClient jobDeployClient = requireNonNull(clientMap.get(argsParser.getMode()), argsParser.getMode() + " not found");
            logger.info("job deploying to {}", argsParser.getMode());
            jobDeployClient.deploy(argsParser);
        }
    }
}
