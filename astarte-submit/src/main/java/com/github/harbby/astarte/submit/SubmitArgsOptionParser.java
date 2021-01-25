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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class SubmitArgsOptionParser
{
    private final CommandLine commandLine;

    public SubmitArgsOptionParser(String[] args)
            throws ParseException
    {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("h", "help", false, "Print this usage information");

        options.addOption(null, "jars", false, "user other jars");
        options.addOption(null, "files", false, "user other files");
        CommandLine commandLine = parser.parse(options, args);
        this.commandLine = commandLine;

        if (commandLine.hasOption('h')) {
            System.out.println("Help Astarte");
            System.exit(0);
        }
    }
}
