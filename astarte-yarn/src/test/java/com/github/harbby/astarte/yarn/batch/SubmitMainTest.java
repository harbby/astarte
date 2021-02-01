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

import com.github.harbby.astarte.submit.SubmitMain;
import org.junit.Ignore;
import org.junit.Test;

public class SubmitMainTest
{
    @Ignore
    @Test
    public void testSubmitMainHelp()
            throws Throwable
    {
        String command = "--help";
        SubmitMain.main(command.split(" "));
    }

    @Test
    public void testSubmitMainLocalMode()
            throws Throwable
    {
        String command = "--mode local[2]  " +
                " --class com.github.harbby.astarte.example.batch.UnionAllDemo" +
                " ../astarte-example/target/astarte-example-0.1.0-SNAPSHOT.jar" +
                " args1";
        SubmitMain.main(command.split(" "));
    }

    @Test
    public void testSubmitMainLocalClusterMode()
            throws Throwable
    {
        String command = "--mode local[2,2]  " +
                " --class com.github.harbby.astarte.example.batch.UnionAllDemo" +
                " ../astarte-example/target/astarte-example-0.1.0-SNAPSHOT.jar" +
                " args1";
        SubmitMain.main(command.split(" "));
    }

    @Ignore
    @Test
    public void testSubmitMainYarnCluster()
            throws Throwable
    {
        String command = "--mode yarn-cluster  " +
                " --name aestSubmitMainYarnCluster" +
                " --files ../astarte-dist/target/astarte-0.1.0-SNAPSHOT-bin/conf" +
                " --class com.github.harbby.astarte.example.batch.UnionAllDemo" +
                " ../astarte-example/target/astarte-example-0.1.0-SNAPSHOT.jar" +
                " args1";
        SubmitMain.main(command.split(" "));
    }
}
