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

import com.github.harbby.astarte.core.BatchContext;
import com.github.harbby.astarte.core.api.DataSet;
import com.github.harbby.astarte.core.api.KvDataSet;

import java.util.Arrays;
import java.util.List;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class TestJobMainClass
{
    private TestJobMainClass() {}

    public static void main(String[] args)
    {
        BatchContext batchContext = BatchContext.builder()
                .cluster(2, 2)
                .getOrCreate();
        DataSet<String> links = batchContext.makeDataSet(Arrays.asList(
                "a",
                "b",
                "c",
                "d",
                "e"), 2);

        KvDataSet<String, Long> zipIndex = links.zipWithIndex();
        List<Long> checked = Arrays.asList(0L, 1L, 2L, 3L, 4L);
        for (int i = 0; i < 10000; i++) {
            List<Long> indexs = zipIndex.values().collect();
            checkState(checked.equals(indexs));
        }
    }
}
