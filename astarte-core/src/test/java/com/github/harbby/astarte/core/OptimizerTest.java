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
package com.github.harbby.astarte.core;

import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.astarte.core.api.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class OptimizerTest
{
    private final BatchContext mppContext = BatchContext.builder()
            .local(2)
            .getOrCreate();

    @Test
    public void optimize1est()
    {
        KvDataSet<String, Integer> ageDs = mppContext.makeKvDataSet(Collections.singletonList(
                Tuple2.of("hp", 18)
        )).reduceByKey(Integer::sum);

        List<Tuple2<String, Tuple2<Integer, Integer>>> data = ageDs
                //.mapKeys(x -> x + 1)  //todo: find bug
                .join(ageDs)
                .collect();
        Assert.assertEquals(data,
                Collections.singletonList(Tuple2.of("hp", Tuple2.of(18, 18))));
    }
}
