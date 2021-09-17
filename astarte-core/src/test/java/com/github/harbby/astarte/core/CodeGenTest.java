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

import com.github.harbby.astarte.core.api.AstarteConf;
import com.github.harbby.astarte.core.api.Constant;
import com.github.harbby.astarte.core.api.DataSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CodeGenTest
{
    @Test
    public void map3Test()
    {
        final AstarteConf astarteConf = new AstarteConf();
        astarteConf.put(Constant.CALC_OPERATOR_CODE_GENERATION_ENABLE, "true");
        final BatchContext mppContext = BatchContext.builder()
                .conf(astarteConf)
                .local(2)
                .getOrCreate();

        DataSet<String> ds = mppContext.makeDataSet(Arrays.asList("a",
                "b",
                "c",
                "d",
                "e"), 1);
        List<String> out = ds.map(x -> x.charAt(0)).map(x -> x + 1).filter(x -> x > 0).map(x -> (char) x.intValue()).map(String::valueOf)
                .collect();
        Assert.assertEquals(out, Arrays.asList("b", "c", "d", "e", "f"));
    }
}
