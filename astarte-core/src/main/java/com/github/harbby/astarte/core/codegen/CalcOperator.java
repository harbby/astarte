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
package com.github.harbby.astarte.core.codegen;

import com.github.harbby.astarte.core.Partitioner;
import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.api.Constant;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.operator.Operator;
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.collection.MutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public abstract class CalcOperator<I, O>
        extends Operator<O>
{
    private final boolean holdPartitioner;
    private final Operator<I> dataSet;
    private final boolean enableCodeGen;

    public CalcOperator(Operator<I> dataSet, boolean holdPartitioner)
    {
        super(dataSet.getContext());
        this.dataSet = unboxing(dataSet);
        this.holdPartitioner = holdPartitioner;
        this.enableCodeGen = context.getConf().getBoolean(Constant.CALC_OPERATOR_CODE_GENERATION_ENABLE, false);
    }

    public abstract Object getOperator();

    protected abstract Iterator<O> doCompute(Iterator<I> iterator);

    public final boolean holdPartitioner()
    {
        return holdPartitioner;
    }

    @Override
    public List<? extends Operator<?>> getDependencies()
    {
        return ImmutableList.of(dataSet);
    }

    @Override
    public final Partitioner getPartitioner()
    {
        if (holdPartitioner) {
            return dataSet.getPartitioner();
        }
        return null;
    }

    private Iterator<?> codeGenPlan(Partition partition, TaskContext taskContext, List<CalcOperator<?, ?>> operators)
    {
        operators.add(this);
        if (dataSet instanceof CalcOperator && !dataSet.isMarkedCache()) {
            return ((CalcOperator<?, ?>) dataSet).codeGenPlan(partition, taskContext, operators);
        }
        else {
            if (operators.size() == 1) {
                return this.doCompute(dataSet.computeOrCache(partition, taskContext));
            }
            checkState(operators.size() > 1);
            //code gen
            List<CalcOperator<?, ?>> list = new ArrayList<>(operators);
            Collections.reverse(list);
            return CodeGenUtil.doCodeGen(dataSet.computeOrCache(partition, taskContext), list);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<O> compute(Partition partition, TaskContext taskContext)
    {
        if (enableCodeGen && dataSet instanceof CalcOperator && !dataSet.isMarkedCache()) {
            return (Iterator<O>) ((CalcOperator<?, ?>) dataSet).codeGenPlan(partition, taskContext, MutableList.of(this));
        }
        return doCompute(dataSet.computeOrCache(partition, taskContext));
    }
}
