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
package com.github.harbby.astarte.core.operator;

import com.github.harbby.astarte.core.Partitioner;
import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.api.Constant;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.function.Filter;
import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.collection.MutableList;
import com.github.harbby.gadtry.collection.tuple.Tuple3;
import com.github.harbby.gadtry.compiler.ByteClassLoader;
import com.github.harbby.gadtry.compiler.JavaClassCompiler;
import com.github.harbby.gadtry.compiler.JavaSourceObject;
import com.github.harbby.gadtry.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

public abstract class CalcOperator<I, O>
        extends Operator<O>
{
    private static final String PACKAGE_NAME = CalcOperator.class.getPackage().getName();
    private static final String CLASS_START_NAME = "CodeGenIterator$";

    private static final AtomicInteger classId = new AtomicInteger(0);
    private static final AtomicInteger fieldId = new AtomicInteger(0);
    private static final String calcCodeModel = loadCodeTemplate("./codemodel/CalcModel.java");
    private static final String flatMapCodeModel = loadCodeTemplate("./codemodel/CalcModelFlatMap.java");

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
            return doCodeGen(dataSet.computeOrCache(partition, taskContext), list);
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

    @SuppressWarnings("unchecked")
    public static <I, O> Iterator<O> doCodeGen(Iterator<I> iterator, List<CalcOperator<?, ?>> operators)
    {
        if (operators.size() == 0) {
            return (Iterator<O>) iterator;
        }
        if (operators.size() == 1) {
            CalcOperator<I, O> calcOperator = (CalcOperator<I, O>) operators.get(0);
            return calcOperator.doCompute(iterator);
        }
        ByteClassLoader classLoader = new ByteClassLoader(ClassLoader.getSystemClassLoader());
        Class<?> aClass = prepareCode(calcCodeModel, classLoader, operators);
        try {
            return (Iterator<O>) aClass.getConstructor(Iterator.class, List.class).newInstance(iterator, operators);
        }
        catch (InstantiationException | NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw Throwables.throwThrowable(e);
        }
    }

    private static String getNextField()
    {
        return "field$" + fieldId.incrementAndGet();
    }

    private static Class<?> prepareCode(String codeModel, ByteClassLoader classLoader, List<CalcOperator<?, ?>> operators)
    {
        checkState(operators.size() > 0, "do not use code gen when there is only " + operators.size() + " transform operator");
        StringBuilder builder = new StringBuilder();
        List<Tuple3<String, String, String>> fieldMapping = new ArrayList<>();
        for (int i = 0; i < operators.size(); i++) {
            CalcOperator<?, ?> calcOperator = operators.get(i);
            if (calcOperator instanceof FlatMapOperator) {
                String field = getNextField();
                fieldMapping.add(Tuple3.of(Mapper.class.getCanonicalName() + "<Object, Iterator<?>>", field, "(" + Mapper.class.getCanonicalName() + "<Object, Iterator<?>>) operators.get(" + i + ").getOperator()"));
                if (i + 1 == operators.size()) {
                    //这个分支表明当前stage最后一个算子是flatMap
                    builder.append("child = (Iterator<O>)" + field + ".map(value);\n");
                }
                else {
                    Class<?> aClass = prepareCode(flatMapCodeModel, classLoader, operators.subList(i + 1, operators.size()));
                    fieldMapping.add(Tuple3.of(FlatMapCalcBase.class.getCanonicalName() + "<O>", "flatMapCalcBase",
                            CalcOperator.class.getSimpleName() + ".flatMapBaseNewInstance(this.getClass().getClassLoader(), \"" + aClass.getCanonicalName() + "\", operators.subList(" + (i + 1) + ", operators.size()));"));
                    builder.append("child = flatMapCalcBase.begin(" + field + ".map(value));\n");
                }
                builder.append("if (child.hasNext()) {\n" +
                        "    option.update(child.next());\n" +
                        "    return true;\n" +
                        "}");
                String classCode = codeModel.replace("$calcCode", builder.toString());
                return doCompute(classLoader, classCode, fieldMapping);
            }

            if (calcOperator instanceof MapOperator) {
                String field = getNextField();
                fieldMapping.add(Tuple3.of(Mapper.class.getCanonicalName() + "<Object, ?>", field, "(" + Mapper.class.getCanonicalName() + "<Object, ?>) operators.get(" + i + ").getOperator()"));
                builder.append("value = " + field + ".map(value);\n");
            }
            else if (calcOperator instanceof FilterOperator) {
                String field = getNextField();
                fieldMapping.add(Tuple3.of(Filter.class.getCanonicalName() + "<Object>", field, "(" + Filter.class.getCanonicalName() + "<Object>) operators.get(" + i + ").getOperator()"));
                builder.append("if (!" + field + ".filter(value)) {\n" +
                        "   continue;\n" +
                        "   }\n");
            }
            else {
                throw new UnsupportedOperationException();
            }
        }
        builder.append("option.update((O) value);\n");
        builder.append("return true;\n");
        String classCode = codeModel.replace("$calcCode", builder.toString());
        return doCompute(classLoader, classCode, fieldMapping);
    }

    public abstract static class FlatMapCalcBase<O>
            implements Iterator<O>
    {
        public abstract Iterator<O> begin(Iterator<?> childIterator);
    }

    @SuppressWarnings("unchecked")
    public static <O> FlatMapCalcBase<O> flatMapBaseNewInstance(ClassLoader classLoader, String className, List<CalcOperator<?, ?>> operators)
    {
        try {
            Class<?> aClass = classLoader.loadClass(className);
            checkState(FlatMapCalcBase.class.isAssignableFrom(aClass));
            return (FlatMapCalcBase<O>) aClass.getConstructor(List.class).newInstance(operators);
        }
        catch (InstantiationException | ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw Throwables.throwThrowable(e);
        }
        catch (InvocationTargetException e) {
            throw Throwables.throwThrowable(e.getTargetException());
        }
    }

    private static Class<?> doCompute(ByteClassLoader classLoader, String codeModel, List<Tuple3<String, String, String>> fieldMapping)
    {
        String classCode = codeModel;
        //field mapping
        classCode = classCode.replace("$fieldDefine", fieldMapping.stream()
                .map(x -> String.format("%s %s;", x.f1(), x.f2())).collect(Collectors.joining("\n")));
        classCode = classCode.replace("$fieldCreate", fieldMapping.stream()
                .map(x -> String.format("this.%s=%s;", x.f2(), x.f3())).collect(Collectors.joining("\n")));

        JavaClassCompiler javaClassCompiler = new JavaClassCompiler();
        String className = CLASS_START_NAME + classId.incrementAndGet();
        String classFullName = PACKAGE_NAME + "." + className;
        classCode = classCode.replace("$className", className);
        classCode = classCode.replace("$packageName", PACKAGE_NAME);

        JavaSourceObject javaFileObject = javaClassCompiler.doCompile(classFullName, classCode, Collections.singletonList("-XDuseUnsharedTable"));
        byte[] bytes = javaFileObject.getClassByteCodes().get(classFullName);
        logger.info("code generation compiled ,class {} code: {}", classFullName, classCode);
        requireNonNull(bytes, "not found " + classFullName + ".class");
        return classLoader.loadClass(classFullName, bytes);
    }

    private static String loadCodeTemplate(String url)
    {
        requireNonNull(url);
        try (InputStream inputStream = CalcOperator.class.getClassLoader().getResourceAsStream(url)) {
            requireNonNull(inputStream, "not found resource " + url);
            return new String(IOUtils.readAllBytes(inputStream));
        }
        catch (IOException e) {
            throw Throwables.throwThrowable(e);
        }
    }
}
