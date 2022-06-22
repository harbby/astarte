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

import com.github.harbby.astarte.core.api.function.Filter;
import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.astarte.core.operator.FilterOperator;
import com.github.harbby.astarte.core.operator.FlatMapOperator;
import com.github.harbby.astarte.core.operator.MapOperator;
import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.gadtry.collection.tuple.Tuple3;
import com.github.harbby.gadtry.compiler.ByteClassLoader;
import com.github.harbby.gadtry.compiler.JavaClassCompiler;
import com.github.harbby.gadtry.compiler.JavaSourceObject;
import com.github.harbby.gadtry.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

public class CodeGenUtil
{
    private static final Logger logger = LoggerFactory.getLogger(CodeGenUtil.class);

    private static final String PACKAGE_NAME = CalcOperator.class.getPackage().getName();
    private static final String CLASS_START_NAME = "CodeGenIterator$";

    private static final AtomicInteger classId = new AtomicInteger(0);
    private static final AtomicInteger fieldId = new AtomicInteger(0);
    private static final String calcCodeModel = loadCodeTemplate("./codemodel/CalcModel.java");
    private static final String flatMapCodeModel = loadCodeTemplate("./codemodel/CalcModelFlatMap.java");

    private static String flatMapInstanceMethod;

    static {
        try {
            Method method = CodeGenUtil.class.getMethod("flatMapBaseNewInstance", ClassLoader.class, String.class, List.class);
            flatMapInstanceMethod = method.getDeclaringClass().getName() + "." + method.getName();
        }
        catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    private CodeGenUtil() {}

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
                    //当前stage最后一个算子是flatMap
                    builder.append("child = (Iterator<O>)" + field + ".map(value);\n");
                }
                else {
                    Class<?> aClass = prepareCode(flatMapCodeModel, classLoader, operators.subList(i + 1, operators.size()));
                    fieldMapping.add(Tuple3.of(FlatMapCalcBase.class.getCanonicalName() + "<O>", "flatMapCalcBase",
                            flatMapInstanceMethod + "(this.getClass().getClassLoader(), \"" + aClass.getCanonicalName() + "\", operators.subList(" + (i + 1) + ", operators.size()));"));
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

    private static Class<?> doCompute(ByteClassLoader classLoader, String codeModel, List<Tuple3<String, String, String>> fieldMapping)
    {
        String classCode = codeModel;
        //field mapping
        classCode = classCode.replace("$fieldDefine", fieldMapping.stream()
                .map(x -> String.format("private final %s %s;", x.f1(), x.f2())).collect(Collectors.joining("\n")));
        classCode = classCode.replace("$fieldCreate", fieldMapping.stream()
                .map(x -> String.format("%s=%s;", x.f2(), x.f3())).collect(Collectors.joining("\n")));

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
