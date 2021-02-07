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

import com.github.harbby.astarte.core.BatchContext;
import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.gadtry.base.Files;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.github.harbby.gadtry.base.Throwables.throwsThrowable;

public class TextFileSource
        extends Operator<String>
{
    private final transient Partition[] partitions;

    public TextFileSource(BatchContext batchContext, URI dataUri)
    {
        super(batchContext);
        String schema = Optional.ofNullable(dataUri.getScheme()).orElse("file");
        switch (schema.toLowerCase()) {
            case "file":
                this.partitions = prepareLocalFileSplit(dataUri);
                break;
            case "hdfs":
                throw new UnsupportedOperationException();
            default:
                throw new UnsupportedOperationException("schema " + schema + " not support URI " + dataUri);
        }
    }

    private static Partition[] prepareLocalFileSplit(URI uri)
    {
        File rootFile = new File(uri.getPath());
        if (!rootFile.exists()) {
            throw throwsThrowable(new NoSuchFileException(rootFile.getPath()));
        }
        List<File> files = Files.listFiles(rootFile, false, file -> file.length() > 0);
        Partition[] partitions = new Partition[files.size()];
        for (int i = 0; i < files.size(); i++) {
            partitions[i] = new TextFileSplit(i, files.get(i));
        }
        return partitions;
    }

    @Override
    public Partition[] getPartitions()
    {
        return partitions;
    }

    private static class TextFileSplit
            extends Partition
    {
        private final File file;

        public TextFileSplit(int index, File file)
        {
            super(index);
            this.file = file;
        }
    }

    @Override
    public Iterator<String> compute(Partition partition, TaskContext taskContext)
    {
        TextFileSplit filePartition = (TextFileSplit) partition;
        try {
            return new FileIteratorReader(filePartition.file);
        }
        catch (FileNotFoundException e) {
            throw throwsThrowable(e);
        }
    }

    private static class FileIteratorReader
            implements Iterator<String>, Serializable
    {
        private final BufferedReader reader;

        private String line;

        private FileIteratorReader(File file)
                throws FileNotFoundException
        {
            //todo: close
            FileInputStream inputStream = new FileInputStream(file);
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            this.reader = new BufferedReader(inputStreamReader);
        }

        @Override
        public boolean hasNext()
        {
            if (line != null) {
                return true;
            }
            try {
                line = reader.readLine();
            }
            catch (IOException e) {
                throw throwsThrowable(e);
            }
            return line != null;
        }

        @Override
        public String next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            String old = this.line;
            this.line = null;
            return old;
        }
    }
}
