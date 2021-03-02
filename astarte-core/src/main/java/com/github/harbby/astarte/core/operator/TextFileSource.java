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

import com.github.harbby.astarte.core.api.Collector;
import com.github.harbby.astarte.core.api.DataSetSource;
import com.github.harbby.astarte.core.api.Split;
import com.github.harbby.gadtry.base.Files;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
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
        implements DataSetSource<String>
{
    private final URI fileUri;
    private volatile boolean stop = false;

    public TextFileSource(URI fileUri)
    {
        this.fileUri = fileUri;
    }

    @Override
    public Split[] trySplit(int tryParallelism)
    {
        String schema = Optional.ofNullable(fileUri.getScheme()).orElse("file");
        switch (schema.toLowerCase()) {
            case "file":
                return prepareLocalFileSplit(fileUri);
            case "hdfs":
                throw new UnsupportedOperationException();
            default:
                throw new UnsupportedOperationException("schema " + schema + " not support URI " + fileUri);
        }
    }

    @Override
    public Iterator<String> phyPlan(Split split)
    {
        TextFileSplit fileSplit = (TextFileSplit) split;
        try {
            return new FileIteratorReader(fileSplit);
        }
        catch (IOException e) {
            throw throwsThrowable(e);
        }
    }

    @Override
    public void pushModePhyPlan(Collector<String> collector, Split split)
    {
        TextFileSplit fileSplit = (TextFileSplit) split;
        try (FileInputStream inputStream = new FileInputStream(fileSplit.file);
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader reader = new BufferedReader(inputStreamReader)) {
            inputStream.getChannel().position(fileSplit.startOffset);
            String line;
            while (!stop && (line = reader.readLine()) != null) {
                collector.collect(line);
            }
        }
        catch (IOException e) {
            throw throwsThrowable(e);
        }
    }

    @Override
    public void close()
    {
        this.stop = true;
    }

    private static Split[] prepareLocalFileSplit(URI uri)
    {
        File rootFile = new File(uri.getPath());
        if (!rootFile.exists()) {
            throw throwsThrowable(new NoSuchFileException(rootFile.getPath()));
        }
        List<File> files = Files.listFiles(rootFile, false, file -> file.length() > 0);
        Split[] splits = new Split[files.size()];
        for (int i = 0; i < files.size(); i++) {
            splits[i] = new TextFileSplit(files.get(i));
        }
        return splits;
    }

    private static class TextFileSplit
            implements Split
    {
        private final File file;
        private final long startOffset;
        private final long length;

        public TextFileSplit(File file)
        {
            this.file = file;
            //todo: file split
            this.startOffset = 0;
            this.length = file.length();
        }

        public File getFile()
        {
            return file;
        }

        public long getLength()
        {
            return length;
        }

        public long getStartOffset()
        {
            return startOffset;
        }
    }

    private static class FileIteratorReader
            implements Iterator<String>, Serializable
    {
        private final BufferedReader reader;

        private String line;

        private FileIteratorReader(TextFileSplit fileSplit)
                throws IOException
        {
            //todo: close
            FileInputStream inputStream = new FileInputStream(fileSplit.file);
            inputStream.getChannel().position(fileSplit.startOffset);

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
