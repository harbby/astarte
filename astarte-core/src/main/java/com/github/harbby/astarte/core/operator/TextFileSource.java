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
import java.nio.file.NoSuchFileException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.github.harbby.gadtry.base.Throwables.throwsThrowable;

public class TextFileSource
        extends Operator<String>
{
    private final File dir;

    public TextFileSource(BatchContext yarkContext, String dir)
    {
        super(yarkContext);
        this.dir = new File(dir);
    }

    @Override
    public Partition[] getPartitions()
    {
        if (!dir.exists()) {
            throw throwsThrowable(new NoSuchFileException(dir.getPath()));
        }
        List<File> files = Files.listFiles(dir, false, file -> file.length() > 0);
        Partition[] partitions = new Partition[files.size()];
        for (int i = 0; i < files.size(); i++) {
            partitions[i] = new TextFilePartition(i, files.get(i));
        }
        return partitions;
    }

    private static class TextFilePartition
            extends Partition
    {
        private final File file;

        public TextFilePartition(int index, File file)
        {
            super(index);
            this.file = file;
        }
    }

    @Override
    public Iterator<String> compute(Partition partition, TaskContext taskContext)
    {
        TextFilePartition filePartition = (TextFilePartition) partition;
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
