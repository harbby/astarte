package test;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.apache.flink.util.Preconditions.checkState;

public class ClueWeb09IteratorReader
        implements Iterator<Tuple2<Integer, int[]>>, Serializable
{
    private final File file;
    private final BufferedReader reader;

    private String line;
    private int number = 0;

    public ClueWeb09IteratorReader(File file)
            throws FileNotFoundException
    {
        this.file = file;
        InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file));
        this.reader = new BufferedReader(inputStreamReader);
    }

    public ClueWeb09IteratorReader(String file)
            throws FileNotFoundException
    {
        this(new File(file));
    }

    @Override
    public boolean hasNext()
    {
        if (line != null) {
            return true;
        }
        do {
            try {
                line = reader.readLine();
                number++;
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            if (line == null) {
                return false;
            }
        }
        while (line.length() == 0);
        return true;
    }

    @Override
    public Tuple2<Integer, int[]> next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        checkState(line.length() > 0);
        String[] targets = this.line.trim().split(" ");
        int[] targetIds = new int[targets.length];
        for (int i = 0; i < targets.length; i++) {
            targetIds[i] = Integer.parseInt(targets[i]);
        }
        this.line = null;
        return new Tuple2<>(number, targetIds);
    }
}