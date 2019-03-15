package codepig.ideal.mppwhater.api.operator;

import codepig.ideal.mppwhater.MppContext;
import codepig.ideal.mppwhater.api.Partition;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

import static com.github.harbby.gadtry.base.Throwables.throwsException;

public class TextFileDataSet
        extends AbstractDataSet<String>
{
    private final String dir;

    public TextFileDataSet(MppContext yarkContext, String dir)
    {
        super(yarkContext);
        this.dir = dir;
    }

    @Override
    public Partition[] getPartitions()
    {
        File file = new File(dir);
        if (file.isFile()) {
            return new TextFilePartition[] {new TextFilePartition(0, file)};
        }
        else {
            throw new UnsupportedOperationException();
        }
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
    public Iterator<String> compute(Partition partition)
    {
        TextFilePartition filePartition = (TextFilePartition) partition;
        try {
            return Files.readAllLines(Paths.get(filePartition.file.toURI())).iterator();
        }
        catch (IOException e) {
            throw throwsException(e);
        }
    }
}
