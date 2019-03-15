package codepig.ideal.mppwhater.api.function;

import java.io.Serializable;

public interface Filter<IN>
        extends Serializable
{
    boolean filter(IN input);
}
