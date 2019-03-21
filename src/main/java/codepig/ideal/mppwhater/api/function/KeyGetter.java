package codepig.ideal.mppwhater.api.function;

import java.io.Serializable;

public interface KeyGetter<Row, KEY>
        extends Serializable
{
    KEY apply(Row input);
}
