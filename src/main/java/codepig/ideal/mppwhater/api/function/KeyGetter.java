package codepig.ideal.mppwhater.api.function;

public interface KeyGetter<Row, KEY>
{
    KEY apply(Row input);
}
