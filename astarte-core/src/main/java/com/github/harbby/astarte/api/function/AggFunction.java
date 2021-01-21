package com.github.harbby.astarte.api.function;

public interface AggFunction<IN, S, OUT>
{
    public S getState();

    public void addRow(IN input);

    public void merge(S other);

    public void clear();

    public OUT getValue();
}
