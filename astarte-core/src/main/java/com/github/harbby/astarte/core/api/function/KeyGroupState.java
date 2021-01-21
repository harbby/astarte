package com.github.harbby.astarte.core.api.function;

public interface KeyGroupState<K, S>
{
    public K getKey();

    void update(S state);

    S getState();

    public static <K, S> KeyGroupState<K, S> createKeyGroupState(K k)
    {
        return new KeyGroupStateImpl<>(k);
    }

    public static class KeyGroupStateImpl<K, S>
            implements KeyGroupState<K, S>
    {
        private final K k;
        private S state;

        public KeyGroupStateImpl(K k)
        {
            this.k = k;
        }

        @Override
        public K getKey()
        {
            return k;
        }

        @Override
        public void update(S state)
        {
            this.state = state;
        }

        @Override
        public S getState()
        {
            return state;
        }
    }
}
