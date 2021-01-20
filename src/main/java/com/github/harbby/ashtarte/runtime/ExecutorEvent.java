package com.github.harbby.ashtarte.runtime;

import java.net.SocketAddress;

public interface ExecutorEvent
        extends Event
{
    public static class ExecutorInitSuccessEvent
            implements ExecutorEvent
    {
        private final SocketAddress shuffleServiceAddress;

        public ExecutorInitSuccessEvent(SocketAddress shuffleServiceAddress) {this.shuffleServiceAddress = shuffleServiceAddress;}

        public SocketAddress getShuffleServiceAddress()
        {
            return shuffleServiceAddress;
        }
    }
}
