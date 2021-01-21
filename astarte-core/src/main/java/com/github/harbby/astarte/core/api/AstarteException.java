package com.github.harbby.astarte.core.api;

public class AstarteException
        extends RuntimeException
{
    private static final long serialVersionUID = -1L;

    public AstarteException(String message)
    {
        super(message);
    }

    public AstarteException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
