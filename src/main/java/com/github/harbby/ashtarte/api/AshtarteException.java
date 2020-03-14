package com.github.harbby.ashtarte.api;

public class AshtarteException
        extends RuntimeException
{
    private static final long serialVersionUID = -1L;

    public AshtarteException(String message)
    {
        super(message);
    }

    public AshtarteException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
