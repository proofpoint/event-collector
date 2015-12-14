package com.proofpoint.event.collector;

public class ConfigLoadingException extends Exception
{
    public ConfigLoadingException(String message, Exception ex)
    {
        super(message, ex);
    }
}
