package com.Exceptions;

public class ColumnNotFound extends RuntimeException {
    public ColumnNotFound(String message){
        super(message);
    }
}
