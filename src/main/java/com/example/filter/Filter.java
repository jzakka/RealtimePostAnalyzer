package com.example.filter;

import java.io.File;

public interface Filter {
    boolean filter(File script, String message);
}
