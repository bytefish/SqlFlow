package de.bytefish.sqlflow.example.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Issue(
        @JsonProperty("stack_trace") String stackTrace
) {}
