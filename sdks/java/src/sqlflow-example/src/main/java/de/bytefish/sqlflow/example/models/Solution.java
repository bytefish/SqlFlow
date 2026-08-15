package de.bytefish.sqlflow.example.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Solution(
            @JsonProperty("patched_code") String patchedCode
    ) {}
