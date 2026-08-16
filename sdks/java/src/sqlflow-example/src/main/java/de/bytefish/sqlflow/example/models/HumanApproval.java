package de.bytefish.sqlflow.example.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public record HumanApproval(
        @JsonProperty("approved") boolean approved,
        @JsonProperty("reason") String reason
) {}
