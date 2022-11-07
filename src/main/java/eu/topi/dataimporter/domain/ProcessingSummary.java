package eu.topi.dataimporter.domain;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class ProcessingSummary {
    int totalNumberOfRecords;
    int numberOfValidRecords;
}
