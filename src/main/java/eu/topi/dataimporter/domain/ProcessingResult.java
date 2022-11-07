package eu.topi.dataimporter.domain;

import java.util.List;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class ProcessingResult<T>{
    ProcessingSummary summary;
    List<T> validRecords;
}


