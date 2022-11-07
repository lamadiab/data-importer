package eu.topi.dataimporter.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import eu.topi.dataimporter.domain.CustomerRecord;
import eu.topi.dataimporter.domain.ProcessingResult;
import eu.topi.dataimporter.domain.ProcessingSummary;

@Service
public class CSVService {

    public ProcessingResult<CustomerRecord> processCustomers(InputStream inputStream) throws IOException, CsvException {
        CSVReader reader = new CSVReader(new InputStreamReader(inputStream));

        Map<String, Integer> headers = extractHeaders(reader.readNext());
        List<CustomerRecord> validCustomerRecords = new ArrayList<CustomerRecord>();
        List<CustomerRecord> invalidCustomerRecords = new ArrayList<CustomerRecord>();

        int refIndex = headers.get("ref");
        int nameIndex = headers.get("name");
        int emailIndex = headers.get("email");
        int addressIndex = headers.get("address");
        int countryCodeIndex = headers.get("country_code");

        String[] nextRecord;
        while ((nextRecord = reader.readNext()) != null) {
            CustomerRecord customerRecord = CustomerRecord.builder()
                    .ref(nextRecord[refIndex])
                    .name(nextRecord[nameIndex])
                    .email(nextRecord[emailIndex])
                    .address(nextRecord[addressIndex])
                    .countryCode(nextRecord[countryCodeIndex])
                    .build();

            if (customerRecord.isValid()) {
                validCustomerRecords.add(customerRecord);
            } else {
                invalidCustomerRecords.add(customerRecord);
            }
        }


        reader.close();

        ProcessingSummary summary = ProcessingSummary.builder()
                .totalNumberOfRecords(validCustomerRecords.size() + invalidCustomerRecords.size())
                .numberOfValidRecords(validCustomerRecords.size()).build();

        return ProcessingResult.<CustomerRecord>builder().validRecords(validCustomerRecords).summary(summary).build();
    }

    public Map<String, Integer> extractHeaders(String[] headLine) {
        Map<String, Integer> headers = new HashMap<>();

        for (int i = 0; i < headLine.length; i++) {
            headers.put(headLine[i], i);
        }

        return headers;
    }

    public List<CustomerRecord> buildCustomerRecords(List<String[]> records, Map<String, Integer> headers) {
        return records.stream().map(record -> CustomerRecord.builder()
                .ref(record[headers.get("ref")])
                .name(record[headers.get("name")])
                .email(record[headers.get("email")])
                .address(record[headers.get("address")])
                .countryCode(record[headers.get("country_code")])
                .build()).collect(Collectors.toList());
    }
}
