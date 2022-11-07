package eu.topi.dataimporter.controller;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import com.opencsv.exceptions.CsvException;

import eu.topi.dataimporter.domain.CustomerRecord;
import eu.topi.dataimporter.domain.ProcessingResult;
import eu.topi.dataimporter.service.CSVService;

@RestController
public class CSVController {

    @Autowired
    CSVService csvService;

    final static String CSV_TYPE = "text/csv; charset=utf-8";

    @PostMapping("/process-file")
    public ResponseEntity<?> processFile(@RequestParam("url") final String url, UriComponentsBuilder ub)
            throws IOException, CsvException {
        URLConnection connection = new URL(url).openConnection();
        String contentType = connection.getContentType();

        ProcessingResult<CustomerRecord> result;

        InputStream inputStream = connection.getInputStream();

        switch (contentType) {
            case CSV_TYPE:
                result = csvService.processCustomers(inputStream);
                return new ResponseEntity<ProcessingResult<CustomerRecord>>(result, HttpStatus.OK);
            default:
                return new ResponseEntity<>(HttpStatus.OK);
        }

    }
}
