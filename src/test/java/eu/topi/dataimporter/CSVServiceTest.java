package eu.topi.dataimporter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import eu.topi.dataimporter.domain.CustomerRecord;
import eu.topi.dataimporter.domain.ProcessingResult;
import eu.topi.dataimporter.service.CSVService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootTest
public class CSVServiceTest {

    @Autowired
    CSVService csvService;

    final String resourcesPath = System.getProperty("user.dir") + "/src/test/resources/";

    @Test
    public void processCSV_valid() throws IOException, CsvException {
        String filePath = resourcesPath + "example.csv";
        ProcessingResult<CustomerRecord> processCSVResult = csvService.processCustomers(new FileInputStream(filePath));
        CSVReader reader = new CSVReader(new FileReader(filePath));
        List<String[]> records = reader.readAll(); 
        Map<String, Integer> headers = csvService.extractHeaders(records.get(0));
        records.remove(0);

        assertEquals(processCSVResult.getSummary().getNumberOfValidRecords(), 10);
        assertEquals(processCSVResult.getSummary().getTotalNumberOfRecords(), 10);
        assertEquals(processCSVResult.getValidRecords(), csvService.buildCustomerRecords(records, headers));  
    }

    @Test
    public void processCSV_extraFields() throws IOException, CsvException, FileNotFoundException {
        String filePath = resourcesPath + "extra_fields.csv";
        ProcessingResult<CustomerRecord> processCSVResult = csvService.processCustomers(new FileInputStream(filePath));
        CSVReader reader = new CSVReader(new FileReader(filePath));
        List<String[]> records = reader.readAll(); 
        Map<String, Integer> headers = csvService.extractHeaders(records.get(0));
        records.remove(0);
        
        assertEquals(processCSVResult.getSummary().getNumberOfValidRecords(), 10);
        assertEquals(processCSVResult.getSummary().getTotalNumberOfRecords(), 10);
        assertEquals(processCSVResult.getValidRecords(), csvService.buildCustomerRecords(records, headers));
    }

    @Test
    public void processCSV_differentOrderKeys() throws IOException, CsvException, FileNotFoundException {
        String filePath = resourcesPath + "different_order_keys.csv";
        ProcessingResult<CustomerRecord> processCSVResult = csvService.processCustomers(new FileInputStream(filePath));
        CSVReader reader = new CSVReader(new FileReader(filePath));
        List<String[]> records = reader.readAll(); 
        Map<String, Integer> headers = csvService.extractHeaders(records.get(0));
        records.remove(0);
        
        assertEquals(processCSVResult.getSummary().getNumberOfValidRecords(), 10);
        assertEquals(processCSVResult.getSummary().getTotalNumberOfRecords(), 10);
        assertEquals(processCSVResult.getValidRecords(), csvService.buildCustomerRecords(records, headers));
    }    
    
    @Test
    public void processCSV_invalid() throws IOException, CsvException, FileNotFoundException {
        String filePath = resourcesPath + "invalid.csv";
        ProcessingResult<CustomerRecord> processCSVResult = csvService.processCustomers(new FileInputStream(filePath));
        CSVReader reader = new CSVReader(new FileReader(filePath));
        List<String[]> records = reader.readAll(); 
        Map<String, Integer> headers = csvService.extractHeaders(records.get(0));
        records.remove(0);
    

        List<CustomerRecord> customerRecords = csvService.buildCustomerRecords(records, headers);
        List<CustomerRecord> validCustomerRecords = customerRecords.stream().filter(record -> record.isValid()).collect(Collectors.toList());
        List<CustomerRecord> inValidCustomerRecords = customerRecords.stream().filter(record -> !record.isValid()).collect(Collectors.toList());
        
        assertEquals(processCSVResult.getSummary().getNumberOfValidRecords(), 5);
        assertEquals(processCSVResult.getSummary().getTotalNumberOfRecords(), 10);
        assertEquals(inValidCustomerRecords.size(), 5);
        assertEquals(processCSVResult.getValidRecords(), validCustomerRecords);
          
    }

    @Test
    public void extractHeaders() throws IOException, CsvException {
        Map<String, Integer> actualHeaders = Map.of("address", 0, "country_code", 1, "email", 2, "name", 3, "ref", 4);

        String filePath = resourcesPath + "different_order_keys.csv";
        CSVReader reader = new CSVReader(new FileReader(filePath));
        List<String[]> records = reader.readAll(); 

        Map<String, Integer> extractedHeaders = csvService.extractHeaders(records.get(0));
        records.remove(0);

        assertEquals(records.size(), 10);
        assertEquals(actualHeaders, extractedHeaders);
    }

    @Test
    public void buildCustomerRecords() throws IOException, CsvException {
        String filePath = resourcesPath + "example.csv";
        CSVReader reader = new CSVReader(new FileReader(filePath));
        List<String[]> records = reader.readAll(); 
        Map<String, Integer> headers = csvService.extractHeaders(records.get(0));
        records.remove(0);

        List<CustomerRecord> customerRecords = csvService.buildCustomerRecords(records, headers);

        assertEquals(customerRecords.get(8).getRef(), "c085e3ec-4c5a-46bd-b3b7-ed01ac395218");
        assertEquals(customerRecords.get(0).getName(), "Hannah Konopelski");
        assertEquals(customerRecords.get(4).getEmail(), "greysonstoltenberg@lemke.name");
        assertEquals(customerRecords.get(6).getAddress(), "88200 New Meadows mouth, San Antonio, Alaska 63591");
        assertEquals(customerRecords.get(2).getCountryCode(), "MG");
    }

    @Test
    public void processPerformance() throws IOException, CsvException {
        String resourcesPath = "fa76708c-ce20-48f9-a96a-14a839ff79c5.csv";

        long t1 = System.currentTimeMillis();
        csvService.processCustomers(new FileInputStream(resourcesPath));
        long t2 = System.currentTimeMillis();

        log.info("\n\n\n\n\nproccessed resourcesPath={}, in dt={}ms, \n\n\n\n\n", resourcesPath, (t2- t1));


        assertTrue(t2 - t1 < 3000);

    }
}
