package net.zslim.report.springbatch.controller;

import net.zslim.report.springbatch.model.TransactionResponseDTO;
import net.zslim.report.springbatch.service.TransactionService;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class TransactionController {

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    TransactionService transactionService;

    @Autowired
    Job job;

    public TransactionController(TransactionService transactionService) {
    }

    /**
     * This method call spring batch
     * step1. load the input.csv file to database temp table
     * step2. generate the daily summary transaction report - data/Output.csv file
     */
    @GetMapping(value = "/load")
    public BatchStatus load() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {

        Map<String, JobParameter> maps = new HashMap<>();
        maps.put("time", new JobParameter(System.currentTimeMillis()));
        JobParameters parameters = new JobParameters(maps);
        JobExecution jobExecution = jobLauncher.run(job, parameters);

        System.out.println("JobExecution: " + jobExecution.getStatus());

        System.out.println("Batch is Running...");
        while (jobExecution.isRunning()) {
            System.out.println("...");
        }

        return jobExecution.getStatus();
    }

    /**
     * This method call spring batch
     * step1. load the data/input.csv file to database temp table
     * step2. generate the daily summary transaction report - data/Output.csv file
     * query database and create the daily summary transaction in json format response
     */
    @GetMapping(value = "/load2")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<List<TransactionResponseDTO>> load2() {
        try {
            Map<String, JobParameter> maps = new HashMap<>();
            maps.put("time", new JobParameter(System.currentTimeMillis()));
            JobParameters parameters = new JobParameters(maps);
            JobExecution jobExecution = jobLauncher.run(job, parameters);

            System.out.println("JobExecution: " + jobExecution.getStatus());

            System.out.println("Batch is Running...");
            while (jobExecution.isRunning()) {
                System.out.println("...");
            }

            List<TransactionResponseDTO> trxDTOList = transactionService.findDailySummaryTransaction();
            trxDTOList.stream().forEach((c) -> System.out.println(c.toString()));

            return new ResponseEntity<>(trxDTOList, HttpStatus.OK);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * This method call spring batch
     * step1. load the data/input.csv file to database temp table
     * step2. generate the daily summary transaction report - data/Output.csv file
     * Prepare the Output.csv file as response
     */
    @GetMapping(value = "/load3")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<ByteArrayResource> load3() {
        try {
            Map<String, JobParameter> maps = new HashMap<>();
            maps.put("time", new JobParameter(System.currentTimeMillis()));
            JobParameters parameters = new JobParameters(maps);
            JobExecution jobExecution = jobLauncher.run(job, parameters);

            System.out.println("JobExecution: " + jobExecution.getStatus());

            System.out.println("Batch is Running...");
            while (jobExecution.isRunning()) {
                System.out.println("...");
            }

            //output path
            byte[] bytes = Files.readAllBytes(Paths.get("data/Output.csv"));
            System.out.println(bytes.length);

            ByteArrayResource resource = new ByteArrayResource(bytes);

            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .contentLength(resource.contentLength())
                    .header(HttpHeaders.CONTENT_DISPOSITION,
                            ContentDisposition.attachment()
                                    .filename("Output.csv")
                                    .build().toString())
                    .body(resource);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
