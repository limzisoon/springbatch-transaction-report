package net.zslim.report.springbatch.config;

import net.zslim.report.springbatch.batch.TransactionItemReader;
import net.zslim.report.springbatch.batch.TransactionItemWriter;
import net.zslim.report.springbatch.model.Transaction;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import java.util.Arrays;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {

    @Autowired
    TransactionItemReader transactionItemReader;

    @Autowired
    TransactionItemWriter transactionItemWriter;

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory,
                   StepBuilderFactory stepBuilderFactory,
                   ItemReader<Transaction> itemReader,
                   ItemProcessor<Transaction, Transaction> itemProcessor,
                   ItemWriter<Transaction> itemWriter
    ) {

        Step step1 = stepBuilderFactory.get("csv-load-db")
                .<Transaction, Transaction>chunk(100)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .build();

        Step step2 = stepBuilderFactory.get("db-write-csv")
                .<Transaction, Transaction>chunk(100)
                .reader(transactionItemReader)
                .writer(transactionItemWriter)
                .build();

        return jobBuilderFactory.get("Daily-Summary-Report")
                .incrementer(new RunIdIncrementer())
                .start(step1)
                .next(step2)
                .build();
    }

    @Bean
    public FlatFileItemReader<Transaction> itemReader() {

        FlatFileItemReader<Transaction> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(new FileSystemResource("data/Input.csv"));
        flatFileItemReader.setName("csv-transaction");
        flatFileItemReader.setLineMapper(lineMapper());
        return flatFileItemReader;
    }

    @Bean
    public LineMapper<Transaction> lineMapper() {

        DefaultLineMapper<Transaction> defaultLineMapper = new DefaultLineMapper<>();

        FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();
        Range[] ranges = new Range[] {
                new Range(1, 3), //
                new Range(4, 7), //
                new Range(8, 11), //
                new Range(12, 15), //
                new Range(16, 19), //
                new Range(20, 25), //
                new Range(26, 27), //
                new Range(28, 31), //
                new Range(32, 37), //
                new Range(38, 45), //
                new Range(46, 48), //
                new Range(49, 50), //
                new Range(51, 51), //
                new Range(52, 52), //
                new Range(53, 62), //
                new Range(63, 63), //
                new Range(64, 73), //
                new Range(74, 85), //
                new Range(86, 86), //
                new Range(87, 89), //
                new Range(90, 101), //
                new Range(102, 102), //
                new Range(103, 105), //
                new Range(106, 117), //
                new Range(118, 118), //
                new Range(119, 121), //
                new Range(122, 129), //
                new Range(130, 135), //
                new Range(136, 141), //
                new Range(142, 147), //
                new Range(148, 162), //
                new Range(163, 168), //
                new Range(169, 175), //
                new Range(176, 176), //
                new Range(177, 303) //
        };
        tokenizer.setColumns(Arrays.asList(ranges).toArray(new Range[ranges.length]));
        tokenizer.setNames(
                new String[] {
                        "recordCode", "clientType", "clientNumber", "acctNumber","subAcctNumber","oppPartyCode","productGroupCode","exchangeCode","symbol","expirationDate",
                        "ccyCode","movementCode","buySellCode","qtyLongSign","qtyLong","qtyShortSign","qtyShort","brokerFee","brokerFeeDc","brokerFeeCurCode","clearingFee",
                        "clearingFeeDc","clearingFeeCurCode","commission","commissionDc","commissionCurCode","trxDate","futureRef","ticketNumber","externalNumber","trxPrice",
                        "traderInt","oppositeTraderId","openCloseCode","filler"
                }
        );
        tokenizer.setStrict(false);

        BeanWrapperFieldSetMapper<Transaction> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Transaction.class);

        defaultLineMapper.setLineTokenizer(tokenizer);
        defaultLineMapper.setFieldSetMapper(fieldSetMapper);

        return defaultLineMapper;
    }

}
