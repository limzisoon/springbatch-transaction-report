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
                .<Transaction, Transaction>chunk(1000)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .build();

        Step step2 = stepBuilderFactory.get("db-write-csv")
                .<Transaction, Transaction>chunk(1000)
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
                new Range(1, 3), // RECORD CODE
                new Range(4, 7), // CLIENT TYPE
                new Range(8, 11), // CLIENT NUMBER
                new Range(12, 15), // ACCOUNT NUMBER
                new Range(16, 19), // SUBACCOUNT NUMBER
                new Range(20, 25), // OPPOSITE PARTY CODE
                new Range(26, 27), // PRODUCT GROUP CODE
                new Range(28, 31), // EXCHANGE CODE
                new Range(32, 37), // SYMBOL
                new Range(38, 45), // EXPIRATION DATE
                new Range(46, 48), // CURRENCY CODE
                new Range(49, 50), // MOVEMENT CODE
                new Range(51, 51), // BUY SELL CODE
                new Range(52, 52), // QUANTITY LONG SIGN
                new Range(53, 62), // QUANTITY LONG
                new Range(63, 63), // QUANTITY SHORT SIGN
                new Range(64, 73), // QUANTITY SHORT
                new Range(74, 85), // BROKER FEE
                new Range(86, 86), // BROKER FEE DC
                new Range(87, 89), // BROKER FEE CUR CODE
                new Range(90, 101), // CLEARING FEE
                new Range(102, 102), // CLEARING FEE DC
                new Range(103, 105), // CLEARING FEE CUR CODE
                new Range(106, 117), // COMMISSION
                new Range(118, 118), // COMMISSION DC
                new Range(119, 121), // COMMISSION CUR CODE
                new Range(122, 129), // TRANSACTION DATE
                new Range(130, 135), // FUTURE REFERENCE
                new Range(136, 141), // TICKET NUMBER
                new Range(142, 147), // EXTERNAL NUMBER
                new Range(148, 162), // TRANSACTION PRICE
                new Range(163, 168), // TRADER INITIALS
                new Range(169, 175), // OPPOSITE TRADER ID
                new Range(176, 176), // OPEN CLOSE CODE
                new Range(177, 303) // FILLER
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
