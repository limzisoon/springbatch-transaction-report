package net.zslim.report.springbatch.batch;

import net.zslim.report.springbatch.model.Transaction;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Writer;

@Component
public class TransactionItemWriter extends FlatFileItemWriter<Transaction> {

    public static final String columns = "clientType,clientNumber,acctNumber,subAcctNumber,exchangeCode,productGroupCode,symbol,expirationDate,totalTrxAmt";

    public TransactionItemWriter() {
        setResource(new FileSystemResource("data/Output.csv"));
        setHeaderCallback(new FlatFileHeaderCallback() {
            @Override
            public void writeHeader(Writer writer) throws IOException {
                writer.write(columns);
            }
        });
        setLineAggregator(getDelimitedLineAggregator());
    }

    public DelimitedLineAggregator<Transaction> getDelimitedLineAggregator() {
        BeanWrapperFieldExtractor<Transaction> beanWrapperFieldExtractor = new BeanWrapperFieldExtractor<Transaction>();
        beanWrapperFieldExtractor.setNames(new String[] {"clientType","clientNumber","acctNumber","subAcctNumber","exchangeCode",
            "productGroupCode","symbol","expirationDate","totalTrxAmt"});
        DelimitedLineAggregator<Transaction> delimitedLineAggregator = new DelimitedLineAggregator<Transaction>();
        delimitedLineAggregator.setDelimiter(",");
        delimitedLineAggregator.setFieldExtractor(beanWrapperFieldExtractor);
        return delimitedLineAggregator;

    }
}