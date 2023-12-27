package net.zslim.report.springbatch.batch;

import net.zslim.report.springbatch.model.Transaction;
import net.zslim.report.springbatch.repository.TransactionRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Primary
public class DBWriter implements ItemWriter<Transaction> {

    private TransactionRepository transactionRepository;

    @Autowired
    public DBWriter (TransactionRepository transactionRepository) {
        this.transactionRepository = transactionRepository;
    }

    @Override
    public void write(List<? extends Transaction> trxs) throws Exception{

        transactionRepository.deleteAll();

        System.out.println("Data Saved for trxs: " + trxs.size());

        transactionRepository.saveAll(trxs);
    }
}
