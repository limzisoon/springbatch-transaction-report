package net.zslim.report.springbatch.repository;

import net.zslim.report.springbatch.model.Transaction;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TransactionRepository extends JpaRepository<Transaction, Integer> {

}



