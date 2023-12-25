package net.zslim.report.springbatch.batch;

import net.zslim.report.springbatch.model.Transaction;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

@Component
public class TransactionItemReader extends JdbcCursorItemReader<Transaction> implements ItemReader<Transaction> {

    public TransactionItemReader(@Autowired DataSource dataSource) {
        setDataSource(dataSource);
        setSql("select client_type,client_number ,acct_number ,sub_acct_number,exchange_code,product_group_code ,symbol ,expiration_date, "+
                "sum(qty_long-qty_short) as  total_trx_amt "+
                "from transactions group by "+
                "client_type,client_number ,acct_number ,sub_acct_number,exchange_code,product_group_code ,symbol ,expiration_date "+
                "order by client_number, exchange_code,symbol ");
        setFetchSize(100);
        setRowMapper(new TransactionRowMapper());
    }

    public class TransactionRowMapper implements RowMapper<Transaction> {
        @Override
        public Transaction mapRow(ResultSet rs, int rowNum) throws SQLException {
            Transaction trx  = new Transaction();
            trx.setClientType(rs.getString("client_type"));
            trx.setClientNumber(rs.getInt("client_number"));
            trx.setAcctNumber(rs.getInt("acct_number"));
            trx.setSubAcctNumber(rs.getInt("sub_acct_number"));
            trx.setExchangeCode(rs.getString("exchange_code"));
            trx.setProductGroupCode(rs.getString("product_group_code"));
            trx.setSymbol(rs.getString("symbol"));
            trx.setExpirationDate(rs.getString("expiration_date"));
            trx.setTotalTrxAmt(rs.getBigDecimal("total_trx_amt"));
            return trx;
        }
    }
}