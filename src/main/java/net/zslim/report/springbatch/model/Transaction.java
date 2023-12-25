package net.zslim.report.springbatch.model;

import lombok.*;
import org.hibernate.annotations.DynamicInsert;

import javax.persistence.*;
import java.math.BigDecimal;

@Entity
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@DynamicInsert
@Table(name = "Transactions")
public class Transaction {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Integer id;
    private String recordCode;
    private String clientType;
    private Integer clientNumber;
    private Integer acctNumber;
    private Integer subAcctNumber;
    private String oppPartyCode;
    private String productGroupCode;
    private String exchangeCode;
    private String symbol;
    private String expirationDate;
    private String ccyCode;
    private String movementCode;
    private String buySellCode;
    private String qtyLongSign;
    private Integer qtyLong;
    private String qtyShortSign;
    private Integer qtyShort;
    private BigDecimal brokerFee;
    private String brokerFeeDc;
    private String brokerFeeCurCode;
    private BigDecimal clearingFee;
    private String clearingFeeDc;
    private String clearingFeeCurCode;
    private BigDecimal commission;
    private String commissionDc;
    private String commissionCurCode;
    private String trxDate;
    private Integer futureRef;
    private String ticketNumber;
    private Integer externalNumber;
    private BigDecimal trxPrice;
    private String traderInt;
    private String oppositeTraderId;
    private String openCloseCode;
    private String filler;

    @Transient
    private BigDecimal totalTrxAmt;
}
