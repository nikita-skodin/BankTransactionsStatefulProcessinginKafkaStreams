package com.github.programmingwithmati.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BankBalance {

    private Long id;
    private BigDecimal amount = BigDecimal.ZERO;
    private Date lastUpdate;
    private BankTransaction latestTransaction;

    public BankBalance process(BankTransaction bankTransaction) {

        this.id = bankTransaction.getBalanceId();
        this.lastUpdate = bankTransaction.getTime();
        this.latestTransaction = bankTransaction;

        if (amount.add(bankTransaction.getAmount()).compareTo(BigDecimal.ZERO) < 0){
            bankTransaction.setState(BankTransaction.BankTransactionState.REJECTED);
        } else {
            bankTransaction.setState(BankTransaction.BankTransactionState.APPROVED);
            amount = amount.add(bankTransaction.getAmount());
        }

        return this;
    }
}
