package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransactionListener {

    private final DatabaseConduit databaseConduit;
    private final IncentiveApiClient incentiveApiClient;

    public KafkaTransactionListener(DatabaseConduit databaseConduit,
                                    IncentiveApiClient incentiveApiClient) {
        this.databaseConduit = databaseConduit;
        this.incentiveApiClient = incentiveApiClient;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-core"
    )
    public void receive(Transaction transaction) {

        UserRecord sender = databaseConduit.findUser(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findUser(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        Incentive incentive = incentiveApiClient.getIncentive(transaction);

        sender.setBalance(sender.getBalance() - transaction.getAmount());

        recipient.setBalance(
                recipient.getBalance()
                        + transaction.getAmount()
                        + incentive.getAmount()
        );

        databaseConduit.saveUser(sender);
        databaseConduit.saveUser(recipient);

        TransactionRecord transactionRecord =
               new TransactionRecord(
        sender,
        recipient,
        transaction.getAmount(),
        incentive.getAmount()
);

        databaseConduit.saveTransaction(transactionRecord);
    }
}