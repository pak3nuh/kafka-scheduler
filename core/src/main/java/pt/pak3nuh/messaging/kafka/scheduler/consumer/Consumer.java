package pt.pak3nuh.messaging.kafka.scheduler.consumer;

import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;

public interface Consumer {
    Iterable<Record> poll();
    void commit(Record record);

    interface Record {
        String getTopic();
        int getPartition();
        long getOffset();
        InternalMessage getMessage();
    }
}
