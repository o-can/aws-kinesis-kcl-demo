package de.ozzc.kinesis.example;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

import java.util.List;

/**
 * @author Ozkan Can
 */

public class KinesisClientDemo implements IRecordProcessor {

    @Override
    public void initialize(String s) {

    }

    @Override
    public void processRecords(List<Record> list, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {

    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {

    }
}
