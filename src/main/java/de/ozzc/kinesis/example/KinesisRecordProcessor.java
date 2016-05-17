package de.ozzc.kinesis.example;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.*;
import com.amazonaws.services.kinesis.model.Record;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

/**
 * @author Ozkan Can
 * @see <a href="https://github.com/aws/aws-sdk-java/blob/master/src/samples/AmazonKinesis/AmazonKinesisApplicationSampleRecordProcessor.java">AmazonKinesisApplicationSampleRecordProcessor.java</a>
 */
public class KinesisRecordProcessor implements IRecordProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisRecordProcessor.class);

    private String shardId;

    // Backoff and retry settings
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;

    // Checkpoint about once a minute
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private long nextCheckpointTimeInMillis;

    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();


    @Override
    public void initialize(@NotNull InitializationInput initializationInput) {
        LOGGER.info("Initializing record processor for shard: {}", initializationInput.getShardId());
        this.shardId = initializationInput.getShardId();
    }

    @Override
    public void processRecords(@NotNull ProcessRecordsInput processRecordsInput) {
        List<Record> records = processRecordsInput.getRecords();
        if (records != null) {
            LOGGER.info("Processing " + records.size() + " records from " + shardId);

            // Process records and perform all exception handling.
            processRecordsWithRetries(records);

            // Checkpoint once every checkpoint interval.
            if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
                checkpoint(processRecordsInput.getCheckpointer());
                nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
            }
        }
    }

    @Override
    public void shutdown(@NotNull ShutdownInput shutdownInput) {
        LOGGER.info("Shutting down record processor for shard: {}", shardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
            checkpoint(shutdownInput.getCheckpointer());
        }
    }


    private void processRecordsWithRetries(@NotNull List<Record> records) {
        for (Record record : records) {
            boolean processedSuccessfully = false;
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
                    //
                    // Logic to process record goes here.
                    //
                    processSingleRecord(record);

                    processedSuccessfully = true;
                    break;
                } catch (Throwable t) {
                    LOGGER.warn("Caught throwable while processing record " + record, t);
                }

                // backoff if we encounter an exception.
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException e) {
                    LOGGER.debug("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                LOGGER.error("Couldn't process record " + record + ". Skipping the record.");
            }
        }
    }

    private void processSingleRecord(Record record) {
        String data = null;
        try {
            // For this app, we interpret the payload as UTF-8 chars.
            data = decoder.decode(record.getData()).toString();
            // Assume this record came from AmazonKinesisSample and log its age.
            long recordCreateTime = new Long(data.substring("testData-".length()));
            long ageOfRecordInMillis = System.currentTimeMillis() - recordCreateTime;

            LOGGER.info("{}, {}, {}, Created {} milliseconds ago.", record.getSequenceNumber(), record.getPartitionKey(), data, ageOfRecordInMillis);
        } catch (NumberFormatException e) {
            LOGGER.info("Record does not match sample record format. Ignoring record with data; {}", data);
        } catch (CharacterCodingException e) {
            LOGGER.error("Malformed data in record", e);
        }
    }


    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOGGER.info("Checkpointing shard " + shardId);
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                LOGGER.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (NUM_RETRIES - 1)) {
                    LOGGER.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    LOGGER.info("Transient issue when checkpointing - attempt " + (i + 1) + " of " + NUM_RETRIES, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOGGER.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                LOGGER.debug("Interrupted sleep", e);
            }
        }
    }
}
