package de.ozzc.kinesis.example;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.config.KinesisClientLibConfigurator;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.Security;
import java.util.Properties;

/**
 * @author Ozkan Can
 */

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private static final String NETWORK_CACHE_TTL = "networkaddress.cache.ttl";
    private static final String DEFAULT_NETWORK_CACHE_TTL = "60";
    private static final String AWS_REGION = "AWSRegion";
    private static final String DEFAULT_AWS_REGION = "eu-central-1";
    private static final InitialPositionInStream STREAM_LATEST = InitialPositionInStream.LATEST;

    public static void main(String[] args) {
        Properties properties = new Properties();
        KinesisClientLibConfiguration clientLibConfiguration = null;
        try {
            properties.load(Main.class.getResourceAsStream("/config.properties"));
            Security.setProperty(NETWORK_CACHE_TTL, properties.getProperty(NETWORK_CACHE_TTL, DEFAULT_NETWORK_CACHE_TTL));
            properties.remove(NETWORK_CACHE_TTL);
            String region = properties.getProperty(AWS_REGION, DEFAULT_AWS_REGION);
            KinesisClientLibConfigurator clientLibConfigurator = new KinesisClientLibConfigurator();
            clientLibConfiguration = clientLibConfigurator.getConfiguration(properties)
                    .withInitialPositionInStream(STREAM_LATEST)
                    .withRegionName(region);
        } catch (IOException e) {
            LOGGER.error("Failed to create configuration.", e);
            System.exit(-1);
        }
        if (args.length > 0 && "delete-resources".equals(args[0])) {
            deleteResources(clientLibConfiguration);
        } else {

            IRecordProcessorFactory recordProcessorFactory = new KinesisRecordProcessorFactory();
            Worker worker = new Worker.Builder()
                    .recordProcessorFactory(recordProcessorFactory)
                    .config(clientLibConfiguration)
                    .build();
            try {
                worker.run();
            } catch (Throwable t) {
                LOGGER.error("Caught throwable while processing data.", t);
                System.exit(-2);
            }

            System.exit(0);
        }
    }

    public static void deleteResources(KinesisClientLibConfiguration kinesisClientLibConfiguration) {
        LOGGER.info("Deleting Kinesis resources ...");
        Region region = Region.getRegion(Regions.fromName(kinesisClientLibConfiguration.getRegionName()));
        AWSCredentialsProvider awsCredentialsProvider = kinesisClientLibConfiguration.getKinesisCredentialsProvider();

        AmazonKinesis kinesisClient = new AmazonKinesisClient(awsCredentialsProvider);
        kinesisClient.setRegion(region);
        try {
            String streamName = kinesisClientLibConfiguration.getStreamName();
            kinesisClient.deleteStream(streamName);
            LOGGER.info("Stream {} deleted." + streamName);
        } catch (com.amazonaws.services.kinesis.model.ResourceNotFoundException e) {
            // The stream does not exist.
        }
        AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(awsCredentialsProvider);
        dynamoDBClient.setRegion(region);
        try
        {
            String appName = kinesisClientLibConfiguration.getApplicationName();
            dynamoDBClient.deleteTable(appName);
            LOGGER.info("DynamoDB table {} deleted." + appName);
        } catch(com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException e) {
            // The table does not exist.
        }
        LOGGER.info("Deleted all Kinesis resources.");

    }
}
