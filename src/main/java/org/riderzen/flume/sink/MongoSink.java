package org.kokidddd.flume.sink;

import com.mongodb.*;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;

import org.apache.flume.*;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.conf.Configurable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import org.bson.Document;


public class MongoSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(MongoSink.class);

    // MongoDB's connection settings
    private String host;
    private int port;
    private boolean authenticationEnabled;
    private String username;
    private String password;
    private String dbName;
    private String authSource;
    private String collectionName;
    private int batchSize;

    // MongoDB client objects
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;
    private SinkCounter sinkCounter;

    @Override
    public void configure(Context context) {
        host = context.getString("mongo.host", "localhost");
        port = context.getInteger("mongo.port", 27017);
        authenticationEnabled = context.getBoolean("mongo.auth.enabled", false);
        username = context.getString("mongo.username", "");
        password = context.getString("mongo.password", "");
        dbName = context.getString("mongo.database", "flume");
        authSource = context.getString("mongo.authSource", "admin");
        collectionName = context.getString("mongo.collection", "events");
        batchSize = context.getInteger("mongo.batchSize", 100);

        // Initialize MongoDB client
        if (authenticationEnabled) {
            MongoCredential credential = MongoCredential.createCredential(username, authSource, password.toCharArray());
            mongoClient = MongoClients.create(
                    MongoClientSettings.builder()
                            .credential(credential)
                            .applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress(host, port))))
                            .build());
        } else {
            mongoClient = MongoClients.create("mongodb://" + host + ":" + port);
        }
        database = mongoClient.getDatabase(dbName);
        collection = database.getCollection(collectionName);

        // Initialize SinkCounter
        sinkCounter = new SinkCounter(getName());
    }

    @Override
    public void start() {
        logger.info("Starting MongoDB sink");

        try {
            // Build MongoDB client settings
            MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder()
                    .applyToClusterSettings(builder ->
                            builder.hosts(Collections.singletonList(new ServerAddress(host, port))));

            // Add authentication if needed
            if (authenticationEnabled) {
                settingsBuilder.credential(
                        MongoCredential.createCredential(username, authSource, password.toCharArray())
                );
            }

            // Create MongoDB client
            mongoClient = MongoClients.create(settingsBuilder.build());
            database = mongoClient.getDatabase(dbName);
            collection = database.getCollection(collectionName);

            // Start counter
            sinkCounter.start();

            logger.info("MongoSink started: Connected to MongoDB at {}:{}", host, port);

        } catch (Exception e) {
            logger.error("Error starting MongoSink", e);

            throw new FlumeException("Failed to start MongoDB sink", e);
        }
    }

    private void closeMongoClient() {
        if (mongoClient != null) {
            try {
                mongoClient.close();
            } catch (Exception e) {
                logger.warn("Error closing MongoDB client", e);
            } finally {
                mongoClient = null;
            }
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        if (mongoClient == null) {
            throw new EventDeliveryException("MongoDB client is not initialized");
        }

        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();

        try {
            transaction.begin();

            List<WriteModel<Document>> bulkOperations = new ArrayList<>();
            int count = 0;

            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();
                if (event == null) {
                    break;  // No more events in the channel
                }

                String eventBody = new String(event.getBody(), StandardCharsets.UTF_8);
                try {
                    Document doc = Document.parse(eventBody);
                    bulkOperations.add(new InsertOneModel<>(doc));
                    count++;
                } catch (Exception e) {
                    logger.warn("Could not parse event as JSON: {}", eventBody, e);
                    // Create a simple document with raw data
                    Document doc = new Document("raw", eventBody);
                    bulkOperations.add(new InsertOneModel<>(doc));
                    count++;
                }
            }

            if (count > 0) {
                if (!bulkOperations.isEmpty()) {
                    try {
                        // Perform the bulk write operation
                        collection.bulkWrite(bulkOperations);
                        sinkCounter.addToEventDrainSuccessCount(count);
                        logger.info("Successfully inserted {} events into MongoDB", count);
                    } catch (MongoException e) {
                        logger.error("Failed to perform bulk write operation", e);
                        throw new EventDeliveryException("Failed to write events to MongoDB", e);
                    }
                }
                sinkCounter.incrementBatchCompleteCount();
            } else {
                sinkCounter.incrementBatchEmptyCount();
                status = Status.BACKOFF;  // No events to process
            }

            transaction.commit();
            return status;

        } catch (Exception e) {
            transaction.rollback();
            logger.error("MongoDB sink process error", e);
            throw new EventDeliveryException("Failed to write events to MongoDB", e);
        } finally {
            transaction.close();
        }
    }


    @Override
    public void stop() {
        logger.info("Stopping MongoDB sink");

        // Close MongoDB client if initialized
        if (mongoClient != null) {
            try {
                mongoClient.close();
                mongoClient = null;
                logger.info("MongoDB client closed successfully");
            } catch (Exception e) {
                logger.warn("Error occurred while closing MongoDB client", e);
            }
        }

        // Stop the sink counter if it's initialized
        if (sinkCounter != null) {
            try {
                sinkCounter.stop();
                logger.info("Sink counter stopped successfully");
            } catch (Exception e) {
                logger.warn("Error occurred while stopping sink counter", e);
            }
        }

        logger.info("MongoDB sink stopped");
    }
}