package org.kokidddd.flume.sink;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.flume.*;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.bson.Document;
import org.apache.flume.Sink.Status;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;
import static org.bson.assertions.Assertions.assertNotNull;
import static org.junit.Assert.assertEquals;

/**
 * Test class for the MongoSink implementation
 */
@Test(singleThreaded = true, threadPoolSize = 1)
public class MongoSinkTest {
    private static MongoClient mongoClient;
    public static final String DBNAME = "flume_test";
    public static final String COLLECTION = "events_test";

    private static Context ctx = new Context();
    private static Channel channel;
    private static MongoSink mongoSink;

    @BeforeMethod(groups = {"dev"})
    public static void setup() {

        // Create MongoCredential
        MongoCredential credential = MongoCredential.createCredential("root", "admin", "12345678".toCharArray());

        // Create MongoDB client for tests
        mongoClient = MongoClients.create(MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Collections.singletonList(new ServerAddress("localhost", 27018))))
                .credential(credential)
                .build());

        // Setup sink configuration
        Map<String, String> ctxMap = new HashMap<>();
        ctxMap.put("mongo.host", "localhost");
        ctxMap.put("mongo.port", "27018");
        ctxMap.put("mongo.database", DBNAME);
        ctxMap.put("mongo.collection", COLLECTION);
        ctxMap.put("mongo.batchSize", "100");
        ctxMap.put("mongo.auth.enabled", "true");
        ctxMap.put("mongo.username", "root");
        ctxMap.put("mongo.password", "12345678");

        ctx.putAll(ctxMap);

        // Configure memory channel
        Context channelCtx = new Context();
        channelCtx.put("capacity", "1000000");
        channelCtx.put("transactionCapacity", "1000000");
        channel = new MemoryChannel();
        Configurables.configure(channel, channelCtx);

        // Setup Flume MongoSink
        mongoSink = new MongoSink();
        mongoSink.configure(ctx);

        // Clear the collection before each test
        try {
            MongoDatabase db = mongoClient.getDatabase(DBNAME);
            MongoCollection<Document> collection = db.getCollection(COLLECTION);
            collection.drop();
        } catch (Exception e) {
            // Ignore if collection doesn't exist
        }
    }

    @AfterMethod(groups = {"dev"})
    public static void tearDown() {
        // Clean up test database
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test(groups = "dev")
    public void basicSinkTest() throws EventDeliveryException {
        // Configure sink
        MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);
        sink.setChannel(channel);
        sink.start();

        try {
            // Create test event with JSON payload
            String jsonPayload = "{\"name\":\"test\", \"age\":25, \"timestamp\":" + System.currentTimeMillis() + "}";
            Event event = EventBuilder.withBody(jsonPayload.getBytes(StandardCharsets.UTF_8));

            // Put event in channel
            Transaction tx = channel.getTransaction();
            tx.begin();
            channel.put(event);
            tx.commit();
            tx.close();

            // Process event
            Status status = sink.process();
            assertEquals(Status.READY, status);

            // Verify document was inserted correctly
            MongoDatabase db = mongoClient.getDatabase(DBNAME);
            MongoCollection<Document> collection = db.getCollection(COLLECTION);

            Document queryDoc = new Document("name", "test");
            Document result = collection.find(queryDoc).first();
            assertNotNull(result);
            assertEquals("The 'name' field in the inserted document should match.", "test", result.getString("name"));
            assertEquals(Integer.valueOf(25), result.getInteger("age"));
            assertNotNull(result.getLong("timestamp"));
        } finally {
            // Ensure the sink is stopped after the test
            sink.stop();
        }
    }

    @Test(groups = "dev")
    public void batchProcessingTest() throws EventDeliveryException {
        // Configure sink with smaller batch size
        Map<String, String> ctxMap = ctx.getParameters();
        Context batchCtx = new Context(ctxMap);
        batchCtx.put("mongo.batchSize", "10");

        MongoSink sink = new MongoSink();
        Configurables.configure(sink, batchCtx);
        sink.setChannel(channel);
        sink.start();

        try {
            // Insert multiple events
            Transaction tx = channel.getTransaction();
            tx.begin();

            for (int i = 0; i < 25; i++) {
                String jsonPayload = "{\"name\":\"test" + i + "\", \"value\":" + i + "}";
                Event event = EventBuilder.withBody(jsonPayload.getBytes(StandardCharsets.UTF_8));
                channel.put(event);
            }

            tx.commit();
            tx.close();

            Status status1 = sink.process(); // Process first batch (10 events)
            assertEquals(Status.READY, status1);

            Status status2 = sink.process(); // Process second batch (next 10 events)
            assertEquals(Status.READY, status2);

            Status status3 = sink.process(); // Process third batch (remaining 5 events)
            assertEquals(Status.READY, status3);

            // Verify all 25 documents were inserted
            MongoDatabase db = mongoClient.getDatabase(DBNAME);
            MongoCollection<Document> collection = db.getCollection(COLLECTION);

            long count = collection.countDocuments(); // Count total documents in collection
            assertEquals(25, count);

            // Verify specific documents inserted correctly
            Document doc0 = collection.find(Filters.eq("name", "test0")).first();
            assertNotNull(doc0);
            assertEquals(Integer.valueOf(0), doc0.getInteger("value"));

            Document doc24 = collection.find(Filters.eq("name", "test24")).first();
            assertNotNull(doc24);
            assertEquals(Integer.valueOf(24), doc24.getInteger("value"));
        } finally {
            // Stop the sink after the test
            sink.stop();
        }
    }

    @Test(groups = "dev")
    public void invalidJsonHandlingTest() throws EventDeliveryException {
        // Configure sink
        MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);
        sink.setChannel(channel);
        sink.start();

        try {
            // Create event with invalid JSON
            String invalidJson = "This is not a valid JSON document";
            Event event = EventBuilder.withBody(invalidJson.getBytes(StandardCharsets.UTF_8));

            // Put event in channel
            Transaction tx = channel.getTransaction();
            tx.begin();
            channel.put(event);
            tx.commit();
            tx.close();

            // Process event - should handle the invalid JSON gracefully
            Status status = sink.process(); // Process event with the sink
            assertEquals(Status.READY, status);

            // Verify document was inserted with raw field
            MongoDatabase db = mongoClient.getDatabase(DBNAME);
            MongoCollection<Document> collection = db.getCollection(COLLECTION);

            Document result = collection.find(new Document("raw", invalidJson)).first();

            assertNotNull(result);
            assertEquals("Expected raw field to contain the invalid JSON string.", invalidJson, result.getString("raw").trim());
        } finally {
            sink.stop();
        }
    }

    @Test(groups = "dev")
    public void emptyChannelTest() throws EventDeliveryException {
        // Configure sink
        MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);
        sink.setChannel(channel);
        sink.start();

        try {
            // Don't add any events to the channel

            // Process should return BACKOFF when the channel is empty
            Status status = sink.process(); // Call process() on the sink
            assertEquals(Status.BACKOFF, status);

            // Verify no documents were inserted into MongoDB
            MongoDatabase db = mongoClient.getDatabase(DBNAME);
            MongoCollection<Document> collection = db.getCollection(COLLECTION);

            // Count documents in the collection and verify it is 0
            long count = collection.countDocuments();
            assertEquals(0, count);
        } finally {
            sink.stop();
        }
    }
}