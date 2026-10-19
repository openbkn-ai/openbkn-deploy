import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Runs only against two explicitly supplied disposable test brokers. */
public final class KafkaBackupSmoke {
    static void check(boolean ok, String message) { if (!ok) throw new AssertionError(message); }
    static KafkaProducer<byte[],byte[]> producer(OpenBknKafkaBackup client, String transaction) {
        Properties config = new Properties(); config.putAll(client.props);
        config.put("compression.type", "zstd");
        if (transaction != null) config.put("transactional.id", transaction);
        return new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
    }
    static void ledgerResume(OpenBknKafkaBackup source, OpenBknKafkaBackup target, Path work) throws Exception {
        String audit = "openbkn.audit.v1", evidence = "openbkn.evidence.v1", group = "bkn-trace-audit-ledger-v1";
        Map<String,String> config = Map.of("cleanup.policy", "delete", "retention.ms", "-1", "retention.bytes", "-1", "message.timestamp.type", "LogAppendTime");
        source.admin.createTopics(List.of(new NewTopic(audit, 1, (short)1).configs(config), new NewTopic(evidence, 1, (short)1).configs(config)))
            .all().get(35, TimeUnit.SECONDS);
        TopicPartition tp = new TopicPartition(audit, 0);
        try (KafkaProducer<byte[],byte[]> producer = producer(source, null)) {
            for (int i = 0; i < 117; i++) producer.send(new ProducerRecord<>(audit, 0, new byte[]{1}, new byte[]{(byte)i})).get(35, TimeUnit.SECONDS);
        }
        source.admin.alterConsumerGroupOffsets(group, Map.of(tp, new OffsetAndMetadata(117))).all().get(35, TimeUnit.SECONDS);
        ObjectNode request = OpenBknKafkaBackup.obj(); request.putArray("topics").add(audit).add(evidence);
        Path payload = work.resolve("ledger-payload"); ObjectNode meta = source.export(request, payload);
        ObjectNode pending = meta.deepCopy(); ((ObjectNode)pending.path("groups").path(group).get(0)).put("offset", 116);
        try { target.preflight(pending); throw new AssertionError("Pending ledger replay accepted"); }
        catch (IllegalArgumentException expected) { check(expected.getMessage().contains("历史重放"), "Unexpected pending-ledger failure"); }
        ObjectNode gap = meta.deepCopy(); ((ObjectNode)gap.path("topics").get(0).path("partitions").get(0)).put("count", 116);
        try { target.preflight(gap); throw new AssertionError("Ledger coordinate reuse accepted"); }
        catch (IllegalArgumentException expected) { check(expected.getMessage().contains("坐标"), "Unexpected coordinate failure"); }
        target.admin.createTopics(List.of(new NewTopic(audit, 1, (short)1).configs(Map.of(
            "message.timestamp.type", "CreateTime", "retention.ms", "86400000"))))
            .all().get(35, TimeUnit.SECONDS);
        ObjectNode plan = target.preflight(meta);
        check(plan.path("config_changes").path(audit).has("message.timestamp.type"), "Config difference not reported");
        check(target.config(audit).path("message.timestamp.type").asText().equals("CreateTime"), "Preflight modified target config");
        JsonNode result = target.restore(payload);
        check(target.config(audit).equals(meta.path("topics").get(0).path("config")), "Backup topic config not applied");
        check(result.path("records").asLong() == 117, "Consumed ledger records missing");
        check(target.admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(35, TimeUnit.SECONDS).get(tp).offset() == 117,
            "Original ledger consumption position not restored");
        check(target.offset(evidence, 0, true, false) == 0, "Empty Evidence topic restore failed");
        List<ConsumerRecord<byte[],byte[]>> restored = new ArrayList<>(); target.consume(tp, 0, 117, restored::add);
        for (int i = 0; i < 117; i++) check(restored.get(i).offset() == i && restored.get(i).value()[0] == (byte)i, "Ledger coordinates changed");
        Properties consumerConfig = new Properties(); consumerConfig.putAll(target.props);
        consumerConfig.put("group.id", group); consumerConfig.put("enable.auto.commit", "false"); consumerConfig.put("auto.offset.reset", "none");
        try (KafkaConsumer<byte[],byte[]> consumer = new KafkaConsumer<>(consumerConfig,
                new org.apache.kafka.common.serialization.ByteArrayDeserializer(), new org.apache.kafka.common.serialization.ByteArrayDeserializer())) {
            consumer.subscribe(List.of(audit)); long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (consumer.assignment().isEmpty() && System.nanoTime() < deadline)
                check(consumer.poll(java.time.Duration.ofMillis(200)).isEmpty(), "Consumed history was replayed");
            check(!consumer.assignment().isEmpty() && consumer.position(tp) == 117, "Consumer did not resume at 117");
            try (KafkaProducer<byte[],byte[]> producer = producer(target, null)) {
                check(producer.send(new ProducerRecord<>(audit, 0, new byte[]{1}, new byte[]{99})).get(35, TimeUnit.SECONDS).offset() == 117,
                    "New records reused old ledger coordinates");
            }
            boolean found = false;
            while (!found && System.nanoTime() < deadline) for (ConsumerRecord<byte[],byte[]> row : consumer.poll(java.time.Duration.ofMillis(200))) {
                check(row.offset() == 117, "Historical audit record replayed"); found = true;
            }
            check(found, "New audit record not consumed after recovery");
        }
        System.out.println("PASS: 117 consumed audit records restored with original coordinates, original group resumes at EOF, empty Evidence restored, new record starts at 117, pending/gapped recovery blocked");
    }
    public static void main(String[] args) throws Exception {
        check("yes".equals(System.getenv("KAFKA_BACKUP_DISPOSABLE_TEST")), "Requires disposable test environment");
        Path work = Path.of(args[2]); Files.createDirectories(work);
        String topic = "openbkn-backup-test-" + UUID.randomUUID().toString(), group = topic + "-group";
        try (OpenBknKafkaBackup source = new OpenBknKafkaBackup(args[0]); OpenBknKafkaBackup target = new OpenBknKafkaBackup(args[1])) {
            source.admin.createTopics(List.of(new NewTopic(topic, 1, (short)1).configs(Map.of(
                "cleanup.policy", "delete", "retention.ms", "-1", "retention.bytes", "-1", "message.timestamp.type", "CreateTime"))))
                .all().get(35, TimeUnit.SECONDS);
            TopicPartition tp = new TopicPartition(topic, 0);
            try (KafkaProducer<byte[],byte[]> producer = producer(source, null)) {
                producer.send(new ProducerRecord<>(topic, 0, Long.valueOf(1234), null, new byte[]{0, -1, 10}, List.of(
                    new RecordHeader("same", null), new RecordHeader("same", new byte[0]), new RecordHeader("中文🙂", new byte[]{92, 10}))))
                    .get(35, TimeUnit.SECONDS);
                producer.send(new ProducerRecord<>(topic, 0, new byte[0], null)).get(35, TimeUnit.SECONDS);
                producer.send(new ProducerRecord<>(topic, 0, new byte[]{1}, new byte[0])).get(35, TimeUnit.SECONDS);
                try (KafkaProducer<byte[],byte[]> aborted = producer(source, topic + "-transaction")) {
                    aborted.initTransactions(); aborted.beginTransaction();
                    aborted.send(new ProducerRecord<>(topic, 0, new byte[]{1}, new byte[]{99})).get(35, TimeUnit.SECONDS);
                    aborted.abortTransaction();
                }
                source.admin.alterConsumerGroupOffsets(group, Map.of(tp, new OffsetAndMetadata(3))).all().get(35, TimeUnit.SECONDS);
                AtomicBoolean stop = new AtomicBoolean(); AtomicReference<Throwable> failure = new AtomicReference<>(); CountDownLatch first = new CountDownLatch(1);
                Thread writer = new Thread(() -> {
                    try {
                        while (!stop.get()) {
                            producer.send(new ProducerRecord<>(topic, 0, new byte[]{1}, new byte[]{2})).get(35, TimeUnit.SECONDS);
                            first.countDown(); Thread.sleep(10);
                        }
                    } catch (Throwable error) { failure.set(error); first.countDown(); }
                });
                writer.start();
                try {
                    check(first.await(35, TimeUnit.SECONDS) && failure.get() == null, "Online producer did not start");
                    ObjectNode request = OpenBknKafkaBackup.obj(); request.putArray("topics").add(topic);
                    Path payload = work.resolve("payload"); ObjectNode meta = source.export(request, payload);
                    check(Arrays.equals(OpenBknKafkaBackup.line(OpenBknKafkaBackup.verifyFiles(payload)), OpenBknKafkaBackup.line(meta)), "Export validation failed");
                    check(writer.isAlive(), "Backup unexpectedly stopped producer");
                    producer.send(new ProducerRecord<>(topic, 0, new byte[]{1}, new byte[]{3})).get(35, TimeUnit.SECONDS);
                    check(source.offset(topic, 0, true, true) > meta.path("topics").get(0).path("partitions").get(0).path("high").asLong(), "Fixed boundary advanced");
                    check(source.admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(35, TimeUnit.SECONDS).get(tp).offset() == 3, "Backup changed source position");
                    stop.set(true); writer.join(35000); check(!writer.isAlive() && failure.get() == null, "Online writer failed");
                    check(!source.clusterId().equals(target.clusterId()), "Source and target must be separate clusters");
                    target.preflight(meta); JsonNode restored = target.restore(payload);
                    long count = restored.path("records").asLong(); check(count >= 4, "Missing committed messages");
                    List<ConsumerRecord<byte[],byte[]>> data = new ArrayList<>(); target.consume(tp, 0, count, data::add);
                    check(data.size() == count && data.get(0).key() == null && Arrays.equals(data.get(0).value(), new byte[]{0,-1,10}), "Binary key/value mismatch");
                    check(data.get(0).headers().toArray().length == 3 && data.get(0).headers().toArray()[0].value() == null
                        && data.get(0).headers().toArray()[1].value().length == 0, "Header/null/empty mismatch");
                    check(data.get(1).key().length == 0 && data.get(1).value() == null && data.get(2).value().length == 0, "Tombstone/empty mismatch");
                    check(data.get(0).timestamp() == 1234, "CreateTime changed");
                    check(target.admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(35, TimeUnit.SECONDS).get(tp).offset() == 3, "Offset gap mapping failed");
                    try { target.preflight(meta); throw new AssertionError("Nonempty retry accepted"); } catch (IllegalArgumentException expected) { check(expected.getMessage().contains("非全新"), "Unexpected retry failure"); }
                    ObjectNode different = meta.deepCopy();
                    ((ObjectNode)different.path("topics").get(0).path("config")).put("retention.ms", "123456");
                    ObjectNode originalConfig = target.config(topic);
                    try { target.preflight(different); throw new AssertionError("Nonempty target accepted with config changes"); }
                    catch (IllegalArgumentException expected) { check(expected.getMessage().contains("非全新"), "Unexpected nonempty config failure"); }
                    check(target.config(topic).equals(originalConfig), "Rejected restore changed config");
                    for (String blocked : List.of("openbkn.audit.v1", "openbkn.evidence.v1")) {
                        ObjectNode invalid = meta.deepCopy(); ((ObjectNode)invalid.path("topics").get(0)).put("name", blocked);
                        for (JsonNode part : invalid.path("topics").get(0).path("partitions")) ((ObjectNode)part).put("file", blocked + "-" + part.path("id").asInt() + ".ndjson");
                        ((ObjectNode)invalid.path("groups")).removeAll();
                        try { target.preflight(invalid); throw new AssertionError("Unsupported historical replay accepted"); }
                        catch (IllegalArgumentException expected) { check(expected.getMessage().contains("历史重放") || expected.getMessage().contains("坐标"), "Unexpected historical replay failure"); }
                    }
                    ledgerResume(source, target, work);
                    System.out.println("PASS: online export, aborted-transaction exclusion, cross-cluster restore, binary/null/Unicode/duplicate headers, timestamp, mapped offsets, readback, retry and replay guards");
                } finally { stop.set(true); writer.join(35000); }
            }
        }
    }
}
