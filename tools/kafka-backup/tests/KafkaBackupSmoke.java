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
        source.admin.alterConsumerGroupOffsets(group, Map.of(tp, new OffsetAndMetadata(80))).all().get(35, TimeUnit.SECONDS);
        String secondaryGroup = "audit-backlog-secondary";
        source.admin.alterConsumerGroupOffsets(secondaryGroup, Map.of(tp, new OffsetAndMetadata(116))).all().get(35, TimeUnit.SECONDS);
        ObjectNode request = OpenBknKafkaBackup.obj(); request.putArray("topics").add(audit).add(evidence);
        Path payload = work.resolve("ledger-payload"); ObjectNode meta = source.export(request, payload);
        target.preflight(meta);
        ObjectNode complete = meta.deepCopy(); ((ObjectNode)complete.path("groups").path(group).get(0)).put("offset", 117);
        target.preflight(complete);
        ObjectNode missing = meta.deepCopy(); ((ObjectNode)missing.path("groups")).remove(group);
        try { target.preflight(missing); throw new AssertionError("Missing ledger position accepted"); }
        catch (IllegalArgumentException expected) { check(expected.getMessage().contains("提交位置"), "Unexpected missing-position failure"); }
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
        check(result.path("records").asLong() == 117, "Ledger records missing");
        check(target.admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(35, TimeUnit.SECONDS).get(tp).offset() == 80,
            "Original ledger consumption position not restored");
        check(target.admin.listConsumerGroupOffsets(secondaryGroup).partitionsToOffsetAndMetadata().get(35, TimeUnit.SECONDS).get(tp).offset() == 116,
            "Secondary consumer backlog position not restored");
        check(target.offset(evidence, 0, true, false) == 0, "Empty Evidence topic restore failed");
        List<ConsumerRecord<byte[],byte[]>> restored = new ArrayList<>(); target.consume(tp, 0, 117, restored::add);
        for (int i = 0; i < 117; i++) check(restored.get(i).offset() == i && restored.get(i).value()[0] == (byte)i, "Ledger coordinates changed");
        Properties consumerConfig = new Properties(); consumerConfig.putAll(target.props);
        consumerConfig.put("group.id", group); consumerConfig.put("enable.auto.commit", "false"); consumerConfig.put("auto.offset.reset", "none");
        try (KafkaConsumer<byte[],byte[]> consumer = new KafkaConsumer<>(consumerConfig,
                new org.apache.kafka.common.serialization.ByteArrayDeserializer(), new org.apache.kafka.common.serialization.ByteArrayDeserializer())) {
            consumer.subscribe(List.of(audit)); long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            List<ConsumerRecord<byte[],byte[]>> pending = new ArrayList<>();
            while (pending.size() < 37 && System.nanoTime() < deadline)
                for (ConsumerRecord<byte[],byte[]> row : consumer.poll(java.time.Duration.ofMillis(200))) pending.add(row);
            check(pending.size() == 37, "Consumer did not resume with 37 pending records");
            for (int i = 0; i < pending.size(); i++) check(pending.get(i).offset() == 80 + i
                && pending.get(i).value()[0] == (byte)(80 + i), "Consumed history repeated or backlog skipped");
            check(consumer.position(tp) == 117, "Consumer did not reach end after draining backlog");
            try { target.preflight(meta, true); throw new AssertionError("Overwrite allowed an active consumer"); }
            catch (IllegalArgumentException expected) { check(expected.getMessage().contains("消费组停止"), "Unexpected active-consumer failure"); }
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
        String untouched = "unrelated-overwrite-guard";
        target.admin.createTopics(List.of(new NewTopic(untouched, 1, (short)1))).all().get(35, TimeUnit.SECONDS);
        try (KafkaProducer<byte[],byte[]> producer = producer(target, null)) {
            producer.send(new ProducerRecord<>(untouched, new byte[]{42})).get(35, TimeUnit.SECONDS);
        }
        String untouchedId = target.description(untouched).topicId().toString();
        String oldId = target.description(audit).topicId().toString();
        ObjectNode overwritePlan = target.preflight(meta, true);
        check(overwritePlan.path("replace_topics").size() == 1
            && overwritePlan.path("replace_topics").get(0).asText().equals(audit), "Unexpected overwrite scope");
        check(target.description(audit).topicId().toString().equals(oldId) && target.offset(audit, 0, true, false) == 118,
            "Overwrite preflight modified the topic");
        JsonNode overwritten = target.restore(payload, true);
        check(overwritten.path("replaced_topics").size() == 1 && !target.description(audit).topicId().toString().equals(oldId),
            "Nonempty audit topic was not recreated");
        check(target.offset(audit, 0, true, false) == 117 && target.admin.listConsumerGroupOffsets(group)
            .partitionsToOffsetAndMetadata().get(35, TimeUnit.SECONDS).get(tp).offset() == 80, "Overwrite lost data or saved position");
        check(target.description(untouched).topicId().toString().equals(untouchedId)
            && target.offset(untouched, 0, true, false) == 1, "Overwrite changed a topic outside the backup");
        System.out.println("PASS: original offsets, groups resume at 80/116, pending records consumed, overwrite recreates only selected nonempty topic, read-only plan and active-consumer guards");
    }
    public static void main(String[] args) throws Exception {
        check("yes".equals(System.getenv("KAFKA_BACKUP_DISPOSABLE_TEST")), "Requires disposable test environment");
        Path work = Path.of(args[2]); Files.createDirectories(work);
        String topic = "openbkn-backup-test-" + UUID.randomUUID().toString(), group = topic + "-group";
        try (OpenBknKafkaBackup source = new OpenBknKafkaBackup(args[0]); OpenBknKafkaBackup target = new OpenBknKafkaBackup(args[1])) {
            source.admin.createTopics(List.of(new NewTopic(topic, 1, (short)1).configs(Map.of(
                "cleanup.policy", "delete", "retention.ms", "-1", "retention.bytes", "-1", "message.timestamp.type", "CreateTime"))))
                .all().get(35, TimeUnit.SECONDS);
            List<NewTopic> connectTopics = new ArrayList<>();
            for (String name : OpenBknKafkaBackup.IGNORED_TOPICS)
                connectTopics.add(new NewTopic(name, 1, (short)1).configs(Map.of("cleanup.policy", "compact")));
            source.admin.createTopics(connectTopics).all().get(35, TimeUnit.SECONDS);
            target.admin.createTopics(connectTopics).all().get(35, TimeUnit.SECONDS);
            try (KafkaProducer<byte[],byte[]> connect = producer(target, null)) {
                for (String name : OpenBknKafkaBackup.IGNORED_TOPICS)
                    connect.send(new ProducerRecord<>(name, new byte[]{1}, new byte[]{42})).get(35, TimeUnit.SECONDS);
            }
            String connectId = target.description("connect-configs").topicId().toString();
            ObjectNode discovered = source.describe(OpenBknKafkaBackup.obj());
            check(discovered.path("topics").size() == 1 && discovered.path("topics").get(0).path("name").asText().equals(topic)
                && discovered.path("ignored_topics").size() == 3, "Default selection included Connect compact topics");
            ObjectNode connectRequest = OpenBknKafkaBackup.obj(); connectRequest.putArray("topics").add("connect-configs");
            ObjectNode empty = source.export(connectRequest, work.resolve("connect-only"));
            check(empty.path("topics").isEmpty(), "Explicit Connect selection was not ignored");
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
                    ObjectNode legacy = meta.deepCopy();
                    ObjectNode legacyTopic = (ObjectNode)legacy.path("topics").get(0);
                    String originalFile = legacyTopic.path("partitions").get(0).path("file").asText();
                    legacyTopic.put("name", "connect-configs"); ((ObjectNode)legacyTopic.path("config")).put("cleanup.policy", "compact");
                    ((ObjectNode)legacyTopic.path("partitions").get(0)).put("file", "connect-configs-0.ndjson");
                    for (JsonNode positions : legacy.path("groups")) for (JsonNode position : positions)
                        ((ObjectNode)position).put("topic", "connect-configs");
                    Path legacyPayload = work.resolve("connect-legacy"); Files.createDirectory(legacyPayload);
                    Files.copy(payload.resolve(originalFile), legacyPayload.resolve("connect-configs-0.ndjson"));
                    Files.write(legacyPayload.resolve("metadata.json"), OpenBknKafkaBackup.line(legacy));
                    check(target.preflight(legacy, true).path("topics").isEmpty(), "Legacy Connect preflight was not ignored");
                    check(target.restore(legacyPayload, true).path("records").asLong() == 0, "Legacy Connect data was restored");
                    check(target.description("connect-configs").topicId().toString().equals(connectId)
                        && target.offset("connect-configs", 0, true, false) == 1, "Legacy restore changed target Connect data");
                    System.out.println("PASS: Connect compact topics excluded from default/explicit backup and legacy restore, target Connect data preserved with overwrite");
                    System.out.println("PASS: online export, aborted-transaction exclusion, cross-cluster restore, binary/null/Unicode/duplicate headers, timestamp, mapped offsets, readback, retry and replay guards");
                } finally { stop.set(true); writer.join(35000); }
            }
        }
    }
}
