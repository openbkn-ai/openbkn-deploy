import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.header.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Runtime uses only Java 17 and libraries already shipped in Kafka 3.9. */
public final class OpenBknKafkaBackup implements AutoCloseable {
    static final int MAX_LINE = 24 * 1024 * 1024;
    static final String MODE = "retained-committed-records-v1";
    static final Set<String> IGNORED_TOPICS = Set.of("connect-configs", "connect-offsets", "connect-status");
    static final Map<String,String> LEDGER_GROUPS = Map.of("openbkn.audit.v1", "bkn-trace-audit-ledger-v1",
        "openbkn.evidence.v1", "bkn-trace-evidence-ledger-v1");
    static final List<String> CONFIGS = List.of("cleanup.policy", "retention.ms", "retention.bytes",
        "segment.ms", "segment.bytes", "max.message.bytes", "message.timestamp.type", "min.insync.replicas");
    static final ObjectMapper JSON = new ObjectMapper(JsonFactory.builder().streamReadConstraints(
        StreamReadConstraints.builder().maxStringLength(MAX_LINE).build()).build());
    final Properties props = new Properties();
    final Admin admin;

    static void require(boolean ok, String message) { if (!ok) throw new IllegalArgumentException(message); }
    static String env(String key, String fallback) { return System.getenv().getOrDefault(key, fallback); }
    static ObjectNode obj() { return JSON.createObjectNode(); }
    static ArrayNode arr() { return JSON.createArrayNode(); }
    static String topicName(String name) {
        require(name.matches("[A-Za-z0-9._-]{1,249}") && !name.startsWith("__") && !name.equals(".") && !name.equals(".."),
            "无效或内部 Topic: " + name);
        return name;
    }
    static MessageDigest digest() throws Exception { return MessageDigest.getInstance("SHA-256"); }
    static String hex(byte[] bytes) { return HexFormat.of().formatHex(bytes); }

    // Match Python's ensure_ascii=True compact JSON, including non-ASCII header names.
    static void appendJson(JsonNode node, StringBuilder out) {
        if (node.isTextual()) {
            out.append('"');
            for (char c : node.textValue().toCharArray()) {
                switch (c) {
                    case '"': out.append("\\\""); break;
                    case '\\': out.append("\\\\"); break;
                    case '\b': out.append("\\b"); break;
                    case '\f': out.append("\\f"); break;
                    case '\n': out.append("\\n"); break;
                    case '\r': out.append("\\r"); break;
                    case '\t': out.append("\\t"); break;
                    default:
                        if (c < 32 || c > 126) out.append(String.format(Locale.ROOT, "\\u%04x", (int)c));
                        else out.append(c);
                }
            }
            out.append('"');
        } else if (node.isArray()) {
            out.append('[');
            boolean first = true;
            for (JsonNode item : node) { if (!first) out.append(','); appendJson(item, out); first = false; }
            out.append(']');
        } else if (node.isObject()) {
            out.append('{');
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            boolean first = true;
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> item = fields.next();
                if (!first) out.append(',');
                appendJson(TextNode.valueOf(item.getKey()), out); out.append(':'); appendJson(item.getValue(), out); first = false;
            }
            out.append('}');
        } else out.append(node.toString());
    }
    static byte[] line(JsonNode node) {
        StringBuilder out = new StringBuilder(); appendJson(node, out); out.append('\n');
        return out.toString().getBytes(StandardCharsets.UTF_8);
    }
    static JsonNode b64(byte[] bytes) { return bytes == null ? NullNode.instance : TextNode.valueOf(Base64.getEncoder().encodeToString(bytes)); }
    static byte[] unb64(JsonNode node) {
        require(node != null && (node.isNull() || node.isTextual()), "消息字节编码无效");
        return node.isNull() ? null : Base64.getDecoder().decode(node.textValue());
    }
    static ObjectNode record(ConsumerRecord<byte[],byte[]> message) {
        ObjectNode row = obj().put("offset", message.offset()).put("timestamp", message.timestamp())
            .put("timestamp_type", message.timestampType().id + 1);
        row.set("key", b64(message.key())); row.set("value", b64(message.value()));
        ArrayNode headers = row.putArray("headers");
        for (Header header : message.headers()) headers.add(arr().add(header.key()).add(b64(header.value())));
        return row;
    }
    static byte[] content(JsonNode row) { return line(arr().add(row.get("key")).add(row.get("value")).add(row.get("headers"))); }
    static JsonNode readLine(InputStream source) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (int b; (b = source.read()) != -1;) {
            bytes.write(b); require(bytes.size() <= MAX_LINE, "消息超过 24MiB Base64 上限");
            if (b == '\n') return JSON.readTree(bytes.toByteArray());
        }
        require(bytes.size() == 0, "消息记录被截断"); return null;
    }
    static JsonNode request() throws Exception {
        byte[] bytes = System.in.readNBytes(8 * 1024 * 1024 + 1);
        require(bytes.length <= 8 * 1024 * 1024, "请求清单过大"); return JSON.readTree(bytes);
    }

    public OpenBknKafkaBackup() throws Exception { this(null); }
    public OpenBknKafkaBackup(String bootstrap) throws Exception {
        require(org.apache.kafka.common.utils.AppInfoParser.getVersion().startsWith("3.9."), "当前工具要求镜像内 Kafka 客户端为 3.9.x");
        props.put("bootstrap.servers", bootstrap == null ? env("TASK_KAFKA_BROKERS", "kafka.resource.svc.cluster.local:9092") : bootstrap);
        String protocol = env("TASK_KAFKA_PROTOCOL", "SASL_PLAINTEXT");
        props.put("security.protocol", protocol); props.put("request.timeout.ms", "30000"); props.put("default.api.timeout.ms", "35000");
        props.put("client.id", "openbkn-data-backup");
        Path credentials = Path.of(System.getProperty("openbkn.kafka.credentials", "/credentials"));
        if (protocol.startsWith("SASL")) {
            String password = Files.readString(credentials.resolve("password"));
            require(!password.isEmpty() && !password.contains(","), "Kafka Secret 必须是单个用户密码，不能是逗号分隔的多用户密码");
            String mechanism = env("TASK_KAFKA_MECHANISM", "PLAIN");
            props.put("sasl.mechanism", mechanism);
            String module = mechanism.equals("PLAIN") ? "org.apache.kafka.common.security.plain.PlainLoginModule"
                : "org.apache.kafka.common.security.scram.ScramLoginModule";
            props.put("sasl.jaas.config", module + " required username=\"" + jaas(env("TASK_KAFKA_USER", "kafkauser"))
                + "\" password=\"" + jaas(password) + "\";");
        }
        if (Files.exists(credentials.resolve("ca.crt"))) {
            props.put("ssl.truststore.type", "PEM"); props.put("ssl.truststore.location", credentials.resolve("ca.crt").toString());
        }
        admin = Admin.create(props);
    }
    static String jaas(String value) {
        require(value.chars().noneMatch(c -> c < 32), "Kafka 认证配置含不支持的控制字符");
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
    public void close() { admin.close(Duration.ofSeconds(5)); }
    String clusterId() throws Exception { return admin.describeCluster().clusterId().get(35, TimeUnit.SECONDS); }
    TopicDescription description(String name) throws Exception { return admin.describeTopics(List.of(name)).allTopicNames().get(35, TimeUnit.SECONDS).get(name); }
    ObjectNode config(String name) throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        Config cfg = admin.describeConfigs(List.of(resource)).all().get(35, TimeUnit.SECONDS).get(resource);
        ObjectNode result = obj();
        for (String key : CONFIGS) { require(cfg.get(key) != null && cfg.get(key).value() != null, "Topic 配置不可读: " + key); result.put(key, cfg.get(key).value()); }
        return result;
    }
    long offset(String name, int partition, boolean latest, boolean committed) throws Exception {
        TopicPartition tp = new TopicPartition(name, partition);
        return admin.listOffsets(Map.of(tp, latest ? OffsetSpec.latest() : OffsetSpec.earliest()),
            new ListOffsetsOptions(committed ? IsolationLevel.READ_COMMITTED : IsolationLevel.READ_UNCOMMITTED))
            .all().get(35, TimeUnit.SECONDS).get(tp).offset();
    }
    ObjectNode groups(Set<String> names) throws Exception {
        ObjectNode result = obj();
        for (ConsumerGroupListing group : admin.listConsumerGroups().all().get(35, TimeUnit.SECONDS)) {
            ArrayNode selected = arr();
            Map<TopicPartition,OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(group.groupId(),
                new ListConsumerGroupOffsetsOptions().requireStable(true)).partitionsToOffsetAndMetadata().get(35, TimeUnit.SECONDS);
            for (Map.Entry<TopicPartition,OffsetAndMetadata> item : offsets.entrySet()) {
                if (item.getValue() != null && item.getValue().offset() >= 0 && names.contains(item.getKey().topic()))
                    selected.add(obj().put("topic", item.getKey().topic()).put("partition", item.getKey().partition()).put("offset", item.getValue().offset()));
            }
            if (!selected.isEmpty()) result.set(group.groupId(), selected);
        }
        return result;
    }
    static Set<String> names(JsonNode meta) { Set<String> names = new TreeSet<>(); for (JsonNode t : meta.path("topics")) names.add(t.path("name").asText()); return names; }
    static ObjectNode restorableMetadata(JsonNode original) {
        ObjectNode meta = original.deepCopy(); ArrayNode topics = arr(); Set<String> kept = new HashSet<>();
        for (JsonNode topic : original.path("topics")) if (!IGNORED_TOPICS.contains(topic.path("name").asText())) {
            topics.add(topic); kept.add(topic.path("name").asText());
        }
        ObjectNode groups = obj();
        original.path("groups").fields().forEachRemaining(group -> {
            ArrayNode positions = arr(); for (JsonNode position : group.getValue())
                if (kept.contains(position.path("topic").asText())) positions.add(position);
            if (!positions.isEmpty()) groups.set(group.getKey(), positions);
        });
        meta.set("topics", topics); meta.set("groups", groups); return meta;
    }
    static TopicPartition tp(JsonNode topic, JsonNode part) { return new TopicPartition(topic.path("name").asText(), part.path("id").asInt()); }
    void idle(JsonNode meta) throws Exception {
        Set<String> names = names(meta);
        Set<String> relevant = new HashSet<>(); groups(names).fieldNames().forEachRemaining(relevant::add);
        Collection<ConsumerGroupListing> listed = admin.listConsumerGroups().all().get(35, TimeUnit.SECONDS);
        if (listed.isEmpty()) return;
        List<String> ids = new ArrayList<>(); for (ConsumerGroupListing group : listed) ids.add(group.groupId());
        Map<String,ConsumerGroupDescription> descriptions = admin.describeConsumerGroups(ids).all().get(35, TimeUnit.SECONDS);
        for (ConsumerGroupDescription group : descriptions.values()) {
            boolean assigned = group.members().stream().flatMap(m -> m.assignment().topicPartitions().stream()).anyMatch(p -> names.contains(p.topic()));
            if (assigned || relevant.contains(group.groupId()) || meta.path("groups").has(group.groupId()))
                require(group.members().isEmpty() && (group.state() == ConsumerGroupState.EMPTY || group.state() == ConsumerGroupState.DEAD),
                    "恢复要求目标消费组停止: " + group.groupId());
        }
    }
    KafkaConsumer<byte[],byte[]> consumer() {
        Properties cfg = new Properties(); cfg.putAll(props);
        cfg.put("enable.auto.commit", "false"); cfg.put("auto.offset.reset", "none"); cfg.put("allow.auto.create.topics", "false");
        cfg.put("isolation.level", "read_committed"); cfg.put("max.poll.records", "100"); cfg.put("fetch.max.bytes", "16777216");
        cfg.put("max.partition.fetch.bytes", "16777216");
        return new KafkaConsumer<>(cfg, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
    interface RecordAction { void accept(ConsumerRecord<byte[],byte[]> row) throws Exception; }
    void consume(TopicPartition tp, long low, long high, RecordAction action) throws Exception {
        if (low == high) return;
        try (KafkaConsumer<byte[],byte[]> consumer = consumer()) {
            consumer.assign(List.of(tp)); consumer.seek(tp, low); long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (consumer.position(tp) < high) {
                ConsumerRecords<byte[],byte[]> batch = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<byte[],byte[]> row : batch) { if (row.offset() >= high) return; action.accept(row); }
                if (!batch.isEmpty()) deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
                require(System.nanoTime() < deadline, "分区读取超时；请检查事务、网络和分区范围");
            }
        }
    }
    ObjectNode describe(JsonNode request) throws Exception {
        Set<String> all = admin.listTopics().names().get(35, TimeUnit.SECONDS);
        Set<String> selected = new TreeSet<>();
        if (request.hasNonNull("topics")) { for (JsonNode name : request.path("topics")) require(selected.add(topicName(name.asText())), "Topic 重复"); }
        else for (String name : all) if (!name.startsWith("__")) selected.add(topicName(name));
        require(all.containsAll(selected), "指定 Topic 不存在");
        Set<String> ignored = new TreeSet<>(selected); ignored.retainAll(IGNORED_TOPICS); selected.removeAll(IGNORED_TOPICS);
        ObjectNode meta = obj().put("mode", MODE).put("source_cluster_id", clusterId()).put("created_at", System.currentTimeMillis() / 1000.0);
        meta.set("ignored_topics", JSON.valueToTree(ignored));
        meta.set("groups", groups(selected)); ArrayNode topics = meta.putArray("topics");
        for (String name : selected) {
            TopicDescription desc = description(name); ObjectNode cfg = config(name);
            require(cfg.path("cleanup.policy").asText().equals("delete"), "当前仅支持 cleanup.policy=delete: " + name);
            ObjectNode topic = obj().put("name", name).put("topic_id", desc.topicId().toString()); topic.set("config", cfg);
            ArrayNode parts = topic.putArray("partitions");
            for (TopicPartitionInfo part : desc.partitions().stream().sorted(Comparator.comparingInt(TopicPartitionInfo::partition)).toList()) {
                long low = offset(name, part.partition(), false, true), high = offset(name, part.partition(), true, true);
                parts.add(obj().put("id", part.partition()).put("low", low).put("high", high).put("file", name + "-" + part.partition() + ".ndjson")
                    .put("count", 0).put("sha256", hex(digest().digest())).put("content_sha256", hex(digest().digest())));
            }
            topics.add(topic);
        }
        validatePositions(meta); return meta;
    }
    static void validatePositions(JsonNode meta) {
        require(meta.path("mode").asText().equals(MODE) && meta.path("topics").isArray() && meta.path("groups").isObject(), "Kafka 清单无效");
        Map<TopicPartition,JsonNode> parts = new HashMap<>(); Set<String> names = new HashSet<>();
        for (JsonNode topic : meta.path("topics")) {
            require(names.add(topicName(topic.path("name").asText())), "Topic 重复");
            require(IGNORED_TOPICS.contains(topic.path("name").asText())
                || topic.path("config").path("cleanup.policy").asText().equals("delete"), "不支持 compact Topic");
            int index = 0;
            for (JsonNode part : topic.path("partitions")) {
                require(part.path("id").asInt(-1) == index++ && part.path("low").asLong(-1) >= 0 && part.path("high").asLong(-1) >= part.path("low").asLong(), "分区范围无效");
                require(part.path("file").asText().equals(topic.path("name").asText() + "-" + part.path("id").asInt() + ".ndjson"), "分区文件名无效");
                parts.put(tp(topic, part), part);
            }
            require(index > 0, "分区清单为空");
        }
        meta.path("groups").fields().forEachRemaining(group -> {
            require(group.getValue().isArray() && !group.getValue().isEmpty(), "消费组清单无效"); Set<TopicPartition> seen = new HashSet<>();
            for (JsonNode item : group.getValue()) {
                TopicPartition tp = new TopicPartition(item.path("topic").asText(), item.path("partition").asInt(-1)); JsonNode part = parts.get(tp);
                require(part != null && seen.add(tp) && item.path("offset").isIntegralNumber() && item.path("offset").asLong() >= part.path("low").asLong()
                    && item.path("offset").asLong() <= part.path("high").asLong(), "消费位置不在备份范围内，不能安全恢复");
            }
        });
    }
    ObjectNode export(JsonNode request, Path root) throws Exception {
        ObjectNode meta = describe(request); Files.createDirectory(root);
        for (JsonNode topic : meta.path("topics")) for (JsonNode part : topic.path("partitions")) {
            TopicPartition tp = tp(topic, part); MessageDigest sha = digest(), dataSha = digest(); long[] count = {0};
            System.err.println("Kafka 导出 " + tp + " [" + part.path("low") + "," + part.path("high") + ")");
            try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(root.resolve(part.path("file").asText())))) {
                consume(tp, part.path("low").asLong(), part.path("high").asLong(), message -> {
                    JsonNode row = record(message); byte[] raw = line(row); require(raw.length <= MAX_LINE, "消息超过大小上限");
                    out.write(raw); sha.update(raw); dataSha.update(content(row)); count[0]++;
                });
            }
            ((ObjectNode)part).put("count", count[0]).put("sha256", hex(sha.digest())).put("content_sha256", hex(dataSha.digest()));
            require(offset(tp.topic(), tp.partition(), false, true) <= part.path("low").asLong(), "导出期间 retention 删除数据，请扩大保留期后重试");
        }
        for (JsonNode topic : meta.path("topics")) {
            TopicDescription current = description(topic.path("name").asText());
            require(current.topicId().toString().equals(topic.path("topic_id").asText()) && current.partitions().size() == topic.path("partitions").size()
                && config(current.name()).equals(topic.path("config")), "导出期间 Topic 身份、分区或配置发生变化");
        }
        Files.write(root.resolve("metadata.json"), line(meta)); return meta;
    }
    ObjectNode preflight(JsonNode meta) throws Exception {
        return preflight(meta, env("TASK_KAFKA_OVERWRITE", "false").equals("true"));
    }
    ObjectNode preflight(JsonNode meta, boolean overwrite) throws Exception {
        validatePositions(meta);
        meta = restorableMetadata(meta);
        if (meta.path("topics").isEmpty()) {
            ObjectNode result = obj().put("target_cluster_id", clusterId()); result.set("topics", arr());
            result.set("replace_topics", arr()); result.set("config_changes", obj()); return result;
        }
        validateLedgerRecovery(meta);
        idle(meta); ObjectNode existingGroups = groups(names(meta));
        for (Iterator<Map.Entry<String,JsonNode>> it = existingGroups.fields(); it.hasNext();) {
            Map.Entry<String,JsonNode> group = it.next();
            for (JsonNode old : group.getValue()) {
                boolean found = false;
                for (JsonNode item : meta.path("groups").path(group.getKey())) if (item.path("topic").equals(old.path("topic")) && item.path("partition").equals(old.path("partition"))) found = true;
                require(found, "目标存在备份之外的消费位置: " + group.getKey());
            }
        }
        ObjectNode changes = obj(); List<String> replacements = new ArrayList<>();
        Set<String> existing = admin.listTopics().names().get(35, TimeUnit.SECONDS);
        int replicas = Integer.parseInt(env("TASK_KAFKA_REPLICAS", "1"));
        require(replicas > 0 && replicas <= admin.describeCluster().nodes().get(35, TimeUnit.SECONDS).size(), "目标 Broker 数量或副本数不足");
        for (JsonNode topic : meta.path("topics")) {
            String name = topic.path("name").asText();
            int minIsr = Integer.parseInt(topic.path("config").path("min.insync.replicas").asText()); require(minIsr <= replicas, "目标副本数不足");
            if (!existing.contains(name)) continue;
            TopicDescription live = description(name);
            boolean replace = live.partitions().size() != topic.path("partitions").size();
            require(!replace || overwrite, "目标 Topic 分区数量不同: " + name);
            ObjectNode current = config(name), differences = obj();
            for (String key : CONFIGS) if (!current.path(key).equals(topic.path("config").path(key))) {
                differences.set(key, obj().set("target", current.path(key)));
                ((ObjectNode)differences.path(key)).set("backup", topic.path("config").path(key));
            }
            if (!differences.isEmpty()) changes.set(name, differences);
            for (TopicPartitionInfo part : live.partitions()) {
                require(part.replicas().size() >= minIsr || overwrite, "目标分区副本数不足");
                if (part.replicas().size() < minIsr || offset(name, part.partition(), false, false) != 0
                    || offset(name, part.partition(), true, false) != 0) replace = true;
            }
            require(!replace || overwrite, "目标 Topic 非全新空分区；覆盖恢复需要 --overwrite（将删除并重建）: " + name);
            if (replace) replacements.add(name);
        }
        ObjectNode result = obj().put("target_cluster_id", clusterId()); result.set("topics", JSON.valueToTree(names(meta)));
        result.set("config_changes", changes); result.set("replace_topics", JSON.valueToTree(replacements)); return result;
    }
    static List<String> ledgerRecoveryTopics(JsonNode meta) {
        List<String> result = new ArrayList<>();
        for (JsonNode topic : meta.path("topics")) if (LEDGER_GROUPS.containsKey(topic.path("name").asText())) {
            for (JsonNode part : topic.path("partitions")) if (part.path("high").asLong() > 0) { result.add(topic.path("name").asText()); break; }
        }
        return result;
    }
    static void validateLedgerRecovery(JsonNode meta) {
        for (JsonNode topic : meta.path("topics")) {
            String name = topic.path("name").asText();
            if (!LEDGER_GROUPS.containsKey(name)) continue;
            for (JsonNode part : topic.path("partitions")) {
                long high = part.path("high").asLong();
                require(part.path("low").asLong() == 0 && part.path("count").asLong() == high,
                    "审计/Evidence 保留范围不完整或存在 offset 空洞，不能保持账本 Kafka 坐标: " + name);
            }
        }
    }
    static JsonNode verifyFiles(Path root) throws Exception {
        require(Files.size(root.resolve("metadata.json")) <= 8 * 1024 * 1024, "清单过大");
        JsonNode meta = JSON.readTree(root.resolve("metadata.json").toFile()); validatePositions(meta);
        Set<String> expected = new HashSet<>(Set.of("metadata.json"));
        for (JsonNode topic : meta.path("topics")) for (JsonNode part : topic.path("partitions")) {
            String filename = part.path("file").asText(); require(expected.add(filename), "重复分区文件");
            MessageDigest sha = digest(), dataSha = digest(); long count = 0, previous = -1;
            try (InputStream source = new BufferedInputStream(new DigestInputStream(Files.newInputStream(root.resolve(filename)), sha))) {
                JsonNode row;
                while ((row = readLine(source)) != null) {
                    long position = row.path("offset").asLong(-1);
                    require(row.path("offset").isIntegralNumber() && position > previous && position >= part.path("low").asLong() && position < part.path("high").asLong(), "消息 offset 乱序或越界");
                    unb64(row.get("key")); unb64(row.get("value")); require(row.path("headers").isArray(), "消息 headers 无效");
                    for (JsonNode header : row.path("headers")) { require(header.isArray() && header.size() == 2 && header.get(0).isTextual(), "消息 header 无效"); unb64(header.get(1)); }
                    require(row.path("timestamp").isIntegralNumber(), "消息时间无效");
                    previous = position; dataSha.update(content(row)); count++;
                }
            }
            require(count == part.path("count").asLong(-1) && hex(sha.digest()).equals(part.path("sha256").asText())
                && hex(dataSha.digest()).equals(part.path("content_sha256").asText()), "消息数量或校验和不符");
        }
        try (java.util.stream.Stream<Path> files = Files.list(root)) {
            List<Path> found = files.toList(); require(found.size() == expected.size(), "包内文件列表不匹配");
            for (Path file : found) require(Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS) && expected.contains(file.getFileName().toString()), "包内存在异常文件");
        }
        return meta;
    }
    ObjectNode restore(Path root) throws Exception {
        return restore(root, env("TASK_KAFKA_OVERWRITE", "false").equals("true"));
    }
    ObjectNode restore(Path root, boolean overwrite) throws Exception {
        JsonNode meta = restorableMetadata(verifyFiles(root)); ObjectNode plan = preflight(meta, overwrite);
        if (meta.path("topics").isEmpty()) {
            ObjectNode result = obj().put("result", "success").put("records", 0);
            result.set("groups", arr()); result.set("replaced_topics", arr()); return result;
        }
        require(ledgerRecoveryTopics(meta).isEmpty() || env("TASK_KAFKA_LEDGER_RESTORED", "false").equals("true"),
            "恢复审计/Evidence 记录前需先恢复同批 MariaDB 账本，并传入 --kafka-ledger-restored 确认");
        List<String> replacements = new ArrayList<>(); plan.path("replace_topics").forEach(name -> replacements.add(name.asText()));
        if (!replacements.isEmpty()) {
            idle(meta);
            System.err.println("覆盖恢复：删除并重建 Topic " + replacements);
            admin.deleteTopics(replacements).all().get(65, TimeUnit.SECONDS);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (true) {
                Set<String> live = admin.listTopics().names().get(35, TimeUnit.SECONDS);
                if (Collections.disjoint(live, replacements)) break;
                require(System.nanoTime() < deadline, "等待 Topic 删除完成超时，业务必须保持停止");
                Thread.sleep(500);
            }
        }
        Set<String> existing = admin.listTopics().names().get(35, TimeUnit.SECONDS);
        for (JsonNode topic : meta.path("topics")) if (!existing.contains(topic.path("name").asText())) {
            Map<String,String> cfg = new HashMap<>(); topic.path("config").fields().forEachRemaining(entry -> cfg.put(entry.getKey(), entry.getValue().asText()));
            admin.createTopics(List.of(new NewTopic(topic.path("name").asText(), topic.path("partitions").size(), Short.parseShort(env("TASK_KAFKA_REPLICAS", "1"))).configs(cfg))).all().get(35, TimeUnit.SECONDS);
        }
        // Preflight is read-only. Apply only the selected backup keys after all safety checks.
        preflight(meta, false);
        Map<ConfigResource,Collection<AlterConfigOp>> updates = new LinkedHashMap<>();
        for (JsonNode topic : meta.path("topics")) {
            String name = topic.path("name").asText(); ObjectNode current = config(name);
            List<AlterConfigOp> operations = new ArrayList<>();
            for (String key : CONFIGS) if (!current.path(key).equals(topic.path("config").path(key)))
                operations.add(new AlterConfigOp(new ConfigEntry(key, topic.path("config").path(key).asText()), AlterConfigOp.OpType.SET));
            if (!operations.isEmpty()) updates.put(new ConfigResource(ConfigResource.Type.TOPIC, name), operations);
        }
        if (!updates.isEmpty()) admin.incrementalAlterConfigs(updates).all().get(35, TimeUnit.SECONDS);
        for (JsonNode topic : meta.path("topics")) require(config(topic.path("name").asText()).equals(topic.path("config")),
            "目标 Topic 配置未能对齐备份: " + topic.path("name").asText());
        preflight(meta, false); Map<String,Map<TopicPartition,OffsetAndMetadata>> mapped = new LinkedHashMap<>();
        meta.path("groups").fieldNames().forEachRemaining(g -> mapped.put(g, new HashMap<>())); Map<TopicPartition,Long> ends = new HashMap<>(); long total = 0;
        Properties cfg = new Properties(); cfg.putAll(props); cfg.put("enable.idempotence", "true"); cfg.put("acks", "all");
        cfg.put("buffer.memory", "16777216"); cfg.put("max.request.size", "16777216"); cfg.put("delivery.timeout.ms", "60000");
        try (KafkaProducer<byte[],byte[]> producer = new KafkaProducer<>(cfg, new ByteArraySerializer(), new ByteArraySerializer())) {
            for (JsonNode topic : meta.path("topics")) for (JsonNode part : topic.path("partitions")) {
                TopicPartition tp = tp(topic, part); Map<String,Long> positions = new HashMap<>(), translated = new HashMap<>();
                meta.path("groups").fields().forEachRemaining(group -> { for (JsonNode item : group.getValue()) if (item.path("topic").asText().equals(tp.topic()) && item.path("partition").asInt() == tp.partition()) positions.put(group.getKey(), item.path("offset").asLong()); });
                long end = 0; System.err.println("Kafka 导入 " + tp + ": " + part.path("count") + " 条");
                try (InputStream source = new BufferedInputStream(Files.newInputStream(root.resolve(part.path("file").asText())))) {
                    JsonNode row;
                    while ((row = readLine(source)) != null) {
                        List<Header> headers = new ArrayList<>(); for (JsonNode header : row.path("headers")) headers.add(new RecordHeader(header.get(0).asText(), unb64(header.get(1))));
                        Long timestamp = topic.path("config").path("message.timestamp.type").asText().equals("CreateTime") && row.path("timestamp").asLong() >= 0 ? Long.valueOf(row.path("timestamp").asLong()) : null;
                        RecordMetadata ack = producer.send(new ProducerRecord<>(tp.topic(), tp.partition(), timestamp, unb64(row.get("key")), unb64(row.get("value")), headers)).get(65, TimeUnit.SECONDS);
                        require(ack.offset() == end, "检测到目标并发写入；禁止直接重试已部分导入的 Topic");
                        require(!LEDGER_GROUPS.containsKey(tp.topic()) || ack.offset() == row.path("offset").asLong(),
                            "审计/Evidence 恢复的 Kafka 坐标发生变化，必须保持业务停止");
                        for (Map.Entry<String,Long> position : positions.entrySet()) if (!translated.containsKey(position.getKey()) && row.path("offset").asLong() >= position.getValue()) translated.put(position.getKey(), ack.offset());
                        end = ack.offset() + 1;
                    }
                }
                MessageDigest dataSha = digest(); long[] count = {0};
                consume(tp, 0, end, message -> { dataSha.update(content(record(message))); count[0]++; });
                require(count[0] == part.path("count").asLong() && hex(dataSha.digest()).equals(part.path("content_sha256").asText()), "消息回读校验失败；消费进度尚未恢复");
                for (String group : positions.keySet()) mapped.get(group).put(tp, new OffsetAndMetadata(translated.getOrDefault(group, end)));
                ends.put(tp, end); total += count[0];
            }
        }
        for (Map.Entry<TopicPartition,Long> end : ends.entrySet()) require(offset(end.getKey().topic(), end.getKey().partition(), false, false) == 0
            && offset(end.getKey().topic(), end.getKey().partition(), true, false) == end.getValue(), "目标分区发生变化；不更新消费进度");
        idle(meta);
        for (Map.Entry<String,Map<TopicPartition,OffsetAndMetadata>> group : mapped.entrySet()) {
            admin.alterConsumerGroupOffsets(group.getKey(), group.getValue()).all().get(35, TimeUnit.SECONDS);
            Map<TopicPartition,OffsetAndMetadata> actual = admin.listConsumerGroupOffsets(group.getKey()).partitionsToOffsetAndMetadata().get(35, TimeUnit.SECONDS);
            for (Map.Entry<TopicPartition,OffsetAndMetadata> item : group.getValue().entrySet()) require(actual.containsKey(item.getKey()) && actual.get(item.getKey()).offset() == item.getValue().offset(), "消费组进度回读不符: " + group.getKey());
        }
        ObjectNode result = obj().put("result", "success").put("records", total); result.set("groups", JSON.valueToTree(mapped.keySet()));
        result.set("replaced_topics", JSON.valueToTree(replacements)); return result;
    }
    public static void main(String[] args) {
        // kubectl exec inherits the image's locale, which can be US-ASCII on Java 17.
        System.setErr(new PrintStream(System.err, true, StandardCharsets.UTF_8));
        try {
            String operation = args[0]; JsonNode result;
            if (operation.equals("verify")) result = verifyFiles(Path.of(args[1]));
            else try (OpenBknKafkaBackup client = new OpenBknKafkaBackup()) {
                switch (operation) {
                    case "health": result = obj().put("mode", MODE).put("target_cluster_id", client.clusterId()).set("topics", arr()); break;
                    case "check": result = client.describe(request()); break;
                    case "export": result = client.export(request(), Path.of(args[1])); break;
                    case "restore-check": result = client.preflight(request()); break;
                    case "restore": result = client.restore(Path.of(args[1])); break;
                    default: throw new IllegalArgumentException("未知操作");
                }
            }
            System.out.write(line(result)); System.out.flush();
        } catch (Exception error) {
            Throwable cause = error; while (cause.getCause() != null) cause = cause.getCause();
            String warning = args.length > 0 && args[0].equals("restore")
                ? "；若已写入部分数据，请保持业务停止，勿向非空 Topic 直接重试。" : "";
            System.err.println("kafka: " + cause.getClass().getSimpleName() + ": " + cause.getMessage()
                + warning); System.exit(1);
        }
    }
}
