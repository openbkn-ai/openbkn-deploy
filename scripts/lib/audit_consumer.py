#!/usr/bin/env python3
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

"""Prepare and validate the complete installer's System Audit deployment."""

import copy
import json
import subprocess
import sys

from audit_kafka_topic_job import render as render_topic_job


def preserved_values(values):
    values = values or {}
    audit = values.get("kafkaConsumers", {}).get("audit")
    if not isinstance(audit, dict):
        return {}
    # Carry only the supported consumer configuration, never inline credentials
    # or unrelated old chart defaults. Current values/--set still take precedence.
    keys = ("enabled", "brokers", "topic", "consumerGroup", "saslMechanism", "existingSecret")
    preserved = {key: audit[key] for key in keys if key in audit}
    if isinstance(preserved.get("existingSecret"), dict):
        secret = preserved["existingSecret"]
        preserved["existingSecret"] = {key: secret[key] for key in
                                       ("name", "usernameKey", "passwordKey") if key in secret}
    return {"kafkaConsumers": {"audit": preserved}}


def defaults(brokers, mechanism, secret):
    connection = {"enabled": True, "brokers": [brokers] if brokers else [],
                  "saslMechanism": mechanism,
                  "existingSecret": {"name": secret, "usernameKey": "username", "passwordKey": "password"}}
    consumer = copy.deepcopy(connection)
    consumer.update(topic="openbkn.audit.v1", consumerGroup="bkn-trace-audit-ledger-v1")
    connection["environment"] = "development"
    return {"core": {"autoMigrate": True}, "kafkaConsumers": {"audit": consumer},
            "auditPublisher": connection}


def preserved_system_values(values):
    values = values or {}
    result = preserved_values(values)
    publisher = values.get("auditPublisher", {})
    keys = ("enabled", "environment", "existingSecret")
    result["auditPublisher"] = {key: copy.deepcopy(publisher[key]) for key in keys if key in publisher}
    for section in (result.get("kafkaConsumers", {}).get("audit", {}), result["auditPublisher"]):
        # Helm user values include defaults emitted by an earlier installer.
        # Resolve MQ-derived connections from today's config/CLI, not that copy.
        section.pop("brokers", None)
        section.pop("saslMechanism", None)
        if isinstance(section.get("existingSecret"), dict):
            section["existingSecret"] = {key: section["existingSecret"][key] for key in
                                         ("name", "usernameKey", "passwordKey") if key in section["existingSecret"]}
        # A formerly disabled component is a deployment gap in a complete
        # platform install. Keep useful connection fields, not empty defaults.
        if section.get("enabled") is False:
            for key in list(section):
                if key == "enabled" or not section[key] or (key == "existingSecret" and not section[key].get("name")):
                    del section[key]
    return result


def rendered_system_values(rendered):
    # Helm has already merged all input files/CLI. release.config omits Chart
    # defaults; combine those maps only, without reimplementing input priority.
    def merge(base, overrides):
        result = copy.deepcopy(base)
        for key, value in overrides.items():
            if isinstance(value, dict) and isinstance(result.get(key), dict):
                result[key] = merge(result[key], value)
            else:
                result[key] = copy.deepcopy(value)
        return result
    return merge(rendered.get("chart", {}).get("values", {}), rendered.get("config", {}))


def same_system_config(desired, installed):
    def config(values):
        core = values.get("core", {})
        return {"consumer": values.get("kafkaConsumers", {}).get("audit", {}),
                "publisher": values.get("auditPublisher", {}),
                "core": {key: core.get(key) for key in ("store", "autoMigrate", "mariadb")}}
    return config(desired) == config(installed or {})


def validate_system(values, namespace):
    consumer = values.get("kafkaConsumers", {}).get("audit", {})
    publisher = values.get("auditPublisher", {})
    for name, section in (("Audit consumer", consumer), ("Core Audit publisher", publisher)):
        if any(key in section for key in ("username", "password")):
            raise ValueError(f"{name} must use Secret references, not inline credentials")
        if section.get("enabled") is not True:
            raise ValueError(f"{name} must be enabled for a complete OpenBKN installation")
        brokers = section.get("brokers")
        if not isinstance(brokers, list) or not brokers or not all(isinstance(b, str) and b.strip() for b in brokers):
            raise ValueError(f"{name} requires Kafka brokers")
    if (not isinstance(consumer.get("consumerGroup"), str) or not consumer["consumerGroup"].strip()
            or consumer.get("consumerGroup") == values.get("kafkaConsumers", {}).get("evidence", {}).get("consumerGroup")
            or consumer.get("topic") != "openbkn.audit.v1"):
        raise ValueError("Audit consumer requires an independent group and openbkn.audit.v1 topic")
    if publisher.get("saslMechanism") != "PLAIN":
        raise ValueError("Core Audit publisher requires the existing PLAIN Kafka path")
    if publisher.get("environment") not in ("development", "test", "staging", "production"):
        raise ValueError("auditPublisher.environment must be development/test/staging/production")
    if consumer.get("saslMechanism") not in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"):
        raise ValueError("Audit consumer requires a supported Kafka SASL mechanism")
    publisher_secret = publisher.get("existingSecret", {})
    core = values.get("core", {})
    if core.get("store") != "mariadb" or core.get("autoMigrate") is not True:
        raise ValueError("System Audit requires Core MariaDB and autoMigrate=true")
    validate(values, namespace)
    check_secret(namespace, publisher_secret.get("name"),
                 [publisher_secret.get("usernameKey", "username"), publisher_secret.get("passwordKey", "password")],
                 component="Core Audit publisher")


def check_secret(namespace, name, keys, component="Audit consumer"):
    if not name:
        raise ValueError("{} requires an existing Secret reference".format(component))
    result = subprocess.run(
        ["kubectl", "get", "secret", name, "-n", namespace, "-o", "json", "--request-timeout=10s"],
        capture_output=True, text=True, check=False, timeout=15,
    )
    if result.returncode:
        raise ValueError(f"{component} cannot read Secret {namespace}/{name}; create it before installation")
    data = json.loads(result.stdout).get("data", {})
    for key in keys:
        if not key or not data.get(key):
            raise ValueError(f"{component} Secret {namespace}/{name} is missing a non-empty key {key}")


def validate(values, namespace):
    audit = values.get("kafkaConsumers", {}).get("audit", {})
    if not audit.get("enabled"):
        return
    # Helm renders and validates brokers/group/topic/SASL/Core before this step.
    secret = audit.get("existingSecret", {})
    check_secret(namespace, secret.get("name"),
                 [secret.get("usernameKey", "username"), secret.get("passwordKey", "password")])
    mariadb = values.get("core", {}).get("mariadb", {})
    check_secret(namespace, mariadb.get("existingSecret", "bkn-trace-core-mariadb"),
                 [mariadb.get("dsnKey", "dsn")])



# Only Chart-owned user settings are carried into a same-version Audit repair.
# Never feed --all (historical Chart defaults) back into Helm.
NON_AUDIT_KEYS = (
    "nameOverride", "fullnameOverride", "replicaCount", "logLevel", "image",
    "imagePullSecrets", "serviceAccount", "podAnnotations", "podLabels", "service",
    "networkPolicy", "ingress", "evidence", "kafkaConsumers", "bknSafe",
    "businessResolver", "enterpriseBusinessProvenance", "observability", "core",
    "resources", "nodeSelector", "tolerations", "affinity", "opensearch",
)


def non_audit_values(values):
    result = {key: copy.deepcopy(values[key]) for key in NON_AUDIT_KEYS if key in values}
    consumers = result.get("kafkaConsumers")
    if isinstance(consumers, dict):
        consumers.pop("audit", None)
        if not consumers:
            result.pop("kafkaConsumers")
    core = result.get("core")
    if isinstance(core, dict):
        core.pop("autoMigrate", None)
        if not core:
            result.pop("core")
    return result


def differences(before, after, path=()):
    if isinstance(before, dict) and isinstance(after, dict):
        for key in sorted(before.keys() | after.keys()):
            if key not in before or key not in after:
                yield path + (key,), before.get(key), after.get(key)
            else:
                yield from differences(before[key], after[key], path + (key,))
    elif before != after:
        yield path, before, after


def lookup(values, path):
    for key in path:
        if not isinstance(values, dict) or key not in values:
            return False, None
        values = values[key]
    return True, values


def check_non_audit(desired, installed, explicit=None):
    # Helm, not this helper, interprets YAML/--set. The explicit render excludes
    # installer-generated non-Audit flags, so they cannot erase old user choices.
    explicit = non_audit_values(explicit or {})
    changed = []
    for path, _, current in differences(non_audit_values(installed), non_audit_values(desired)):
        found, requested = lookup(explicit, path)
        if not found or requested != current:
            changed.append(".".join(path))
    if changed:
        raise ValueError("same-version Audit repair would change non-Audit fields: " +
                         ", ".join(changed) + "; explicitly configure/review those fields first")


def release_state(rows):
    if not isinstance(rows, list) or len(rows) > 1:
        raise ValueError("cannot identify the Agent Observability release")
    if not rows:
        return {}
    row = rows[0]
    return {key: row.get(key) for key in ("name", "namespace", "revision", "status", "chart")}


def main():
    try:
        action = sys.argv[1]
        if action == "defaults":
            json.dump(defaults(*sys.argv[2:5]), sys.stdout)
            return 0
        values = json.load(sys.stdin)
        if action == "release-state":
            json.dump(release_state(values), sys.stdout, sort_keys=True)
        elif action == "preserve-non-audit":
            json.dump(non_audit_values(values or {}), sys.stdout)
        elif action in ("check-non-audit", "same-non-audit"):
            with open(sys.argv[2]) as installed_file:
                installed = json.load(installed_file)
            explicit = {}
            if action == "check-non-audit":
                with open(sys.argv[3]) as explicit_file:
                    explicit = json.load(explicit_file).get("config", {})
            check_non_audit(rendered_system_values(values), installed, explicit)
        elif action == "preserve-system":
            json.dump(preserved_system_values(values), sys.stdout)
        elif action == "preserve":
            json.dump(preserved_values(values), sys.stdout)
        elif action == "validate-system":
            validate_system(rendered_system_values(values), sys.argv[2])
        elif action == "render-topic-job":
            json.dump(render_topic_job(rendered_system_values(values), *sys.argv[2:6]), sys.stdout)
        elif action == "same-system":
            with open(sys.argv[2]) as installed:
                return 0 if same_system_config(rendered_system_values(values), json.load(installed)) else 1
        else:
            validate(values.get("config", {}), sys.argv[2])
    except subprocess.TimeoutExpired:
        print("Audit consumer preflight: Secret lookup timed out; check cluster connectivity before installation", file=sys.stderr)
        return 1
    except (ValueError, OSError, TypeError, KeyError) as error:
        print(f"Audit consumer preflight: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
