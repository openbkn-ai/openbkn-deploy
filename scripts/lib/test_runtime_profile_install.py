#!/usr/bin/env python3
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

"""Exercise the ordinary installer with real Helm and a private local API fixture."""
import base64
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from test_runtime_profile import profile

ROOT = Path(__file__).resolve().parents[2]

SHIM = r'''#!/usr/bin/env python3
import json, os, pathlib, re, subprocess, sys
path=pathlib.Path(os.environ['RUNTIME_TEST_STATE'])
state=json.loads(path.read_text());args=sys.argv[1:];tool=pathlib.Path(sys.argv[0]).name
if tool=='kubectl' and args[0]=='get':
    value=state['objects'].get(args[1]+'|'+args[2])
    if value: print(json.dumps(value))
elif tool=='kubectl' and args[0] in ('create','replace') and args[1:]==['-f','-']:
    value=json.load(sys.stdin);state['revision']+=1;value['metadata']['resourceVersion']=str(state['revision'])
    state['objects'][value['kind'].lower()+'|'+value['metadata']['name']]=value
    state['writes'].append(tool+' '+args[0]+' '+value['kind'])
elif tool=='kubectl' and args[:2]==['create','namespace']:
    pass
elif tool=='kubectl' and args[0]=='patch':
    operations=json.loads(pathlib.Path(args[-1]).read_text());value=state['objects'][args[1]+'|'+args[2]]
    if value['metadata']['resourceVersion']!=operations[0]['value']: sys.exit(1)
    value['spec']['template']['spec']['containers'][0]['env']=operations[1]['value']
    state['revision']+=1;value['metadata']['resourceVersion']=str(state['revision']);state['writes'].append('kubectl patch')
elif tool=='helm' and args[0]=='list':
    print('\n'.join(release for release in state['values'] if re.fullmatch(args[-1], release)))
elif tool=='helm' and args[:2]==['get','values']:
    print(json.dumps(state['values'][args[2]]))
elif tool=='helm' and args[0] in ('show','package'):
    result=subprocess.run([os.environ['RUNTIME_TEST_HELM'],*args],capture_output=True,text=True)
    if result.returncode: sys.stderr.write(result.stderr);sys.exit(result.returncode)
    sys.stdout.write(result.stdout)
elif tool=='helm' and args[0]=='status':
    print('{"info":{"status":"deployed"}}')
elif tool=='helm' and args[0]=='template':
    if state.get('fail_render'): print('old chart without profile support')
    else:
        result=subprocess.run([os.environ['RUNTIME_TEST_HELM'],*args],capture_output=True,text=True)
        if result.returncode: sys.stderr.write(result.stderr);sys.exit(result.returncode)
        state['renders'][args[1]]=result.stdout;sys.stdout.write(result.stdout)
elif tool=='helm' and args[:2]==['upgrade','--install']:
    release=args[2];files=[args[i+1] for i,x in enumerate(args) if x=='-f']
    values=json.loads(pathlib.Path(next(p for p in files if p.endswith('.values.json'))).read_text())
    state['values'][release]=values;state['writes'].append('helm upgrade '+release)
    entry=json.loads(os.environ['RUNTIME_TEST_PROFILE_JSON'])['releases'][release]
    workload=entry.get('workload')
    if workload:
        env=list(values.get('extraEnv',[]))
        for key,name in entry.get('valueEnv',{}).items():
            value=values
            for part in key.split('.'): value=value[part]
            env.append({'name':name,'value':value})
        state['revision']+=1
        state['objects']['deployment|'+workload['name']]={'metadata':{'resourceVersion':str(state['revision'])},'spec':{'template':{'spec':{'containers':[{'name':workload['container'],'env':env}]}}}}
else: raise SystemExit('Unexpected fixture command: '+tool+' '+args[0])
path.write_text(json.dumps(state));path.chmod(0o600)
'''


@unittest.skipUnless(shutil.which("helm"), "real Helm is required")
class RuntimeInstallerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.helm = shutil.which("helm")
        for tool in ("kubectl", "helm"):
            executable = self.bin / tool
            executable.write_text(SHIM)
            executable.chmod(0o700)
        self.state = self.root / "cluster.json"
        self.state.write_text(json.dumps({"objects": {}, "values": {}, "renders": {}, "writes": [], "revision": 0}))
        self.config = self.root / "config.yaml"
        self.config.write_text("namespace: isolated\n")
        self.profile = profile()
        self.profile["platformVersion"] = "0.2.2"
        self.profile["releases"] = dict(zip(("bkn-backend", "agent-retrieval"), self.profile["releases"].values()))
        self.profile["releases"]["bkn-backend"]["workload"] = {"name": "bkn-backend", "container": "bkn-backend"}
        self.profile["releases"]["agent-retrieval"]["workload"] = {"name": "agent-retrieval", "container": "agent-retrieval"}
        self.charts = self.root / "charts"
        self.charts.mkdir()
        self.chart_paths = {}
        supplied = os.environ.get("OPENBKN_TEST_RUNTIME_PROFILE")
        if supplied:
            self.profile = json.loads(Path(supplied).read_text())
        self.input = self.root / "profile.json"
        self.input.write_text(json.dumps(self.profile))
        for release in ("bkn-backend", "agent-retrieval"):
            supplied_chart = os.environ.get("OPENBKN_TEST_" + release.upper().replace("-", "_") + "_CHART")
            chart = Path(supplied_chart) if supplied_chart else self.fixture_chart(release)
            self.chart_paths[release] = str(chart)
            subprocess.run([self.helm, "package", str(chart), "--destination", str(self.charts)], check=True, capture_output=True)
        self.script = self.root / "install.sh"
        self.script.write_text(r'''
set -euo pipefail
SCRIPT_DIR="$RUNTIME_TEST_ROOT"
source "$SCRIPT_DIR/scripts/lib/common.sh"
source "$SCRIPT_DIR/scripts/services/openbkn.sh"
log_info(){ :; }; log_warn(){ :; }; log_error(){ echo "$*" >&2; }
_openbkn_resolve_latest_manifest(){ :; }
_openbkn_require_version_manifest(){ :; }
_openbkn_apply_default_set_values(){ :; }
ensure_platform_prerequisites(){ :; }; reconcile_openbkn_firewall(){ :; }
setup_dockerhub_mirror(){ :; }; init_openbkn_databases(){ :; }
_openbkn_resolve_target_namespace(){ echo isolated; }
_openbkn_resolve_charts_dir(){ [[ "$RUNTIME_TEST_MODE" != local ]] || echo "$RUNTIME_TEST_CHARTS"; }
_openbkn_release_names(){ printf 'bkn-backend\nagent-retrieval\n'; }
_openbkn_release_exists(){ return 1; }
_openbkn_resolve_release_version(){ echo 0.2.2; }; _openbkn_resolve_chart_name(){ echo "$1"; }
_manifest_read_top_level_field(){ echo 0.2.2; }
_openbkn_trace_kafka_brokers(){ echo broker:9092; }
should_skip_upgrade_same_chart_version(){ return 0; }
_openbkn_drop_literal_env_now_from_secret(){ :; }
_openbkn_uninstall_retired_releases(){ :; }; gen_install_status_json(){ :; }
config_yaml_top_field(){ :; }; _read_access_address_field(){ :; }
_openbkn_should_show_bkn_safe_initial_password(){ return 1; }
parse_manifest_source(){ :; }; log_chart_source(){ :; }; ensure_chart_source(){ :; }
build_chart_ref(){ if [[ "$2" == bkn-backend ]]; then echo "$RUNTIME_TEST_BACKEND_CHART"; else echo "$RUNTIME_TEST_RETRIEVAL_CHART"; fi; }
CORE_NAMESPACE=isolated
CONFIG_YAML_PATH="$RUNTIME_TEST_CONFIG"
CORE_VERSION_MANIFEST_FILE=fixture
CORE_SET_VALUES=(image.registry=community.example)
OFFLINE_MODE=false
OPENBKN_SKIP_PLATFORM_BOOTSTRAP=false
ONBOARD_SKIP_OSS_STORAGE=true
HELM_CHART_VERSION=0.2.2
HELM_CHART_REPO_NAME=fixture
CORE_RUNTIME_PROFILE="$RUNTIME_TEST_INPUT"
install_openbkn
''')

    def fixture_chart(self, release):
        chart = self.root / release
        (chart / "templates").mkdir(parents=True)
        (chart / "Chart.yaml").write_text(f"apiVersion: v2\nname: {release}\nversion: 0.2.2\n")
        (chart / "values.yaml").write_text("{}\n")
        (chart / "templates/runtime.yaml").write_text('''apiVersion: v1
kind: ConfigMap
metadata:
  name: fixture
  namespace: {{ .Values.namespace }}
data:
  image: {{ .Values.image | toJson | quote }}
  env: {{ .Values.extraEnv | toJson | quote }}
  extension: {{ .Values.extensionJSON | default "" | quote }}
  upstream: {{ (.Values.upstream | default dict).url | default "" | quote }}
''')
        return chart

    def install(self, mode="local", provided=True):
        env = {**os.environ, "PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
               "RUNTIME_TEST_ROOT": str(ROOT), "RUNTIME_TEST_STATE": str(self.state), "RUNTIME_TEST_HELM": self.helm,
               "RUNTIME_TEST_MODE": mode, "RUNTIME_TEST_CHARTS": str(self.charts), "RUNTIME_TEST_CONFIG": str(self.config),
               "RUNTIME_TEST_INPUT": str(self.input) if provided else "", "RUNTIME_TEST_PROFILE_JSON": json.dumps(self.profile),
               "RUNTIME_TEST_BACKEND_CHART": self.chart_paths["bkn-backend"], "RUNTIME_TEST_RETRIEVAL_CHART": self.chart_paths["agent-retrieval"]}
        return subprocess.run(["bash", str(self.script)], env=env, capture_output=True, text=True, timeout=90)

    def load(self):
        return json.loads(self.state.read_text())

    def test_ordinary_local_and_repository_installs_preserve_keys_and_reconcile_same_version(self):
        for mode in ("local", "repo"):
            with self.subTest(mode=mode):
                first = self.install(mode)
                self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
                state = self.load()
                name = self.profile["secrets"][0]["name"]
                keys = copy.deepcopy(state["objects"]["secret|" + name]["data"])
                previous = len(state["writes"])
                again = self.install(mode, provided=False)
                self.assertEqual(again.returncode, 0, again.stdout + again.stderr)
                state = self.load()
                self.assertEqual(keys, state["objects"]["secret|" + name]["data"])
                self.assertIn("helm upgrade bkn-backend", state["writes"][previous:])
                self.assertIn("helm upgrade agent-retrieval", state["writes"][previous:])
                expected_registry = self.profile["releases"]["bkn-backend"]["values"]["image"]["registry"]
                self.assertIn(expected_registry, state["renders"]["bkn-backend"])
                for value in keys.values():
                    self.assertNotIn(value, first.stdout + first.stderr + again.stdout + again.stderr)

    def test_corrupt_retained_keys_stop_the_normal_installer_before_any_workload_changes(self):
        self.assertEqual(self.install().returncode, 0)
        state = self.load()
        name = self.profile["secrets"][0]["name"]
        key = next(iter(state["objects"]["secret|" + name]["data"]))
        state["objects"]["secret|" + name]["data"][key] = "invalid"
        before = list(state["writes"])
        self.state.write_text(json.dumps(state))
        result = self.install(provided=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("restore", result.stderr)
        self.assertEqual(before, self.load()["writes"])

    def test_incompatible_chart_stops_before_secret_or_profile_creation(self):
        state = self.load()
        state["fail_render"] = True
        self.state.write_text(json.dumps(state))
        result = self.install()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("update the platform Chart", result.stderr)
        self.assertEqual(self.load()["writes"], [])

    def test_helm_and_live_mapped_addresses_survive_local_and_repository_upgrades(self):
        entry = self.profile["releases"]["agent-retrieval"]
        field = next(key for key in entry["valueEnv"] if key not in entry.get("exclusiveValues", []))
        env_name = entry["valueEnv"][field]
        for mode in ("local", "repo"):
            with self.subTest(mode=mode):
                first = self.install(mode)
                self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
                state = self.load()
                desired = state["values"]["agent-retrieval"]
                for part in field.split(".")[:-1]:
                    desired = desired[part]
                desired[field.split(".")[-1]] = "https://helm-custom.example:3443"
                workload = state["objects"]["deployment|agent-retrieval"]
                workload["spec"]["template"]["spec"]["containers"][0]["env"] = []
                self.state.write_text(json.dumps(state))
                for patched in (False, True):
                    address = "https://patched-custom.example:3443" if patched else "https://helm-custom.example:3443"
                    if patched:
                        state = self.load()
                        env = state["objects"]["deployment|agent-retrieval"]["spec"]["template"]["spec"]["containers"][0]["env"]
                        next(e for e in env if e["name"] == env_name)["value"] = address
                        self.state.write_text(json.dumps(state))
                    result = self.install(mode, provided=False)
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    state = self.load()
                    actual = state["values"]["agent-retrieval"]
                    for part in field.split("."):
                        actual = actual[part]
                    self.assertEqual(actual, address)
                    self.assertIn(address, state["renders"]["agent-retrieval"])

    def test_new_profile_adopts_a_manual_address_from_an_older_profile(self):
        first = self.install()
        self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
        state = self.load()
        secret = state["objects"]["secret|openbkn-runtime-profile"]
        old = json.loads(base64.b64decode(secret["data"]["profile.json"]))
        entry = old["releases"]["agent-retrieval"]
        field = next(key for key in entry["valueEnv"] if key not in entry.get("exclusiveValues", []))
        env_name = entry["valueEnv"].pop(field)
        for values in (entry["values"], state["values"]["agent-retrieval"]):
            for part in field.split(".")[:-1]:
                values = values[part]
            del values[field.split(".")[-1]]
        secret["data"]["profile.json"] = base64.b64encode(json.dumps(old).encode()).decode()
        env = state["objects"]["deployment|agent-retrieval"]["spec"]["template"]["spec"]["containers"][0]["env"]
        next(e for e in env if e["name"] == env_name)["value"] = "https://older-patch.example:3443"
        self.state.write_text(json.dumps(state))
        result = self.install(provided=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("https://older-patch.example:3443", self.load()["renders"]["agent-retrieval"])

    def test_empty_chart_value_uses_provider_default(self):
        result = self.install()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        state = self.load()
        entry = self.profile["releases"]["agent-retrieval"]
        field = next(key for key in entry["valueEnv"] if key not in entry.get("exclusiveValues", []))
        expected = entry["values"]
        for part in field.split("."):
            expected = expected[part]
        values = state["values"]["agent-retrieval"]
        for part in field.split(".")[:-1]:
            values = values[part]
        values[field.split(".")[-1]] = ""
        env = state["objects"]["deployment|agent-retrieval"]["spec"]["template"]["spec"]["containers"][0]["env"]
        env[:] = [e for e in env if e["name"] != entry["valueEnv"][field]]
        self.state.write_text(json.dumps(state))
        result = self.install(provided=False)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        actual = self.load()["values"]["agent-retrieval"]
        for part in field.split("."):
            actual = actual[part]
        self.assertEqual(actual, expected)
        self.assertIn(expected, self.load()["renders"]["agent-retrieval"])


if __name__ == "__main__":
    unittest.main()
