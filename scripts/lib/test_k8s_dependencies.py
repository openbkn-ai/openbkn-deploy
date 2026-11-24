"""Regression coverage for #87; all system commands are isolated or mocked."""
import os
import io
from pathlib import Path
import subprocess
import tempfile
import tarfile
import unittest


REPO = Path(__file__).resolve().parents[2]
MOCKS = r'''
GREEN='' YELLOW='' RED='' NC='' PREFLIGHT_REPORT_FILE=''
log_info() { echo "INFO $*"; }
log_warn() { echo "WARN $*"; }
log_error() { echo "ERROR $*"; }
apt-cache() { echo "Candidate: ${CANDIDATE:-(none)}"; }
dpkg-query() { return 1; }
curl() {
    echo "curl $*" >> "$CALLS"
    [[ "$FAIL" != download ]] || return 28
    local target=""
    while [[ $# -gt 0 ]]; do
        if [[ "$1" == --output ]]; then target="$2"; shift; fi
        shift
    done
    echo key > "$target"
}
gpg() {
    [[ "$FAIL" != key ]] || return 1
    [[ "$*" != *--show-keys* ]] || return 0
    while [[ $# -gt 0 ]]; do
        if [[ "$1" == --output ]]; then echo keyring > "$2"; return; fi
        shift
    done
}
apt-get() {
    echo "apt-get $*" >> "$CALLS"
    if [[ "$1" == update ]]; then [[ "$FAIL" != update ]]; return; fi
    [[ "$FAIL" != install ]] || return 100
    touch "$K8S_SYSTEM_ROOT/installed"
}
apt-mark() { echo "hold $*" >> "$CALLS"; [[ "$FAIL" != hold ]]; }
systemctl() { echo "systemctl $*" >> "$CALLS"; [[ "$FAIL" != service ]]; }
kubeadm() { [[ -f "$K8S_SYSTEM_ROOT/installed" && "$FAIL" != kubeadm ]] || return 1; echo v1.28.15; }
kubelet() { [[ -f "$K8S_SYSTEM_ROOT/installed" && "$FAIL" != kubelet ]] || return 1; echo Kubernetes v1.28.15; }
kubectl() {
    [[ -f "$K8S_SYSTEM_ROOT/installed" && "$FAIL" != kubectl ]] || return 1
    if [[ "$FAIL" == malformed ]]; then echo broken; else echo '{"clientVersion":{"gitVersion":"v1.28.15"}}'; fi
}
_k8s_ensure_cni_bin_plugins() { echo CNI >> "$CALLS"; [[ "$FAIL" != cni ]]; }
install_crictl() { echo crictl >> "$CALLS"; return 0; }
'''


class DependenciesTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.apt = self.root / 'etc/apt'
        (self.apt / 'sources.list.d').mkdir(parents=True)
        (self.apt / 'keyrings').mkdir()
        self.source = self.apt / 'sources.list.d/kubernetes.list'
        self.source.write_text('deb https://apt.kubernetes.io/ kubernetes-xenial main\n')
        self.key = self.apt / 'keyrings/kubernetes-apt-keyring.gpg'
        self.key.write_text('original key')
        self.calls = self.root / 'calls'
        self.calls.touch()

    def run_bash(self, body, fail='', candidate='(none)', prefix=''):
        script = (f'set -euo pipefail\nsource "{REPO}/scripts/services/k8s.sh"\n'
                  + prefix + MOCKS + '\n' + body)
        return subprocess.run(['bash', '-c', script], text=True, capture_output=True,
                              env={**os.environ, 'K8S_SYSTEM_ROOT': str(self.root),
                                   'CALLS': str(self.calls), 'FAIL': fail,
                                   'CANDIDATE': candidate, 'OFFLINE_MODE': 'false'},
                              timeout=10)

    def test_required_failures_stop_and_never_report_success(self):
        for failure in ['download', 'key', 'update', 'install', 'hold',
                        'kubeadm', 'kubelet', 'kubectl', 'malformed', 'service', 'cni']:
            with self.subTest(failure=failure):
                (self.root / 'installed').unlink(missing_ok=True)
                self.source.write_text('deb https://apt.kubernetes.io/ kubernetes-xenial main\n')
                self.calls.write_text('')
                result = self.run_bash('if install_kubernetes; then echo UNEXPECTED; else exit 23; fi', fail=failure)
                self.assertEqual(result.returncode, 23, result.stdout + result.stderr)
                self.assertNotIn('Kubernetes components installed', result.stdout)
                self.assertNotIn('UNEXPECTED', result.stdout)
                if failure != 'cni':
                    self.assertNotIn('CNI', self.calls.read_text())

    def test_failed_key_or_refresh_preserves_configuration(self):
        for failure in ['download', 'key', 'update']:
            with self.subTest(failure=failure):
                before = self.source.read_bytes(), self.key.read_bytes()
                result = self.run_bash('k8s_dependency_repo', fail=failure)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(before, (self.source.read_bytes(), self.key.read_bytes()))

    def test_migration_covers_both_legacy_urls_and_preserves_other_repos(self):
        other = self.apt / 'sources.list'
        other.write_text('deb https://example.org/ubuntu stable main\n'
                         'deb https://packages.cloud.google.com/apt kubernetes-xenial main\n')
        result = self.run_bash('install_kubernetes')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('Kubernetes components installed and verified', result.stdout)
        self.assertIn('pkgs.k8s.io/core:/stable:/v1.28/deb/', self.source.read_text())
        self.assertIn('deb https://example.org/ubuntu stable main', other.read_text())
        self.assertIn('# Disabled legacy Kubernetes source: deb https://packages', other.read_text())
        self.assertTrue(list(self.source.parent.glob('kubernetes.list.bak.*')))
        self.assertTrue(list(other.parent.glob('sources.list.bak.*')))
        calls = self.calls.read_text()
        self.assertIn('--connect-timeout 10 --max-time 60', calls)
        self.assertIn('--retry 2', calls)
        self.assertIn('kubeadm=1.28.*', calls)
        self.assertIn('hold kubeadm kubelet kubectl', calls)

    def test_valid_installed_components_do_not_touch_repos_or_packages(self):
        (self.root / 'installed').touch()
        before = self.source.read_bytes()
        result = self.run_bash('install_kubernetes; install_kubernetes')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.source.read_bytes(), before)
        self.assertNotIn('apt-get', self.calls.read_text())
        self.assertNotIn('curl', self.calls.read_text())

    def test_repeat_repo_preparation_does_not_duplicate_sources(self):
        result = self.run_bash('k8s_dependency_repo; CANDIDATE=1.28.15; k8s_dependency_repo')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.source.read_text().count('deb ['), 1)
        self.assertEqual(self.calls.read_text().count('curl '), 1)

    def test_offline_missing_components_fail_without_network(self):
        result = self.run_bash('OFFLINE_MODE=true; if install_kubernetes; then exit 99; else exit 23; fi')
        self.assertEqual(result.returncode, 23)
        self.assertEqual(self.calls.read_text(), '')

    def test_offline_valid_components_skip_packages_and_crictl(self):
        (self.root / 'installed').touch()
        result = self.run_bash('OFFLINE_MODE=true; install_kubernetes')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.calls.read_text(), 'CNI\n')

    def test_explicit_minor_is_used_and_invalid_input_rejected(self):
        result = self.run_bash('PREFLIGHT_K8S_APT_MINOR=v1.29; k8s_dependency_repo')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('/v1.29/deb/', self.source.read_text())
        result = self.run_bash('PREFLIGHT_K8S_APT_MINOR=garbage; k8s_dependency_minor')
        self.assertNotEqual(result.returncode, 0)

    def test_preflight_uses_shared_install_and_reports_failure(self):
        prefix = f'source "{REPO}/scripts/lib/preflight_checks.sh"\n'
        result = self.run_bash('if preflight_fix_k8s_bins; then exit 99; else '
                               '[[ "$PREFLIGHT_K8S_FIX_FAILED" == true && "$PREFLIGHT_FAIL_COUNT" == 1 ]]; fi',
                               fail='hold', prefix=prefix)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn('version-verified', result.stdout)
        self.assertIn('hold kubeadm kubelet kubectl', self.calls.read_text())

    def test_ensure_k8s_stops_before_containerd_helm_and_init(self):
        prefix = f'source "{REPO}/scripts/lib/common.sh"\n'
        result = self.run_bash(r'''
OPENBKN_K8S_ENSURED=false
k8s_is_running() { return 1; }
check_root() { return 0; }
detect_package_manager() { return 0; }
install_containerd() { echo UNEXPECTED; }
install_helm() { echo UNEXPECTED; }
init_k8s_master() { echo UNEXPECTED; }
if ensure_k8s; then exit 99; else exit 23; fi
''', fail='install', prefix=prefix)
        self.assertEqual(result.returncode, 23, result.stdout + result.stderr)
        self.assertNotIn('UNEXPECTED', result.stdout)

    def test_existing_cluster_and_skip_bootstrap_do_not_prepare_packages(self):
        prefix = f'source "{REPO}/scripts/lib/common.sh"\n'
        result = self.run_bash(r'''
OPENBKN_K8S_ENSURED=false
k8s_is_running() { return 0; }
install_kubernetes() { echo UNEXPECTED; return 1; }
ensure_k8s
OPENBKN_SKIP_PLATFORM_BOOTSTRAP=true
ensure_platform_prerequisites
''', prefix=prefix)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn('UNEXPECTED', result.stdout)
        self.assertEqual(self.calls.read_text(), '')

    def test_warning_conclusion_keeps_exit_code_two(self):
        text = (REPO / 'preflight.sh').read_text()
        conclusion = text[text.index('exit_code=0\n'):]
        prefix = f'source "{REPO}/scripts/lib/preflight_checks.sh"\n'
        result = self.run_bash('PREFLIGHT_FAIL_COUNT=0; PREFLIGHT_WARN_COUNT=1; '
                               'PREFLIGHT_OUTPUT_JSON=false; _PF_BAR=bar; '
                               '_pf_section() { :; };\n' + conclusion, prefix=prefix)
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn('No blocking failures', result.stdout)
        self.assertNotIn('NOT all clear', result.stdout)

    def test_rpm_install_lock_and_failure_propagation(self):
        rpm_mocks = r'''
command() {
    if [[ "$1" == -v ]]; then
        case "$2" in
            apt-get|apt-cache|dpkg-query) return 1 ;;
            dnf|yum) [[ "$2" == "$TEST_PM" ]]; return ;;
        esac
    fi
    builtin command "$@"
}
rpm_mock() {
    echo "$TEST_PM $*" >> "$CALLS"
    case "$*" in
        *'list installed'*|*'list available'*) return 1 ;;
        makecache*) [[ "$FAIL" != update ]]; return ;;
        'versionlock --help') return 0 ;;
        'versionlock add'*) [[ "$FAIL" != hold ]]; return ;;
        install*)
            [[ "$FAIL" != install ]] || return 1
            touch "$K8S_SYSTEM_ROOT/installed"
            ;;
    esac
}
dnf() { rpm_mock "$@"; }
yum() { rpm_mock "$@"; }
'''
        for pm in ['dnf', 'yum']:
            for failure in ['', 'download', 'key', 'update', 'install', 'hold', 'kubectl']:
                with self.subTest(pm=pm, failure=failure):
                    (self.root / 'installed').unlink(missing_ok=True)
                    repo = self.root / 'etc/yum.repos.d/kubernetes.repo'
                    repo.parent.mkdir(parents=True, exist_ok=True)
                    repo.write_text('original RPM repository')
                    result = self.run_bash(rpm_mocks + '\nTEST_PM=' + pm + '\ninstall_kubernetes',
                                           fail=failure)
                    if failure:
                        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                        self.assertNotIn('components installed and verified', result.stdout)
                        if failure in ['download', 'key', 'update']:
                            self.assertEqual(repo.read_text(), 'original RPM repository')
                    else:
                        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                        self.assertIn(pm + ' versionlock add kubeadm kubelet kubectl', self.calls.read_text())

    def test_fix_failure_survives_preflight_recheck(self):
        prefix = f'source "{REPO}/scripts/lib/preflight_checks.sh"\n'
        result = self.run_bash(r'''
PREFLIGHT_K8S_FIX_FAILED=true
PREFLIGHT_REPORT_FILE=''
preflight_run_all_checks() { :; }
preflight_recheck_after_fixes
[[ "$PREFLIGHT_FAIL_COUNT" == 1 ]]
''', prefix=prefix)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_k3s_bootstrap_does_not_install_kubeadm_dependencies(self):
        prefix = f'source "{REPO}/scripts/lib/common.sh"\n'
        result = self.run_bash(r'''
KUBE_DISTRO=k3s
ensure_k3s() { echo K3S; }
ensure_data_services() { return 0; }
install_kubernetes() { echo UNEXPECTED; return 1; }
ensure_platform_prerequisites
''', prefix=prefix)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('K3S', result.stdout)
        self.assertNotIn('UNEXPECTED', result.stdout)

    def test_real_cni_helper_offline_and_download_failure(self):
        body = f'source "{REPO}/scripts/lib/k8s_dependencies.sh"\n' + r'''
detect_package_manager() { PKG_MANAGER=apt; PKG_MANAGER_INSTALL='apt-get install -y'; }
'''
        result = self.run_bash(body + 'OFFLINE_MODE=true; _k8s_ensure_cni_bin_plugins')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.calls.read_text(), '')
        result = self.run_bash(body + '_k8s_ensure_cni_bin_plugins', fail='download')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Download failed', result.stdout)
        self.assertFalse((self.root / 'opt/cni/bin/loopback').exists())

    def test_real_cni_tarball_fallback_and_reuse(self):
        archive = self.root / 'cni.tar.gz'
        with tarfile.open(archive, 'w:gz') as tar:
            data = b'#!/bin/sh\nexit 0\n'
            info = tarfile.TarInfo('loopback')
            info.size = len(data)
            info.mode = 0o755
            tar.addfile(info, io.BytesIO(data))
        body = f'source "{REPO}/scripts/lib/k8s_dependencies.sh"\n' + r'''
detect_package_manager() { PKG_MANAGER=apt; PKG_MANAGER_INSTALL='apt-get install -y'; }
curl() {
    echo 'curl CNI' >> "$CALLS"
    while [[ $# -gt 0 ]]; do
        if [[ "$1" == --output ]]; then cp "$K8S_SYSTEM_ROOT/cni.tar.gz" "$2"; return; fi
        shift
    done
}
_k8s_ensure_cni_bin_plugins
_k8s_ensure_cni_bin_plugins
'''
        result = self.run_bash(body)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(os.access(self.root / 'opt/cni/bin/loopback', os.X_OK))
        self.assertEqual(self.calls.read_text().count('curl CNI'), 1)


if __name__ == '__main__':
    unittest.main()
