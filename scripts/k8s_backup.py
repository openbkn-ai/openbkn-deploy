"""Read-only Kubernetes configuration inventory; intentionally no apply/restore."""
import copy
import datetime
import json
import re
import io
import hashlib
from pathlib import PurePosixPath
import tarfile

MODE = 'kubernetes-manifests-v1'
RESOURCES = ('deployments,statefulsets,daemonsets,services,ingresses,configmaps,'
             'cronjobs,jobs,horizontalpodautoscalers,networkpolicies,poddisruptionbudgets,persistentvolumeclaims,'
             'serviceaccounts,roles,rolebindings')


def add_arguments(parser):
    parser.add_argument('--k8s-namespaces', nargs='+', default=['openbkn', 'resource'],
                        help='K8s 配置留档范围，默认 openbkn resource')
    parser.add_argument('--k8s-secrets', nargs='+', default=[], metavar='NAMESPACE/NAME',
                        help='只导出显式列出的 Secret；不导出 ServiceAccount token 或 Helm release Secret')


def clean(value):
    if isinstance(value, list):
        return [clean(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = copy.deepcopy(value)
    result.pop('status', None)
    metadata = result.get('metadata')
    if isinstance(metadata, dict):
        for key in ('managedFields', 'resourceVersion', 'uid', 'creationTimestamp', 'generation',
                    'deletionTimestamp', 'deletionGracePeriodSeconds'):
            metadata.pop(key, None)
        # Last-applied JSON can embed Secret values or stale runtime fields.
        metadata.get('annotations', {}).pop('kubectl.kubernetes.io/last-applied-configuration', None)
    return result


def validate_metadata(metadata, error):
    if (not isinstance(metadata, dict) or metadata.get('mode') not in (MODE, 'kubernetes-reference-v1')
            or metadata.get('restore_supported') is not False
            or not isinstance(metadata.get('namespaces'), list)
            or not metadata['namespaces'] or any(not isinstance(n, str) for n in metadata['namespaces'])
            or not isinstance(metadata.get('secrets'), list)):
        raise error('K8s 配置留档清单无效')


def validate_document(document, error):
    if (not isinstance(document, dict) or document.get('mode') not in (MODE, 'kubernetes-reference-v1')
            or document.get('restore_supported') is not False
            or not isinstance(document.get('namespaces'), list)
            or not isinstance(document.get('cluster_resources'), list)):
        raise error('K8s 配置留档格式无效')


class K8sBackup:
    def __init__(self, args, run, error, log):
        self.args, self.run, self.error, self.log = args, run, error, log
        self.namespaces = list(dict.fromkeys(args.k8s_namespaces))
        self.secrets = list(dict.fromkeys(args.k8s_secrets))
        name = r'[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?'
        if not self.namespaces or any(not re.fullmatch(name, ns) for ns in self.namespaces):
            raise error('K8s namespace 名称无效')
        for secret in self.secrets:
            parts = secret.split('/')
            if len(parts) != 2 or parts[0] not in self.namespaces or not re.fullmatch(name, parts[1]):
                raise error('--k8s-secrets 需要已选命名空间内的 namespace/name')

    def read(self, command):
        return json.loads(self.run(command, timeout=self.args.timeout))

    def collect(self):
        document = {'mode': MODE, 'restore_supported': False,
                    'created_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    'note': '可用于人工恢复的资源清单；不自动恢复，不含 etcd/PVC 数据。values/ConfigMap 可能含密码。',
                    'version': self.read(['kubectl', 'version', '-o', 'json']),
                    'cluster_resources': [], 'namespaces': [], 'secrets': []}
        # Deliberately omit nodes, kube-system and cluster certificates/credentials.
        for resource in ('persistentvolumes', 'storageclasses'):
            rows = self.read(['kubectl', 'get', resource, '-o', 'json'])['items']
            if resource == 'persistentvolumes':
                rows = [row for row in rows if row.get('spec', {}).get('claimRef', {}).get('namespace') in self.namespaces]
            document['cluster_resources'].extend(clean(rows))
        for namespace in self.namespaces:
            ns = self.read(['kubectl', 'get', 'namespace', namespace, '-o', 'json'])
            resources = self.read(['kubectl', '-n', namespace, 'get', RESOURCES, '-o', 'json'])['items']
            releases = self.read(['helm', 'list', '-n', namespace, '-o', 'json'])
            helm = []
            for release in releases:
                helm.append({'name': release['name'], 'chart': release.get('chart'),
                             'app_version': release.get('app_version'), 'revision': release.get('revision'),
                             'values': self.read(['helm', 'get', 'values', release['name'], '-n', namespace, '-o', 'json'])})
            document['namespaces'].append({'namespace': clean(ns), 'resources': clean(resources), 'helm': helm})
        for entry in self.secrets:
            ns, name = entry.split('/')
            value = self.read(['kubectl', '-n', ns, 'get', 'secret', name, '-o', 'json'])
            if value.get('type') in ('kubernetes.io/service-account-token', 'helm.sh/release.v1'):
                raise self.error('不允许导出 ServiceAccount token 或 Helm 内部 Secret: ' + entry)
            document['secrets'].append(clean(value))
        validate_document(document, self.error)
        return document

    def check(self):
        # Read all selected resources so permission/missing-namespace errors fail
        # before any component starts exporting data. Nothing is written to K8s.
        self.collect()

    def backup(self, path):
        self.log('导出 K8s 人工恢复配置包（不自动恢复）: ' + ', '.join(self.namespaces))
        document = self.collect()
        write_package(path, document, self.error)
        return {'mode': MODE, 'restore_supported': False, 'namespaces': self.namespaces,
                'secrets': self.secrets}


def manifest(resource):
    """Remove old-cluster identity and allocated fields, retaining user intent."""
    value = clean(resource)
    metadata = value.setdefault('metadata', {})
    for key in ('ownerReferences', 'finalizers', 'selfLink'):
        metadata.pop(key, None)
    annotations = metadata.get('annotations', {})
    for key in list(annotations):
        if key.startswith(('pv.kubernetes.io/', 'volume.kubernetes.io/')):
            annotations.pop(key)
    spec = value.get('spec', {})
    kind = value.get('kind')
    if kind == 'Service':
        headless = spec.get('clusterIP') == 'None'
        for key in ('clusterIPs', 'ipFamilies', 'ipFamilyPolicy', 'healthCheckNodePort'):
            spec.pop(key, None)
        if not headless:
            spec.pop('clusterIP', None)
        for port in spec.get('ports', []):
            port.pop('nodePort', None)
    if kind == 'PersistentVolumeClaim':
        spec.pop('volumeName', None)
    if kind == 'Namespace':
        spec.pop('finalizers', None)
    if kind == 'ServiceAccount':
        # Token references from the old cluster cannot be reused. Explicit
        # imagePullSecrets are retained and need separate selected Secret backups.
        value.pop('secrets', None)
    return value


def resource_path(resource):
    namespace = resource.get('metadata', {}).get('namespace')
    prefix = 'manifests/namespaced/' + namespace if namespace else 'manifests/cluster'
    return prefix + '/' + resource['kind'].lower() + '/' + resource['metadata']['name'] + '.json'


def write_package(path, document, error):
    files = {}
    def put(name, value):
        if name in files:
            raise error('K8s 导出存在重复文件: ' + name)
        files[name] = (json.dumps(value, ensure_ascii=False, indent=2) + '\n').encode()
    put('inventory.json', document)
    for resource in document['cluster_resources']:
        if resource.get('kind') == 'PersistentVolume':
            # Disk paths/CSI handles identify the old storage, not a portable PV.
            put('reference/persistentvolumes/' + resource['metadata']['name'] + '.json', resource)
        else:
            put(resource_path(resource), manifest(resource))
    for item in document['namespaces']:
        ns = item['namespace']['metadata']['name']
        put(resource_path(item['namespace']), manifest(item['namespace']))
        for resource in item['resources']:
            resource = copy.deepcopy(resource)
            resource.setdefault('metadata', {}).setdefault('namespace', ns)
            if resource.get('kind') == 'Job':
                put('reference/jobs/' + ns + '/' + resource['metadata']['name'] + '.json', resource)
            else:
                put(resource_path(resource), manifest(resource))
        for release in item['helm']:
            prefix = 'helm/' + ns + '/' + release['name']
            put(prefix + '/release.json', {k: v for k, v in release.items() if k != 'values'})
            put(prefix + '/values.json', release['values'] or {})
    for resource in document.get('secrets', []):
        put(resource_path(resource), manifest(resource))
    files['RESTORE.zh.md'] = RESTORE_GUIDE.encode()
    hashes = {name: hashlib.sha256(content).hexdigest() for name, content in files.items()}
    put('manifest.json', {'mode': MODE, 'automatic_restore': False,
                          'created_at': document.get('created_at'), 'sha256': hashes})
    with tarfile.open(path, 'w:') as archive:
        for name, content in sorted(files.items()):
            safe_path(name, error)
            member = tarfile.TarInfo(name)
            member.mode = 0o600
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))


def safe_path(name, error):
    path = PurePosixPath(name)
    if not name or path.is_absolute() or '..' in path.parts or str(path) != name:
        raise error('K8s 备份包存在不安全路径: ' + name)


def validate_archive(path, error):
    with tarfile.open(path, 'r:') as archive:
        members = archive.getmembers()
        names = set()
        for member in members:
            safe_path(member.name, error)
            if not member.isfile() or member.name in names:
                raise error('K8s 备份包存在非普通文件或重复文件')
            names.add(member.name)
        if not {'manifest.json', 'inventory.json', 'RESTORE.zh.md'} <= names:
            raise error('K8s 备份包缺少恢复清单或说明')
        metadata = json.load(archive.extractfile('manifest.json'))
        hashes = metadata.get('sha256')
        if (metadata.get('mode') != MODE or metadata.get('automatic_restore') is not False
                or not isinstance(hashes, dict) or set(hashes) != names - {'manifest.json'}):
            raise error('K8s 备份包文件清单无效')
        for member in members:
            if member.name == 'manifest.json':
                continue
            digest = hashlib.sha256()
            with archive.extractfile(member) as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(chunk)
            if digest.hexdigest() != hashes[member.name]:
                raise error('K8s 文件校验失败: ' + member.name)
        validate_document(json.load(archive.extractfile('inventory.json')), error)


RESTORE_GUIDE = """# Kubernetes 人工恢复配置备份

本包提供可供 kubectl apply 使用的标准 Kubernetes JSON 清单（JSON 同样可用于 Helm values）。
当前工具不执行自动恢复。不是完整集群灾备，不含 etcd、PVC 数据、CRD、自定义资源或集群 RBAC。

## 推荐方式：重新安装

1. 重建兼容版本 Kubernetes，安装存储/网络等基础组件。
2. 根据 inventory.json 和 helm/<namespace>/<release>/release.json 确定应用及 Chart 版本。
3. 准备该版本 Chart（本包不包含 Chart 文件和镜像，需从部署仓库/制品库获取）。
4. 审核 values.json 中域名、地址、存储类、密码及环境相关参数；按需恢复已选择的应用 Secret。
5. 使用 helm upgrade --install <release> <对应版本Chart> -n <namespace> --create-namespace -f helm/<namespace>/<release>/values.json 安装。
6. 按业务数据备份流程恢复数据，最后启动业务。

## 选择性使用资源清单

- manifests/cluster/namespace 可通过 kubectl apply -f <文件> 创建命名空间。
- 然后按需恢复 StorageClass、Secret、ConfigMap、ServiceAccount、Role/RoleBinding、PVC、Service、工作负载、Ingress 等。
- 例如 kubectl apply -f manifests/namespaced/<namespace>/configmap/<name>.json。
- Helm 管理的资源优先用 Helm 重装；不要同时用清单和 Helm 对同一资源重复恢复。
- 恢复数据前保持业务生产者、消费者和定时任务停止；不要对 manifests/ 整目录直接递归 apply。
- 已清除旧 UID、状态、ownerReferences、finalizers、普通 Service ClusterIP/NodePort、PVC volumeName 等分配信息。
- Headless Service 的 clusterIP: None 保留。新分配的 Service 地址与旧集群可能不同，须修改相关连接配置。
- PVC 仅重建申请，不恢复卷内数据；PV 的旧磁盘路径/CSI 标识放在 reference/，需按新存储人工配置。
- 已运行的 Job 放在 reference/jobs/，避免恢复时重复执行迁移或任务。
- 节点选择器、卷路径、外部地址、LB 地址、存储类及 HPA/CronJob 等仍需审核适配目标环境。

## 完整性与 Secret

manifest.json 记录每个文件的 SHA-256；inventory.json 保存原始配置参考和 Helm 版本。
Secret 只有显式列入 --k8s-secrets 的才包含；恢复引用未备份的 Secret 前必须另行准备。
ServiceAccount token 由新集群生成，不备份旧 token；Helm 内部 Secret 不导出。
即使没有显式 Secret，values、ConfigMap、环境变量仍可能含密码；文件须限制权限。
"""
