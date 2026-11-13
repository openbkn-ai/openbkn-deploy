# 历史终态载荷元数据：离线排查与有范围恢复

关联 [openbkn-deploy #82](https://github.com/openbkn-ai/openbkn-deploy/issues/82)。

## 修复与影响边界

`output_payload` / `error_payload` 外层的 `_trace_evidence_completion` 属于存储控制字段，不属于 `PayloadEnvelope` 业务 DTO。转换器将其移到 CallFact 顶层 `evidence_completion`，保留原始 durability、expectation、拒绝原因/hash、业务载荷、ID、归属和引用。`inline` 内的同名业务字段不处理。没有该外层字段的旧数据保持兼容；两处同时出现元数据（即使相同）、非法 durability / expectation / 拒绝 hash 等情况必须失败，保留源记录，不默认补值。

此缺陷可能使历史交互在 native preflight 时被拒绝，并使相关依赖未被导入。它不证明源数据被物理删除，也不意味着所有跳过记录都由此原因导致。本文和报告工具不读取现网、不恢复数据、不调用模型。

## 已冻结快照的离线盘点

在已获准读取的私有快照副本上执行，`--source` 必须是现有 `snapshot.json` / `records.jsonl` 目录。工具先校验快照计数与 SHA-256，报告文件必须不存在：

```bash
python3 scripts/upgrades/0.2.0/historical_trace_data/report_terminal_payloads.py \
  --source /private/path/frozen-snapshot \
  --output /private/path/terminal-completion-report.json
```

只在终端打印 `call_facts`（检查行数）、`wrapped`（包含外层字段的行数，不是字段数）、`invalid`（JSON/控制元数据异常行数）。私有报告记录源 SHA、转换器源码 SHA、源序号、operation/attempt/interaction 定位及安全原因；不复制业务载荷或 exception 原文。报告不是导入计划，不证明目标当前缺失、原生预检成功或源内容丢失。覆盖范围仅为该快照中的 CallFact，不是全环境受影响数量。

本次开发验收仅用合成数据：2 条分别携带 output/error 元数据的记录、1 条旧记录、1 条非法 JSON，盘点为 `call_facts=4, wrapped=2, invalid=1`。未读取业务快照，因此不提供未经核对的现网受影响数量。

## 受控恢复计划（执行前需要环境 Owner 单独批准）

1. 冻结转换器的提交与文件 SHA、源快照 SHA、目标版本、原生校验器的源码提交/二进制 SHA 或镜像 digest，保存原迁移的拒绝/依赖排除 manifest。不要只记录可变镜像标签。CI 离线合同使用 Core `9a0a0a26c1efd2119b8f9a54b10e06d45863de97`；实际恢复须与部署目标合同核对，不能把此测试 pin 当作现网版本证明。
2. 以盘点命中的 interaction 为候选，与原 manifest 的 `native_core_preflight_rejected` / `call_facts` 及依赖排除记录逐项对账；其他原因分开处理。只读核对目标实际存在的记录、调用主体/归属和读取权限，区分“未导入”“已存在但不可见”。排查记录保存在私有目录，不公开业务 ID、hash 或正文。
3. 从权威原始快照生成包含完整父级、CallFacts、对应 receipts、ledger/artifacts/溯源依赖的最小闭包，保持原身份、顺序、引用和原始内容/hash。只选择原 manifest 能证实被跳过的依赖；不从问答消息、预览、旧解释摘要或新模型回答补写权威源文。禁止为通过校验而重算被修改内容的 hash。
4. 用目标对应的原生离线 `historical-data-validate --validate-core-records` 及其 ledger/artifact/provenance 校验路径预检整个闭包，保存输入 SHA、返回码和 verdict。转换器单测或盘点无异常都不能代替 native preflight。实际部署包装器 `run_upgrade.py` 会执行 Pod exec，不是纯离线 dry-run；不要拿它直接对现场试跑。
5. 提交具体候选清单、范围计数、完整预检和回读/冲突处理方案给环境 Owner 审批。审批后仅用官方 native 导入路径执行；已存在且完全相同的记录按幂等复用，任何内容/归属/引用冲突立即停止，不覆盖、不删除源、不改索引映射或别名。不单独 PUT 两个 artifact 绕过拥有它们的交互校验。
6. 私下核对原生读取的业务内容/hash、ownership/scope、引用完整性及幂等再次预检结果，确认平台可经正常权限读取后再验收页面源文入口。保留源快照、拒绝及执行凭据；失败时停止后续恢复并保留现场，不采用删除现有数据的“回滚”。发布/恢复验收完成前，不能把源文入口不可见改为推测的成功。

## 原生校验回归

`BKN_TEST_CORE_VALIDATOR` 指向固定版本、可信构建的离线二进制。下面只对合成 fixture 执行 `--validate-core-records`，不建立任何数据库连接：

```bash
BKN_TEST_CORE_VALIDATOR=/private/path/historical-data-validate \
  python3 -m unittest discover \
  -s scripts/upgrades/0.2.0/historical_trace_data -p 'test_*.py'
```

CI 必须启用该变量，不能通过跳过原生用例替代验证。合法 output/error 计划通过，原存储形态及未识别字段仍由严格输入校验拒绝。业务 inline、原记录和 ledger 不因转换而改变。
