# chat 历史上下文与检索说明

本仓库基于 `Him666233/astrbot_plugin_group_chat_plus` 修改，插件内部 ID 仍为 `astrbot_plugin_group_chat_plus`，以兼容已有配置和 JSON 历史数据。显示名称改为 **chat**。

## 读取顺序

普通群聊上下文：

```text
chat_plus 自定义 JSON 最近记录（最多 700 条）
    ↓
读取最多 700 条候选
    ↓
先保留最近 200 条
    ↓
对更早的候选按当前话题和发送者评分
    ↓
额外加入最多 20 条相关消息
    ↓
按消息 ID 去重并按时间排序
    ↓
交给模型
```

自定义 JSON 为空或读取失败时，才回退到 AstrBot SQLite 最近历史。

## SQLite 历史检索

用户明确要求查找以前聊天、搜索原话或回忆旧话题时，模型会调用 `search_chat_history` 工具：

```text
当前群号和最近 30 天范围
    ↓
SQLite FTS5 trigram 关键词候选（最多 200 条）
    ↓
BM25 + 时间 + 发送者 + 群号加权
    ↓
15～20 条结果
    ↓
每条补充前后各 2 条
    ↓
按消息 ID 去重
    ↓
软上限 10000 字，硬上限 15000 字
    ↓
交给模型总结
```

## 搜索缓存

相同“平台 + 群范围 + 关键词 + 天数 + 搜索参数”的结果缓存 **10 分钟**。最多保存 64 个查询结果。

## 权限

- 普通用户只能搜索当前群。
- 只有 AstrBot 管理员可以将 `all_groups=true`，搜索全部群。
- 默认时间范围为最近 30 天，最大 365 天。

## 数据与索引

- 源数据：`data_v4.db` 中的 `platform_message_history`。
- FTS 表：`chat_history_fts`。
- 索引状态表：`chat_history_fts_state`。
- 使用 `trigram` 中文索引；两字以下或 FTS 无结果时，使用受群号和时间范围约束的 `LIKE` 兜底。
- 递归解析转发消息节点，提取转发者与被转发正文；索引版本变化时自动重建。
- 新消息由后台索引任务增量更新，默认每 10 分钟刷新一次；每次关键词搜索前仍会即时检查增量。

## 推荐配置

```text
max_context_messages = 200
context_candidate_messages = 700
context_relevant_extra = 20
custom_storage_max_messages = 700
history_search_candidates = 200
history_search_results = 18
history_search_context_radius = 2
history_search_soft_chars = 10000
history_search_hard_chars = 15000
history_search_cache_seconds = 600
```

SQLite 核心历史上限由 AstrBot 主配置控制：

```text
provider_ltm_settings.group_message_history_max_cnt = 10000
```

## 升级兼容

插件 ID、目录名、配置文件名和运行数据路径均保持不变。已有群号配置、Web 面板设置和 `chat_history/aiocqhttp/group/*.json` 可以直接沿用。
