# OBS Data Exporter

OBS Data Exporter 是一个用于从观测云远程 OpenAPI 获取数据并导出到 CSV 文件的 CLI 工具。

## 特性

- 调用远程 OpenAPI 获取数据
- 将数据导出到 CSV 文件
- 提供详细的进度显示
- 支持日志记录

## 安装

1. 克隆仓库：

```bash
git clone <repository-url>
cd obs-data-exporter
```

2. 安装依赖：

```bash
pip install -r requirements.txt
```

## 使用方法

### 基本用法

```bash
python -m exporter.cli \
  --api-domain "https://api.example.com" \
  --api-key "your-api-key" \
  --dql 'L("default")::re(`.*`):(host, service, source) { }' \
  --start-time "2025/11/12T23:46:30+08:00" \
  --end-time "2025/11/13T00:46:30+08:00" \
  --max-rows 10000 \
  --time-slice 1 \
  --output "/path/to/output.csv"
```

### 参数说明

| 参数 | 是否必需 | 说明 | 示例 |
|------|---------|------|------|
| `--api-domain` | 是 | 数据查询的站点 OpenAPI 域名地址 | `https://openapi.example.com` |
| `--api-key` | 是 | 在 对应站点的 `工作空间` -> `管理` -> `API Keys 管理` 中创建 `API Key` | `your-api-key`  |
| `--output` | 是 | 输出 CSV 文件路径 | `/path/to/output.csv` |
| `--dql` | 是 | DQL 查询语句 | `L("default")::re(\`.*\`):(host, service, source) { }` |
| `--start-time` | 是 | 开始时间（ISO 格式） | `2024-01-01T00:00:00+08:00` |
| `--end-time` | 是 | 结束时间（ISO 格式） | `2024-01-01T23:59:59+08:00` |
| `--time-slice` | 否 | 时间切片大小（分钟），用于将时间范围分割成多个段进行导出。默认值为 1 分钟 | `5` |
| `--max-rows` | 否 | 最大导出行数。达到此数量或数据查询完毕时停止。默认值为 10000 行 | `100000` |

## 注意事项

- 请确保 OpenAPI domain 正确且可访问
- 请确保有写入输出文件的权限
- CLI 的执行日志会保存在当前工作目录的 `export.log` 文件中
