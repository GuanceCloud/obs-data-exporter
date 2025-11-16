# OBS Data Exporter

OBS Data Exporter 是一个用于从远程 API 获取数据并导出到 CSV 文件的工具。

## 特性

- 调用远程 API 获取数据
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
  --api-domain "https://api.example.com/data" \
  --api-key "your-api-key" \
  --dql 'L("default")::re(`.*`):(host, service, source) { }' \
  --start-time "2025/11/12T23:46:30+08:00" \
  --end-time "2025/11/13T00:46:30+08:00" \
  --output "/path/to/output.csv"
```

## 注意事项

- 请确保 API URL 正确且可访问
- 请确保有写入输出文件的权限
- 日志文件会保存在当前工作目录的 `export.log` 文件中

## 开发

当前版本包含基本的框架结构，具体的 API 调用和 CSV 导出逻辑需要根据实际需求进行实现。
