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

或者使用 pip 安装：

```bash
pip install -e .
```

## 使用方法

### 基本用法

```bash
obs-data-exporter \
  --api-url "https://api.example.com/data" \
  --output "/path/to/output.csv"
```

### 使用 API 密钥

可以通过环境变量设置：

```bash
export API_KEY="your-api-key"
obs-data-exporter \
  --api-url "https://api.example.com/data" \
  --output "/path/to/output.csv"
```

或者通过命令行参数：

```bash
obs-data-exporter \
  --api-url "https://api.example.com/data" \
  --output "/path/to/output.csv" \
  --api-key "your-api-key"
```

## 注意事项

- 请确保 API URL 正确且可访问
- 请确保有写入输出文件的权限
- 日志文件会保存在当前工作目录的 `export.log` 文件中

## 开发

当前版本包含基本的框架结构，具体的 API 调用和 CSV 导出逻辑需要根据实际需求进行实现。
