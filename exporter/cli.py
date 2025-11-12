import click
from pathlib import Path
from typing import Optional
import logging
from .exporter import DataExporter


@click.command()
@click.option('--api-domain', required=True, help='API 域名（如：https://cn3-open-api.guance.com）')
@click.option('--output', required=True, type=click.Path(), help='输出 CSV 文件路径')
@click.option('--api-key', help='API 密钥（可选，也可以从环境变量 API_KEY 获取）')
@click.option('--dql', type=str, required=True, help='DQL 查询语句')
@click.option('--start-time', type=str, required=True, help='开始时间（ISO 格式，如：2024-01-01T00:00:00+08:00）')
@click.option('--end-time', type=str, required=True, help='结束时间（ISO 格式，如：2024-01-01T23:59:59+08:00）')
@click.option('--max-rows', type=int, default=10000, help='最大导出行数（可选，达到此数量或数据查询完毕时停止，默认导出 10000 行）')
def export(api_domain: str, output: str, api_key: Optional[str], dql: str, start_time: str, end_time: str, max_rows: Optional[int]):
    """从远程 API 获取数据并导出到 CSV 文件"""
    # 设置日志
    logging.basicConfig(
        filename='export.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    output_path = Path(output)
    
    # 初始化导出器
    exporter = DataExporter(api_domain=api_domain, api_key=api_key, dql=dql)
    
    max_rows_info = f" - 最大行数: {max_rows}" if max_rows else ""
    logging.info(f"开始导出任务 - API 域名: {api_domain} - 输出文件: {output_path} - 开始时间: {start_time} - 结束时间: {end_time}{max_rows_info}")
    
    try:
        # 获取数据
        data = exporter.fetch_data(
            start_time=start_time,
            end_time=end_time,
            max_rows=max_rows,
            position=0,
            desc="Fetching data"
        )
        
        # 导出到 CSV
        exporter.export_to_csv(
            data=data,
            output_path=output_path,
            position=1,
            desc="Exporting to CSV"
        )
        
        count = len(data)
        success = True
        
    except Exception as e:
        error_message = str(e)
        logging.error(f"处理数据时发生错误: {error_message}")
        print(f"处理数据时发生错误: {error_message}")
        success = False
        count = 0
    
    if success:
        logging.info(f"导出任务完成 - 成功导出 {count} 条数据")
        click.echo(f"导出完成！")
        click.echo(f"成功导出 {count} 条数据到 {output_path}")
    else:
        logging.error("导出任务失败")
        click.echo("导出失败，请查看日志文件 export.log 获取详细信息")


if __name__ == '__main__':
    export()
