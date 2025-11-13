import os
from pathlib import Path
from re import A
from typing import Optional, Dict, List
import requests
import csv
from tqdm import tqdm
import logging
import time
import dateutil.parser
from datetime import datetime, timedelta


class DataExporter:
    """数据导出器主类，用于调用远程 API 并导出数据到 CSV"""
    
    # API 路径固定部分
    API_PATH = "/api/v1/df/asynchronous/query_data"
    
    def __init__(self, api_domain: str, api_key: Optional[str] = None, dql: Optional[str] = None):
        """
        初始化数据导出器
        
        Args:
            api_domain: API 域名（如：https://cn3-openapi.guance.com）
            api_key: API 密钥（可选，也可以从环境变量获取）
            dql: DQL 查询语句
        """
        # 移除域名末尾的斜杠（如果有）
        api_domain = api_domain.rstrip('/')
        
        # 构建完整的 API URL
        self.api_url = f"{api_domain}{self.API_PATH}"
        
        self.api_key = api_key or os.getenv('API_KEY')
        self.dql = dql
        
        self.headers = {
            "Content-Type": "application/json;charset=UTF-8"
        }
        if self.api_key:
            self.headers["DF-API-KEY"] = self.api_key
        
        self.progress_bars = {}
        self.active_positions = set()
        self.current_tasks = {}
        
        # 设置日志
        self._setup_logger()
    
    def _setup_logger(self):
        """设置日志记录器"""
        self.logger = logging.getLogger('exporter')
        self.logger.setLevel(logging.INFO)
        
        if not self.logger.handlers:
            log_file = Path.cwd() / 'export.log'
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            
            self.logger.addHandler(file_handler)
    
    def _create_progress_bar(self, position: int, desc: str) -> tqdm:
        """创建进度条"""
        self.active_positions.add(position)
        
        if position not in self.progress_bars:
            self.progress_bars[position] = tqdm(
                desc=desc,
                unit=" items",
                position=position,
                leave=True,
                dynamic_ncols=True,
                bar_format='{desc}'
            )
        
        pbar = self.progress_bars[position]
        pbar.reset()
        pbar.set_description(desc)
        self.current_tasks[position] = desc
        
        return pbar
    
    def _iso_to_timestamp(self, iso_time: str) -> int:
        """
        将 ISO 格式时间转换为时间戳（毫秒）
        
        Args:
            iso_time: ISO 格式时间字符串
            
        Returns:
            时间戳（毫秒）
        """
        dt = dateutil.parser.parse(iso_time)
        return int(dt.timestamp() * 1000)
    
    def _query_api(self, query_params: Dict, async_id: Optional[str] = None) -> Dict:
        """
        发送 API 请求
        
        Args:
            query_params: 查询参数
            async_id: 异步查询 ID（如果存在）
            
        Returns:
            API 响应数据
        """
        queries = [{
            "qtype": "dql",
            "query": query_params
        }]
        
        if async_id:
            queries[0]["async_id"] = async_id
        
        payload = {"queries": queries}
        
        self.logger.info(f"payload: {payload}")

        response = requests.post(
            self.api_url,
            headers=self.headers,
            json=payload
        )
        
        if response.status_code != 200:
            raise ExportError(f"API 请求失败，状态码: {response.status_code}, 响应: {response.text}")
        
        result = response.json()
        if not result.get("success", False):
            raise ExportError(f"API 返回错误: {result.get('message', '未知错误')}")
        
        return result
    
    def _wait_for_async_query(self, async_id: str, position: int, desc: str) -> Dict:
        """
        等待异步查询完成
        
        Args:
            async_id: 异步查询 ID
            position: 进度条位置
            desc: 进度条描述
            
        Returns:
            查询结果
        """
        max_retries = 100  # 最大重试次数
        retry_count = 0
        
        while retry_count < max_retries:
            # 使用空的查询参数，只传递 async_id
            query_params = {
                "q": self.dql,
                "timeRange": [0, 0],  # 占位，实际不使用
                "orderby": [{"time": "desc"}],
                "disableMultipleField": False,
                "disable_sampling": True,
                "disable_streaming_aggregation": True,
                "align_time": False,
                "offset": 0,
                "slimit": 1000,
                "limit": 1000,
                "search_after": [],
                "scan_completed": False,
                "scan_index": "",
                "indexes": ["default"],
                "tz": "Asia/Shanghai"
            }

            self.logger.info(f"等待异步查询完成 - async_id: {async_id} - query_params: {query_params}")
            
            result = self._query_api(query_params, async_id=async_id)
            
            if not result.get("content", {}).get("data"):
                raise ExportError("API 返回数据为空")
            
            query_data = result["content"]["data"][0]
            query_status = query_data.get("query_status", "")
            
            if query_status == "finished":
                return query_data
            elif query_status in ["running", "pending"]:
                retry_count += 1
                if position in self.progress_bars:
                    status = f"{desc} [等待异步查询完成，重试 {retry_count}/{max_retries}]"
                    self.progress_bars[position].set_description(status)
                    self.progress_bars[position].refresh()
                time.sleep(1)  # 等待 1 秒后重试
            else:
                raise ExportError(f"查询状态异常: {query_status}")
        
        raise ExportError(f"异步查询超时，已重试 {max_retries} 次")

    def _generate_time_slices(self, start_time: str, end_time: str, slice_minutes: int) -> List[tuple[str, str]]:
        """
        生成时间切片列表

        Args:
            start_time: 开始时间（ISO 格式）
            end_time: 结束时间（ISO 格式）
            slice_minutes: 每个切片的时间长度（分钟）

        Returns:
            时间切片列表，每个元素为 (slice_start, slice_end) 的元组
        """
        start_dt = dateutil.parser.parse(start_time)
        end_dt = dateutil.parser.parse(end_time)

        time_slices = []
        current_time = start_dt

        while current_time < end_dt:
            slice_end = min(current_time + timedelta(minutes=slice_minutes), end_dt)
            time_slices.append((
                current_time.isoformat(),
                slice_end.isoformat()
            ))
            current_time = slice_end

        return time_slices

    def fetch_data_with_time_slices(self, start_time: str, end_time: str, time_slice_minutes: int = 5,
                                  max_rows: Optional[int] = None, position: int = 0, desc: str = "Fetching data") -> List[Dict]:
        """
        按时间切片获取数据

        Args:
            start_time: 开始时间（ISO 格式）
            end_time: 结束时间（ISO 格式）
            time_slice_minutes: 时间切片大小（分钟）
            max_rows: 最大导出行数（可选，达到此数量时停止）
            position: 进度条位置
            desc: 进度条描述

        Returns:
            返回的数据列表
        """
        pbar = self._create_progress_bar(position, desc)

        try:
            self.logger.info(f"开始按时间切片获取数据 - {desc} - 开始时间: {start_time} - 结束时间: {end_time} - 切片大小: {time_slice_minutes}分钟")

            # 生成时间切片
            time_slices = self._generate_time_slices(start_time, end_time, time_slice_minutes)
            self.logger.info(f"生成了 {len(time_slices)} 个时间切片")

            all_data = []
            total_slices = len(time_slices)
            processed_slices = 0

            for i, (slice_start, slice_end) in enumerate(time_slices):
                slice_desc = f"{desc} [切片 {i+1}/{total_slices}: {slice_start} 至 {slice_end}]"
                self.logger.info(f"开始处理时间切片 {i+1}/{total_slices}: {slice_start} 至 {slice_end}")

                # 为每个切片创建独立的进度条位置
                slice_position = position + i + 1

                # 计算当前切片允许的最大行数
                slice_max_rows = None
                if max_rows:
                    remaining_rows = max_rows - len(all_data)
                    if remaining_rows <= 0:
                        self.logger.info(f"已达到最大行数限制: {max_rows}，跳过剩余切片")
                        break
                    slice_max_rows = remaining_rows
                    self.logger.info(f"当前切片最多允许获取 {slice_max_rows} 条数据（剩余需要 {remaining_rows} 条）")

                # 获取当前切片的数据
                slice_data = self.fetch_data(
                    start_time=slice_start,
                    end_time=slice_end,
                    max_rows=slice_max_rows,  # 设置单个切片的最大行数限制
                    position=slice_position,
                    desc=slice_desc
                )

                # 添加到总数据中
                all_data.extend(slice_data)
                processed_slices += 1

                # 更新主进度条
                if position in self.progress_bars and self.current_tasks.get(position) == desc:
                    max_info = f"/{max_rows}" if max_rows else ""
                    status = f"{desc} [已处理 {processed_slices}/{total_slices} 个切片，已获取 {len(all_data)}{max_info} 条数据]"
                    self.progress_bars[position].set_description(status)
                    self.progress_bars[position].refresh()

                # 检查是否达到最大行数限制
                if max_rows and len(all_data) >= max_rows:
                    self.logger.info(f"已达到最大行数限制: {max_rows}，停止继续导出")
                    all_data = all_data[:max_rows]  # 截取到最大行数
                    break

            if position in self.progress_bars and self.current_tasks.get(position) == desc:
                stop_reason = "达到最大行数限制" if (max_rows and len(all_data) >= max_rows) else "所有时间切片处理完毕"
                final_status = f"{desc} [完成，共 {len(all_data)} 条数据，{processed_slices}/{total_slices} 个切片，{stop_reason}]"
                self.progress_bars[position].set_description(final_status)
                self.progress_bars[position].refresh()

            stop_reason = "达到最大行数限制" if (max_rows and len(all_data) >= max_rows) else "所有时间切片处理完毕"
            self.logger.info(f"按时间切片获取数据完成 - 共获取 {len(all_data)} 条数据，处理了 {processed_slices}/{total_slices} 个切片，停止原因: {stop_reason}")
            return all_data

        except Exception as e:
            self.logger.error(f"按时间切片获取数据失败 - {desc} - 错误: {str(e)}")
            if position in self.progress_bars:
                self.progress_bars[position].clear()
            raise ExportError(f"按时间切片获取数据失败: {str(e)}")

    def fetch_data(self, start_time: str, end_time: str, max_rows: Optional[int] = None, position: int = 0, desc: str = "Fetching data") -> List[Dict]:
        """
        调用远程 API 获取数据，处理分页和异步查询
        
        Args:
            start_time: 开始时间（ISO 格式）
            end_time: 结束时间（ISO 格式）
            max_rows: 最大导出行数（可选，达到此数量时停止）
            position: 进度条位置
            desc: 进度条描述
            
        Returns:
            返回的数据列表（所有 series 的合并数据）
        """
        pbar = self._create_progress_bar(position, desc)
        
        try:
            max_rows_info = f" - 最大行数: {max_rows}" if max_rows else ""
            self.logger.info(f"开始获取数据 - {desc} - 开始时间: {start_time} - 结束时间: {end_time}{max_rows_info}")
            
            # 转换时间戳
            start_timestamp = self._iso_to_timestamp(start_time)
            end_timestamp = self._iso_to_timestamp(end_time)
            cursor_time = end_timestamp  # 初始 cursor_time 为结束时间
            
            all_data = []  # 存储所有分页的数据
            cursor_token = None
            page_count = 0
            reached_max_rows = False  # 标记是否达到最大行数
            
            while True:
                page_count += 1
                self.logger.info(f"开始获取第 {page_count} 页数据 - cursor_time: {cursor_time}")
                
                # 构建查询参数
                query_params = {
                    "q": self.dql,
                    "timeRange": [start_timestamp, end_timestamp],
                    "orderby": [{"time": "desc"}],
                    "disableMultipleField": False,
                    "disable_sampling": True,
                    "disable_streaming_aggregation": True,
                    "align_time": False,
                    "offset": 0,
                    "slimit": 1000,
                    "limit": 1000,
                    "search_after": [],
                    "scan_completed": False,
                    "scan_index": "",
                    "cursor_time": cursor_time,
                    "indexes": ["default"],
                    "tz": "Asia/Shanghai"
                }
                
                self.logger.info(f"开始获取第 {page_count} 页数据 - query_params: {query_params}")

                # 如果有 cursor_token，添加到查询参数中
                if cursor_token:
                    query_params["cursor_token"] = cursor_token
                
                # 发送请求
                result = self._query_api(query_params)
                
                if not result.get("content", {}).get("data"):
                    self.logger.warning("API 返回数据为空")
                    break
                
                self.logger.info(f"查询结果: {result}")

                query_data = result["content"]["data"][0]
                query_status = query_data.get("query_status", "")
                
                # 处理异步查询
                if query_status != "finished":
                    async_id = query_data.get("async_id")
                    if async_id:
                        self.logger.info(f"查询转为异步，async_id: {async_id}，等待查询完成")
                        query_data = self._wait_for_async_query(async_id, position, desc)
                    else:
                        raise ExportError(f"查询状态异常且无 async_id: {query_status}")
                
                # 提取 series 数据
                series_list = query_data.get("series", [])
                if not series_list:
                    self.logger.warning("本次查询未返回 series 数据")
                    break
                
                # 合并所有 series 的数据
                for series in series_list:
                    column_names = series.get("column_names", [])
                    values = series.get("values", [])
                    
                    if not column_names or not values:
                        continue
                    
                    # 将二维数组转换为字典列表
                    for row in values:
                        # 检查是否达到最大行数
                        if max_rows and len(all_data) >= max_rows:
                            reached_max_rows = True
                            self.logger.info(f"已达到最大行数限制: {max_rows}，停止获取数据")
                            break
                        
                        row_dict = {}
                        for i, col_name in enumerate(column_names):
                            if i < len(row):
                                row_dict[col_name] = row[i]
                        all_data.append(row_dict)
                    
                    # 如果达到最大行数，跳出 series 循环
                    if reached_max_rows:
                        break
                
                # 更新进度条
                if position in self.progress_bars and self.current_tasks.get(position) == desc:
                    max_info = f"/{max_rows}" if max_rows else ""
                    status = f"{desc} [已获取 {len(all_data)}{max_info} 条数据，第 {page_count} 页]"
                    self.progress_bars[position].set_description(status)
                    self.progress_bars[position].refresh()
                
                # 如果达到最大行数，停止查询
                if reached_max_rows:
                    self.logger.info(f"已达到最大行数限制: {max_rows}，停止查询")
                    break
                
                # 检查是否有下一页
                next_cursor_time = query_data.get("next_cursor_time")
                next_cursor_token = query_data.get("next_cursor_token")
                
                if next_cursor_time and next_cursor_token:
                    cursor_time = next_cursor_time
                    cursor_token = next_cursor_token
                    self.logger.info(f"准备获取下一页 - next_cursor_time: {next_cursor_time}, next_cursor_token: {next_cursor_token}")
                else:
                    self.logger.info("没有更多数据，查询完成")
                    break
            
            if position in self.progress_bars and self.current_tasks.get(position) == desc:
                stop_reason = "达到最大行数限制" if reached_max_rows else "数据查询完毕"
                final_status = f"{desc} [完成，共 {len(all_data)} 条数据，{page_count} 页，{stop_reason}]"
                self.progress_bars[position].set_description(final_status)
                self.progress_bars[position].refresh()
            
            stop_reason = "达到最大行数限制" if reached_max_rows else "数据查询完毕"
            self.logger.info(f"数据获取完成 - 共获取 {len(all_data)} 条数据，{page_count} 页，停止原因: {stop_reason}")
            return all_data
            
        except Exception as e:
            self.logger.error(f"获取数据失败 - {desc} - 错误: {str(e)}")
            if position in self.progress_bars:
                self.progress_bars[position].clear()
            raise ExportError(f"数据获取失败: {str(e)}")
    
    def export_to_csv(self, data: List[Dict], output_path: Path, position: int = 0, desc: str = "Exporting to CSV") -> bool:
        """
        将数据导出到 CSV 文件
        
        Args:
            data: 要导出的数据列表（字典列表）
            output_path: 输出 CSV 文件路径
            desc: 进度条描述
            
        Returns:
            是否导出成功
        """
        pbar = self._create_progress_bar(position, desc)
        
        try:
            self.logger.info(f"开始导出数据到 CSV - {output_path} - 数据条数: {len(data)}")
            
            if not data:
                self.logger.warning("没有数据需要导出")
                if position in self.progress_bars and self.current_tasks.get(position) == desc:
                    final_status = f"{desc} [无数据]"
                    self.progress_bars[position].set_description(final_status)
                    self.progress_bars[position].refresh()
                return True
            
            # 确定 CSV 的列名（从第一条数据中提取所有键）
            fieldnames = list(data[0].keys())
            
            # 确保所有数据都有相同的字段（填充缺失字段）
            for row in data:
                for field in fieldnames:
                    if field not in row:
                        row[field] = ""
            
            # 创建输出目录
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 写入 CSV 文件
            with open(output_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for i, row in enumerate(data):
                    writer.writerow(row)
                    pbar.update(1)
                    
                    # 每 100 条更新一次进度条描述
                    if (i + 1) % 100 == 0:
                        if position in self.progress_bars and self.current_tasks.get(position) == desc:
                            status = f"{desc} [已导出 {i + 1}/{len(data)} 条]"
                            self.progress_bars[position].set_description(status)
                            self.progress_bars[position].refresh()
            
            if position in self.progress_bars and self.current_tasks.get(position) == desc:
                final_status = f"{desc} [完成，共 {len(data)} 条]"
                self.progress_bars[position].set_description(final_status)
                self.progress_bars[position].refresh()
            
            self.logger.info(f"数据导出完成 - {output_path} - 共导出 {len(data)} 条数据")
            return True
            
        except Exception as e:
            self.logger.error(f"导出数据失败 - {output_path} - 错误: {str(e)}")
            if position in self.progress_bars:
                self.progress_bars[position].clear()
            raise ExportError(f"数据导出失败: {str(e)}")
    
    def __del__(self):
        """清理所有进度条"""
        for pbar in self.progress_bars.values():
            pbar.clear()
            pbar.close()


class ExportError(Exception):
    """导出过程中的错误"""
    pass

