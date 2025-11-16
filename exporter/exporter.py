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
    
    def __init__(self, api_domain: str, api_key: Optional[str] = None, dql: Optional[str] = None,
                 start_time: Optional[str] = None, end_time: Optional[str] = None,
                 max_rows: Optional[int] = None, time_slice_minutes: int = 5,
                 output_path: Optional[Path] = None):
        """
        初始化数据导出器
        
        Args:
            api_domain: API 域名（如：https://cn3-openapi.guance.com）
            api_key: API 密钥（可选，也可以从环境变量获取）
            dql: DQL 查询语句
            start_time: 开始时间（ISO 格式）
            end_time: 结束时间（ISO 格式）
            max_rows: 最大导出行数（可选，达到此数量时停止）
            time_slice_minutes: 时间切片大小（分钟），默认 5 分钟
            output_path: 输出文件路径（可选）
        """
        # 移除域名末尾的斜杠（如果有）
        api_domain = api_domain.rstrip('/')
        
        # 构建完整的 API URL
        self.api_url = f"{api_domain}{self.API_PATH}"
        
        self.api_key = api_key or os.getenv('API_KEY')
        self.dql = dql
        
        # 导出配置参数
        self.start_time = start_time
        self.end_time = end_time
        self.max_rows = max_rows
        self.time_slice_minutes = time_slice_minutes
        self.output_path = Path(output_path) if output_path else None
        
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
        生成时间切片列表，从 end_time 往 start_time 方向切片

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
        current_time = end_dt

        while current_time > start_dt:
            slice_start = max(current_time - timedelta(minutes=slice_minutes), start_dt)
            time_slices.append((
                slice_start.isoformat(),
                current_time.isoformat()
            ))
            current_time = slice_start

        return time_slices

    def fetch_data_with_time_slices(self, position: int = 0, desc: str = "Fetching data",
                                  resume_from_slice: int = 1, resume_context: Optional[Dict] = None) -> List[Dict]:
        """
        按时间切片获取数据

        Args:
            position: 进度条位置
            desc: 进度条描述
            resume_from_slice: 从第几个切片开始恢复（用于断点续传，默认1）
            resume_context: 恢复上下文，包含游标信息等（用于精确断点续传）

        Returns:
            返回的数据列表
        """
        # 验证必需的配置参数
        if not self.start_time or not self.end_time:
            raise ExportError("start_time 和 end_time 必须在初始化时设置")

        pbar = self._create_progress_bar(position, desc)

        try:
            # 生成时间切片
            time_slices = self._generate_time_slices(self.start_time, self.end_time, self.time_slice_minutes)
            total_slices = len(time_slices)

            # 处理断点续传：跳过已处理的切片
            if resume_from_slice > 1:
                if resume_from_slice <= len(time_slices):
                    time_slices = time_slices[resume_from_slice - 1:]
                    self.logger.info(f"从第 {resume_from_slice} 个切片开始恢复，共 {len(time_slices)} 个切片待处理")
                else:
                    self.logger.warning(f"恢复切片编号 {resume_from_slice} 超出范围，使用默认值1")
                    resume_from_slice = 1

            actual_total_slices = len(time_slices)
            self.logger.info(f"开始按时间切片获取数据 - {desc} - 开始时间: {self.start_time} - 结束时间: {self.end_time} - 切片大小: {self.time_slice_minutes}分钟 - 共 {actual_total_slices} 个切片")

            # 初始化进度条 - 显示总体信息
            def update_main_progress(completed_slices, total_data_count):
                if position in self.progress_bars and self.current_tasks.get(position) == desc:
                    progress_status = f"{desc} [已完成 {completed_slices}/{actual_total_slices} 个切片"
                    if resume_from_slice > 1:
                        progress_status += f"（从第 {resume_from_slice} 个切片恢复）"
                    progress_status += "]"
                    if self.max_rows:
                        progress_status += f" [累计 {total_data_count}/{self.max_rows} 条数据]"
                    else:
                        progress_status += f" [累计 {total_data_count} 条数据]"
                    self.progress_bars[position].set_description(progress_status)
                    self.progress_bars[position].refresh()

            # 显示初始状态
            update_main_progress(0, 0)

            all_data = []
            processed_slices = 0
            exported_count = 0  # 已导出到文件的数据量
            # 使用字典来存储停止标志，可以在回调函数和 fetch_data 之间共享
            stop_flags = {'should_stop': False}

            # 为每个切片创建独立的进度条
            slice_progress_positions = {}
            current_slice_info = None
            current_slice_exported = 0  # 当前切片已导出的数据量

            # 定义页面回调函数，用于在每一页数据获取完成后导出
            def page_export_callback(page_data, page_num, total_pages, context=None):
                nonlocal all_data, exported_count, current_slice_info, current_slice_exported

                # 如果已经达到限制，直接返回，不再处理
                if stop_flags['should_stop']:
                    return

                # 计算本次新增的数据
                previous_count = len(all_data)
                all_data = page_data  # 更新累积数据
                new_data_count = len(all_data) - previous_count
                new_data = all_data[previous_count:] if new_data_count > 0 else []

                # 更新主进度条 - 显示已完成的切片数和累计数据量
                update_main_progress(processed_slices, exported_count)

                # 更新切片进度条 - 显示切片处理状态和页面信息
                if current_slice_info:
                    slice_start, slice_end, slice_position = current_slice_info
                    if slice_position in self.progress_bars:
                        current_slice_num = processed_slices + 1
                        slice_status = f"切片 {current_slice_num}/{total_slices} [{slice_start[:19]} → {slice_end[:19]}] [处理中 - 页面 {page_num}，已导出 {current_slice_exported} 条]"
                        self.progress_bars[slice_position].set_description(slice_status)
                        self.progress_bars[slice_position].refresh()

                # 检查是否已达到全局最大行数限制
                if self.max_rows and exported_count >= self.max_rows:
                    stop_flags['should_stop'] = True
                    self.logger.info(f"已达到全局最大行数限制 {self.max_rows}，停止导出和查询（已导出 {exported_count} 条数据）")
                    return

                # 如果提供了输出路径，且有新增数据，且未达到最大行数限制，则追加导出到文件
                if self.output_path and new_data and (not self.max_rows or exported_count < self.max_rows):
                    # 计算本次可以导出的数据量
                    export_data = new_data
                    if self.max_rows and (exported_count + len(new_data)) > self.max_rows:
                        # 如果本次导出会导致超过限制，只导出剩余部分
                        remaining_slots = self.max_rows - exported_count
                        export_data = new_data[:remaining_slots]
                        stop_flags['should_stop'] = True
                        self.logger.info(f"本次导出将导致超过限制，只导出 {remaining_slots} 条数据，达到限制后停止")

                    try:
                        self.logger.info(f"页面 {page_num} 数据获取完成，正在追加导出新增数据到文件（新增 {len(export_data)} 条，累计 {len(page_data)} 条数据）")
                        # 对于页面导出，使用追加模式，避免覆盖之前的数据
                        self._append_to_csv(
                            data=export_data,
                            output_path=self.output_path,
                            is_first_batch=(exported_count == 0)  # 第一次写入时包含表头
                        )
                        exported_count += len(export_data)
                        current_slice_exported += len(export_data)  # 更新当前切片的导出计数
                        self.logger.info(f"页面 {page_num} 新增数据已追加到 {self.output_path}（本次追加 {len(export_data)} 条，文件累计 {exported_count} 条）")

                        # 导出后立即检查是否达到限制，如果达到则设置停止标志
                        if self.max_rows and exported_count >= self.max_rows:
                            stop_flags['should_stop'] = True
                            self.logger.info(f"已达到全局最大行数限制 {self.max_rows}，停止后续导出和查询（已导出 {exported_count} 条数据）")
                            return  # 立即返回，不再处理后续逻辑

                        # 保存断点信息，包含游标信息用于精确恢复
                        if context and not stop_flags['should_stop']:
                            self._save_checkpoint(
                                self.output_path,
                                processed_slices + 1,
                                total_slices,
                                exported_count,
                                context.get('slice_start', slice_start),
                                context.get('slice_end', slice_end),
                                next_cursor_time=context.get('next_cursor_time'),
                                next_cursor_token=context.get('next_cursor_token'),
                                current_page=page_num
                            )
                        elif not stop_flags['should_stop']:
                            self._save_checkpoint(self.output_path, processed_slices + 1, total_slices, exported_count, slice_start, slice_end)

                    except Exception as export_error:
                        self.logger.error(f"追加页面 {page_num} 数据失败: {export_error}")
                elif self.output_path and new_data and self.max_rows and len(page_data) > self.max_rows:
                    stop_flags['should_stop'] = True
                    self.logger.info(f"页面 {page_num} 数据已达到最大行数限制 {self.max_rows}，停止导出和查询（新增 {len(new_data)} 条，累计 {len(page_data)} 条数据）")

            for i, (slice_start, slice_end) in enumerate(time_slices):
                # 检查是否已达到全局最大行数限制，如果是则停止处理
                if self.max_rows and exported_count >= self.max_rows:
                    self.logger.info(f"已达到全局最大行数限制 {self.max_rows}，停止处理剩余切片（已导出 {exported_count} 条数据）")
                    break

                # 计算实际的切片编号（考虑断点续传）
                actual_slice_num = (resume_from_slice - 1) + i + 1

                # 计算当前切片允许的最大行数
                slice_max_rows = None
                if self.max_rows:
                    remaining_rows = self.max_rows - exported_count  # 使用已导出的数据量计算剩余
                    if remaining_rows <= 0:
                        self.logger.info(f"已达到最大行数限制: {self.max_rows}，跳过剩余切片")
                        break
                    # 设置切片的最大行数限制，但不传递给 fetch_data，因为 fetch_data 的 max_rows 用于控制单次查询的最大行数
                    # 这里我们通过 page_callback 来控制实际导出量
                    slice_max_rows = None  # 不限制单次查询，让它获取所有可用数据，然后在回调中控制导出

                # 为当前切片创建独立的进度条（位置在主进度条之后）
                slice_position = position + 1 + i  # 主进度条位置之后，从1开始
                slice_progress_positions[i] = slice_position

                # 设置当前切片信息，用于页面回调中更新进度
                current_slice_info = (slice_start, slice_end, slice_position)

                # 重置当前切片的导出计数
                current_slice_exported = 0

                # 创建并初始化切片进度条
                slice_progress_desc = f"切片 {actual_slice_num}/{total_slices} [{slice_start[:19]} → {slice_end[:19]}] [开始处理]"
                self._create_progress_bar(slice_position, slice_progress_desc)

                # 立即刷新显示
                if slice_position in self.progress_bars:
                    self.progress_bars[slice_position].refresh()

                self.logger.info(f"正在处理切片 {actual_slice_num}/{total_slices}：{slice_start} 至 {slice_end}")

                # 准备恢复上下文（如果有的话）
                slice_resume_context = None
                if resume_context and actual_slice_num == resume_from_slice:
                    slice_resume_context = resume_context

                # 获取当前切片的数据，使用页面回调函数
                slice_data = self.fetch_data(
                    start_time=slice_start,
                    end_time=slice_end,
                    max_rows=slice_max_rows,
                    position=slice_position + 1000,  # 远离切片进度条的位置
                    desc=f"切片 {actual_slice_num}/{total_slices} 数据获取",
                    show_progress=False,  # 不显示切片级别的详细进度
                    page_callback=page_export_callback if self.output_path else None,  # 只有当有输出路径时才设置回调
                    page_callback_context={'slice_start': slice_start, 'slice_end': slice_end},
                    resume_context=slice_resume_context,  # 传递恢复上下文
                    stop_flags=stop_flags  # 传递停止标志，用于在达到限制时停止查询
                )

                # 标记切片完成
                if slice_position in self.progress_bars:
                    final_slice_status = f"切片 {actual_slice_num}/{total_slices} [{slice_start[:19]} → {slice_end[:19]}] [完成，共导出 {current_slice_exported} 条数据]"
                    self.progress_bars[slice_position].set_description(final_slice_status)
                    self.progress_bars[slice_position].refresh()

                # 注意：由于使用了回调函数，all_data 已经在回调中更新了
                processed_slices += 1

                # 检查是否达到最大行数限制（基于已导出的数据量）
                if self.max_rows and exported_count >= self.max_rows:
                    self.logger.info(f"已达到最大行数限制: {self.max_rows}，停止继续导出（已导出 {exported_count} 条数据）")
                    all_data = all_data[:self.max_rows] if len(all_data) > self.max_rows else all_data
                    break

            # 如果达到了最大行数限制，需要截断数据并重新导出完整文件
            if self.max_rows and len(all_data) >= self.max_rows:
                self.logger.info(f"达到最大行数限制 {self.max_rows}，截断数据并重新导出完整文件")
                all_data = all_data[:self.max_rows]

                # 如果提供了输出路径，需要重新导出完整文件
                if self.output_path:
                    try:
                        self.logger.info(f"正在重新导出截断后的完整数据到文件（共 {len(all_data)} 条数据）")
                        self._export_to_csv_silent(data=all_data, output_path=self.output_path)
                        self.logger.info(f"截断后的数据已重新导出到 {self.output_path}（共 {len(all_data)} 条数据）")
                    except Exception as export_error:
                        self.logger.error(f"重新导出截断数据失败: {export_error}")

            # 清理所有进度条，避免与 CLI 的输出冲突
            for pbar in self.progress_bars.values():
                pbar.clear()
                pbar.close()
            self.progress_bars.clear()

            stop_reason = "达到最大行数限制" if (self.max_rows and len(all_data) >= self.max_rows) else "所有时间切片处理完毕"
            self.logger.info(f"按时间切片获取数据完成 - 共获取 {len(all_data)} 条数据，处理了 {processed_slices}/{total_slices} 个切片，停止原因: {stop_reason}")
            return all_data

        except Exception as e:
            self.logger.error(f"按时间切片获取数据失败 - {desc} - 错误: {str(e)}")
            # 清理所有进度条
            for pbar in self.progress_bars.values():
                pbar.clear()
                pbar.close()
            self.progress_bars.clear()
            raise ExportError(f"按时间切片获取数据失败: {str(e)}")

    def fetch_data(self, start_time: str, end_time: str, max_rows: Optional[int] = None, position: int = 0, desc: str = "Fetching data", show_progress: bool = True, page_callback: Optional[callable] = None, page_callback_context: Optional[Dict] = None, resume_context: Optional[Dict] = None, stop_flags: Optional[Dict] = None) -> List[Dict]:
        """
        调用远程 API 获取数据，处理分页和异步查询

        Args:
            start_time: 开始时间（ISO 格式）
            end_time: 结束时间（ISO 格式）
            max_rows: 最大导出行数（可选，达到此数量时停止）
            position: 进度条位置
            desc: 进度条描述
            show_progress: 是否显示详细进度信息
            page_callback: 每页数据处理完成后的回调函数，参数为(page_data: List[Dict], page_num: int, total_pages: int)

        Returns:
            返回的数据列表（所有 series 的合并数据）
        """
        if show_progress:
            pbar = self._create_progress_bar(position, desc)

        try:
            max_rows_info = f" - 最大行数: {max_rows}" if max_rows else ""
            self.logger.info(f"开始获取数据 - {desc} - 开始时间: {start_time} - 结束时间: {end_time}{max_rows_info}")
            
            # 转换时间戳
            start_timestamp = self._iso_to_timestamp(start_time)
            end_timestamp = self._iso_to_timestamp(end_time)

            all_data = []  # 存储所有分页的数据
            reached_max_rows = False  # 标记是否达到最大行数

            # 处理断点恢复：如果有恢复上下文，使用保存的游标信息
            if resume_context and resume_context.get('next_cursor_time') and resume_context.get('next_cursor_token'):
                cursor_time = resume_context['next_cursor_time']
                cursor_token = resume_context['next_cursor_token']
                page_count = resume_context.get('current_page', 1) - 1  # 从下一页开始，所以减1
                self.logger.info(f"从断点恢复：使用保存的游标信息继续查询 - cursor_time: {cursor_time}, 从第 {page_count + 1} 页开始")
            else:
                cursor_time = end_timestamp  # 初始 cursor_time 为结束时间
                cursor_token = None
                page_count = 0  # 初始化页码计数器
            
            while True:
                # 检查是否应该停止查询
                if stop_flags and stop_flags.get('should_stop', False):
                    self.logger.info(f"收到停止信号，停止继续查询（已获取 {len(all_data)} 条数据）")
                    break

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
                
                _next_cursor_time = result["content"]["data"][0].get("next_cursor_time", "")
                _trace_id = result.get("trace_id", "")
                self.logger.info(f"查询结果: {_next_cursor_time}, trace_id: {_trace_id}")
                if _next_cursor_time == -1:
                    self.logger.info(f"最后一次查询结果: {result}")

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

                # 调用页面回调函数，传入当前页的数据和上下文信息
                if page_callback:
                    try:
                        context = page_callback_context or {}
                        context.update({
                            'next_cursor_time': next_cursor_time,
                            'next_cursor_token': next_cursor_token,
                            'page_count': page_count,
                            'slice_start': start_time,
                            'slice_end': end_time
                        })
                        page_callback(all_data.copy(), page_count, None, context)  # total_pages 暂时设为 None，因为我们不知道总数
                    except Exception as callback_error:
                        self.logger.warning(f"页面回调函数执行失败: {callback_error}")

                # 检查是否应该停止（回调函数可能设置了停止标志）
                if stop_flags and stop_flags.get('should_stop', False):
                    self.logger.info(f"收到停止信号，停止继续查询（已获取 {len(all_data)} 条数据）")
                    break

                # 如果达到最大行数，停止查询
                if reached_max_rows:
                    self.logger.info(f"已达到最大行数限制: {max_rows}，停止查询")
                    break
                
                # 检查是否有下一页
                next_cursor_time = query_data.get("next_cursor_time")
                next_cursor_token = query_data.get("next_cursor_token")

                if next_cursor_time is not None and next_cursor_time != -1:
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

    def _export_to_csv_silent(self, data: List[Dict], output_path: Path) -> bool:
        """
        静默导出数据到 CSV 文件（不显示进度条）

        Args:
            data: 要导出的数据列表（字典列表）
            output_path: 输出 CSV 文件路径

        Returns:
            是否导出成功
        """
        try:
            self.logger.info(f"开始静默导出数据到 CSV - {output_path} - 数据条数: {len(data)}")

            if not data:
                self.logger.warning("没有数据需要导出")
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

                for row in data:
                    writer.writerow(row)

            self.logger.info(f"数据导出完成 - {output_path} - 共导出 {len(data)} 条数据")
            return True

        except Exception as e:
            self.logger.error(f"导出数据失败 - {output_path} - 错误: {str(e)}")
            raise ExportError(f"数据导出失败: {str(e)}")

    def _append_to_csv(self, data: List[Dict], output_path: Path, is_first_batch: bool = False) -> bool:
        """
        追加数据到 CSV 文件

        Args:
            data: 要追加的数据列表（字典列表）
            output_path: 输出 CSV 文件路径
            is_first_batch: 是否是第一次写入（需要写入表头）

        Returns:
            是否导出成功
        """
        try:
            if not data:
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

            # 打开文件：第一次写入用 'w' 模式（包含表头），后续用 'a' 模式（追加数据）
            mode = 'w' if is_first_batch else 'a'
            with open(output_path, mode, encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)

                # 只有第一次写入时才写表头
                if is_first_batch:
                    writer.writeheader()

                # 写入数据
                for row in data:
                    writer.writerow(row)

            return True

        except Exception as e:
            self.logger.error(f"追加数据到 CSV 失败 - {output_path} - 错误: {str(e)}")
            raise ExportError(f"追加数据失败: {str(e)}")

    def _save_checkpoint(self, output_path: Path, current_slice: int, total_slices: int,
                        exported_count: int, slice_start: str, slice_end: str,
                        next_cursor_time: Optional[str] = None, next_cursor_token: Optional[str] = None,
                        current_page: int = 1) -> None:
        """
        保存导出断点信息

        Args:
            output_path: 输出文件路径
            current_slice: 当前处理的切片编号
            total_slices: 总切片数
            exported_count: 已导出的数据条数
            slice_start: 当前切片的开始时间
            slice_end: 当前切片的结束时间
            next_cursor_time: 下一页的游标时间
            next_cursor_token: 下一页的游标令牌
            current_page: 当前切片已处理的页数
        """
        try:
            checkpoint_file = output_path.parent / f"{output_path.stem}_checkpoint.json"
            checkpoint_data = {
                "output_file": str(output_path),
                "current_slice": current_slice,
                "total_slices": total_slices,
                "exported_count": exported_count,
                "slice_start": slice_start,
                "slice_end": slice_end,
                "next_cursor_time": next_cursor_time,
                "next_cursor_token": next_cursor_token,
                "current_page": current_page,
                "timestamp": time.time()
            }

            import json
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)

            self.logger.debug(f"断点信息已保存到 {checkpoint_file}")
        except Exception as e:
            self.logger.warning(f"保存断点信息失败: {e}")

    def load_checkpoint(self, output_path: Path) -> Optional[Dict]:
        """
        加载导出断点信息

        Args:
            output_path: 输出文件路径

        Returns:
            断点信息字典，如果不存在则返回None
        """
        try:
            checkpoint_file = output_path.parent / f"{output_path.stem}_checkpoint.json"
            if not checkpoint_file.exists():
                return None

            import json
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)

            self.logger.info(f"找到断点信息文件: {checkpoint_file}")
            return checkpoint_data
        except Exception as e:
            self.logger.warning(f"加载断点信息失败: {e}")
            return None

    def resume_from_checkpoint(self) -> Optional[List[Dict]]:
        """
        从断点恢复导出（使用实例变量中的配置）

        Returns:
            如果成功恢复，返回已导出的数据；否则返回None
        """
        if not self.output_path:
            self.logger.error("输出路径未设置，无法恢复断点")
            return None

        checkpoint = self.load_checkpoint(self.output_path)
        if not checkpoint:
            return None

        try:
            current_slice = checkpoint["current_slice"]
            exported_count = checkpoint["exported_count"]
            next_cursor_time = checkpoint.get("next_cursor_time")
            next_cursor_token = checkpoint.get("next_cursor_token")
            current_page = checkpoint.get("current_page", 1)

            self.logger.info(f"从断点恢复导出：已处理 {current_slice-1}/{checkpoint['total_slices']} 个切片，当前切片已处理 {current_page} 页，已导出 {exported_count} 条数据")

            # 创建恢复上下文，包含游标信息
            resume_context = {
                'next_cursor_time': next_cursor_time,
                'next_cursor_token': next_cursor_token,
                'current_page': current_page,
                'exported_count': exported_count
            }

            # 重新开始导出，但跳过已处理的切片（使用实例变量中的配置）
            return self.fetch_data_with_time_slices(
                position=0,
                desc="Resuming data fetch",
                resume_from_slice=current_slice,
                resume_context=resume_context
            )
        except Exception as e:
            self.logger.error(f"从断点恢复导出失败: {e}")
            return None

    def __del__(self):
        """清理所有进度条"""
        for pbar in self.progress_bars.values():
            pbar.clear()
            pbar.close()


class ExportError(Exception):
    """导出过程中的错误"""
    pass

