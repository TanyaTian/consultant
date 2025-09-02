
from machine_lib import *

import os
import csv
from typing import List, Tuple
import re
from fnmatch import fnmatch

ts_ops_2 = ["ts_rank", "ts_zscore", "ts_sum", "ts_delay", "ts_av_diff", "ts_ir",
            "ts_std_dev", "ts_mean",  "ts_arg_min", "ts_arg_max","ts_scale", "ts_quantile",
            "ts_kurtosis", "ts_max_diff", "ts_product", "ts_returns"]


def filter_fields_by_keywords(data, keywords=["gro", "pe", "chg"], minuskey=["pe"]):
    """
    Filter rows from data where id or description contains any of the specified keywords.
    Adds a '-' prefix to ids matched by keywords in minuskey. If minuskey is empty, no prefixes are added.
    Returns a deduplicated list.
    
    Args:
        data (list): List of dictionaries containing 'id' and 'description' keys.
        keywords (list): List of keywords to search for in id and description (case-insensitive).
        minuskey (list): List of keywords whose matches will have a '-' prefix in the output. Empty list means no prefixes.
    
    Returns:
        list: Deduplicated list of ids, with '-' prefix for minuskey-matched ids (if minuskey is non-empty).
    """
    # Input validation
    if not isinstance(data, list):
        raise ValueError("Data must be a list of dictionaries")
    if not isinstance(keywords, list) or not keywords:
        raise ValueError("Keywords must be a non-empty list")
    if not isinstance(minuskey, list):
        raise ValueError("minuskey must be a list")
    # Validate that minuskey is a subset of keywords (skip if minuskey is empty)
    if minuskey:
        invalid_minuskey = [k for k in minuskey if k not in keywords]
        if invalid_minuskey:
            raise ValueError(f"minuskey contains invalid keywords not in keywords: {invalid_minuskey}")
    
    # Initialize result set for deduplication
    result = set()
    minuskey_matched_ids = set()  # Track ids matched by minuskey keywords (if any)
    
    # Create regex patterns for each keyword (case-insensitive)
    regex_patterns = {keyword: re.compile(re.escape(keyword), re.IGNORECASE) for keyword in keywords}
    
    # Filter fields where id or description contains any keyword
    for item in data:
        if not isinstance(item, dict) or "id" not in item or "description" not in item:
            continue  # Skip invalid items
        field_id = item["id"]
        description = item["description"]
        
        # Check if any keyword matches id or description
        keyword_matched = False
        minuskey_matched = False
        for keyword, regex in regex_patterns.items():
            if regex.search(str(field_id)) or regex.search(str(description)):
                keyword_matched = True
                if minuskey and keyword in minuskey:
                    minuskey_matched = True
        
        # Add to result if any keyword matched
        if keyword_matched:
            result.add(field_id)
            if minuskey_matched:
                minuskey_matched_ids.add(field_id)
    
    # Create final list with '-' prefix for minuskey-matched ids (if minuskey is non-empty)
    return [f"-{field_id}" if field_id in minuskey_matched_ids else field_id for field_id in result]

def filter_fields_by_suffix(data, suffixes=["_gro", "_pe", "_chg"]):
    # Initialize result list
    result = []
    
    # Create regex pattern for suffixes
    pattern = "|".join(re.escape(suffix) + "$" for suffix in suffixes)
    
    # Filter fields matching any of the suffixes
    for item in data:
        field_id = item["id"]
        if re.search(pattern, field_id):
            result.append(field_id)
    
    return result

def generate_field_expressions(data, suffix1="_gro", suffix2="_st_dev", operator="/"):
    # Initialize result list
    result = []
    
    # Escape suffixes for regex
    suffix1_escaped = re.escape(suffix1)
    suffix2_escaped = re.escape(suffix2)
    
    # Separate fields with suffix1 and suffix2
    fields1 = [item for item in data if re.search(f"{suffix1_escaped}$", item["id"])]
    fields2 = [item for item in data if re.search(f"{suffix2_escaped}$", item["id"])]
    
    # For each field1, find matching field2 with same prefix
    for item1 in fields1:
        field1_id = item1["id"]
        # Extract prefix by removing suffix1
        prefix = re.sub(f"{suffix1_escaped}$", "", field1_id)
        
        # Find corresponding field2
        matching_field2 = next(
            (item for item in fields2 if item["id"] == f"{prefix}{suffix2}"),
            None
        )
        
        if matching_field2:
            field2_id = matching_field2["id"]
            # Generate expression
            expression = f"{field1_id} {operator} {field2_id}"
            result.append(expression)
    
    return result

def extract_target_data_fields(file_path):
    """
    从文件中每行代码字符串中提取 target_data 中的 data_field，返回去重并排序的列表。

    Args:
        file_path (str): 文件路径

    Returns:
        list: 去重并排序的 data_field 列表
    """
    # 正则表达式：匹配 target_data = winsorize(ts_backfill(data_field, 63), std=4.0);
    pattern = r'target_data\s*=\s*winsorize\(ts_backfill\(([^,]+),\s*63\),\s*std=4\.0\);'
    
    # 存储所有 data_field 的集合（去重）
    field_set = set()
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                # 将每行解析为列表
                try:
                    row = eval(line.strip())
                    if not isinstance(row, list) or len(row) < 2:
                        print(f"跳过无效行: {line.strip()}")
                        continue
                    
                    # 提取代码字符串（第二个元素）
                    code_str = row[1].strip()
                    
                    # 分割代码字符串为多行，取第一行
                    first_line = code_str.split('\n')[0].strip()  # 跳过首行空行
                    
                    # 匹配 target_data 定义
                    match = re.match(pattern, first_line)
                    if match:
                        data_field = match.group(1).strip()
                        field_set.add(data_field)
                    else:
                        print(f"行 '{first_line}' 不符合预期格式，跳过")
                
                except (SyntaxError, ValueError) as e:
                    print(f"解析行失败: {line.strip()}，错误: {e}")
                    continue
    except FileNotFoundError:
        print(f"文件 {file_path} 不存在")
        return []
    except Exception as e:
        print(f"读取文件时发生错误: {e}")
        return []
    return field_set

def filter_expressions_by_list_b(expression_list, list_b):
    """
    根据 list_b 过滤表达式列表，只保留所有字段都在 list_b 中的表达式。
    支持函数式表达式（如 subtract(arg1 ,arg2)）和数学式表达式（如 arg1 / arg2）。

    Args:
        expression_list (list): 表达式列表，例如 ['subtract(anl14_high_cfps_fp3 ,anl14_high_div_fp3)', 'fnd17_2anrhsfcfq / fnd17_2rhsfcfq']
        list_b (list): 过滤依据，例如 ['anl14_high_cfps_fp3', 'anl14_high_div_fp3']

    Returns:
        list: 过滤后的表达式列表
    """
    common_list = ['0', '4', '6', '9766', '1', '2', '3', '5', '10', '20', '250', '01', '90', '7',
                   '21', '50', '100', '252', '60', '40', '63', '365', 'ts_rank', 'ts_delay', 
                   'vec_avg', 'abs', 'ts_mean', 'power',
                   'ts_std_dev', 'ts_sum', 'ts_delta', 'signed_power', 'log']
    # 将 list_b 转换为集合，便于快速查找
    list_b_set = set(list_b + common_list)
    
    # 正则表达式 1：匹配函数式表达式 function_name(arg1 ,arg2)
    func_pattern = r'^(subtract|add|divide|ts_corr|multiply)\(([^,]+)\s*,\s*([^)]+)\)'
    
    # 正则表达式 2：匹配字段名称（字母、数字、下划线组成）
    field_pattern = r'[a-zA-Z0-9_]+'
    
    # 存储过滤后的表达式
    filtered_expressions = []
    
    # 打印过滤前数量
    print("过滤前表达式数量:", len(expression_list))
    
    # 遍历表达式列表
    for expr in expression_list:
        expr = expr.strip()
        
        # 首先尝试匹配函数式表达式
        func_match = re.match(func_pattern, expr)
        if func_match:
            # 提取 arg1 和 arg2
            arg1 = func_match.group(2).strip()
            arg2 = func_match.group(3).strip()
            fields = [arg1, arg2]
        else:
            # 如果不是函数式表达式，视为数学式表达式，提取所有字段
            fields = re.findall(field_pattern, expr)
            if not fields:
                print(f"表达式 '{expr}' 格式不正确或无有效字段，跳过")
                continue
        
        # 检查所有字段是否都在 list_b 中
        missing_fields = [field for field in fields if field not in list_b_set]
        if not missing_fields:
            filtered_expressions.append(expr)
        else:
            print(f"表达式 '{expr}' 被过滤：字段 {', '.join(missing_fields)} 不在 list_b 中")
    
    # 打印过滤后数量
    print("过滤后表达式数量:", len(filtered_expressions))
    
    return filtered_expressions


def generate_pending_simulation_data(alpha_pool: List[Tuple[str, int]],
                                     neut: str,
                                     region: str,
                                     universe: str,
                                     max_trade: str,
                                     visualization: bool = False,
                                     mode: str = "append",
                                     output_filename: str = None) -> None:
    """
    生成待模拟的 alpha 数据并写入指定的 CSV 文件。

    Args:
        alpha_pool (List[Tuple[str, int]]): 任务列表，每个任务包含多个 (alpha, decay) 元组。
        neut (str): 中性化参数，如 "MARKET", "SECTOR", "COUNTRY"。
        region (str): 区域，如 "USA"。
        universe (str): 投资宇宙，如 "TOP3000"。
        mode (str): 文件操作模式，"append" 表示追加，"overwrite" 表示删除后新建，默认为 "append"。
        output_filename (str, optional): 输出文件名，若未指定则使用默认值 "alpha_list_pending_simulated.csv"。

    Returns:
        None: 数据直接写入文件，不返回任何值。

    Raises:
        ValueError: 如果 mode 参数值无效。
    """
    # 验证 mode 参数
    if mode not in ["append", "overwrite"]:
        raise ValueError("mode must be 'append' or 'overwrite'")

    # 定义输出目录和默认文件名
    output_dir = "output"
    default_filename = "alpha_list_pending_simulated.csv"
    
    # 使用指定的文件名，若未指定则使用默认文件名
    filename = output_filename if output_filename else default_filename
    
    # 确保文件名以 .csv 结尾
    if not filename.endswith('.csv'):
        filename += '.csv'
    
    # 构造完整的输出文件路径
    output_file = os.path.join(output_dir, filename)

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)

    # 根据 mode 决定文件操作模式
    if mode == "overwrite":
        # 删除文件（如果存在）
        if os.path.exists(output_file):
            os.remove(output_file)
            print(f"Deleted existing file: {output_file}")
        file_mode = 'w'  # 新建模式
    else:
        file_mode = 'a'  # 追加模式

    # 打开文件写入
    with open(output_file, file_mode, newline='') as file:
        # 定义 CSV 文件的字段名，与 simulation_data 的结构一致
        fieldnames = ['type', 'settings', 'regular']
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        # 如果文件为空，写入表头
        if file_mode == 'w' or (file_mode == 'a' and os.path.getsize(output_file) == 0):
            writer.writeheader()

        # 遍历 alpha_pool 中的每个项目
        for x, item in enumerate(alpha_pool):
            # 处理不同的输入结构
            if isinstance(item, tuple) and len(item) >= 2:
                # 标准格式: (alpha, decay)
                alpha, decay = item[0], item[1]
            elif isinstance(item, str):
                # 只有 alpha 字符串，使用默认 decay
                alpha = item
                decay = 0  # 默认 decay 值
            else:
                print(f"跳过无效项目: {item}")
                continue
            
            # 构造 simulation_data，与 single_simulate 一致
            simulation_data = {
                'type': 'REGULAR',  # 模拟类型为常规 alpha
                'settings': {  # alpha 的配置参数
                    'instrumentType': 'EQUITY',  # 工具类型：股票
                    'region': region,  # 区域
                    'universe': universe,  # 投资宇宙
                    'delay': 1,  # 延迟天数
                    'decay': decay,  # 衰减参数
                    'neutralization': neut,  # 中性化参数
                    'truncation': 0.08,  # 截断参数
                    'pasteurization': 'ON',  # 启用巴氏消毒
                    'testPeriod': 'P0Y',  # 测试周期
                    'unitHandling': 'VERIFY',  # 单位处理
                    'nanHandling': 'ON',  # NaN 处理
                    'language': 'FASTEXPR',  # 表达式语言
                    'visualization': visualization,  # 不启用可视化
                    'maxTrade': max_trade
                },
                'regular': alpha  # alpha 表达式
            }

            # 将 settings 转换为字符串，以便写入 CSV
            simulation_data['settings'] = str(simulation_data['settings'])

            # 写入一行 simulation_data
            writer.writerow(simulation_data)

    print(f"All simulation data written to {output_file} in {mode} mode")


def get_ids_from_csv_directory(directory_path, suffix_pattern="*.csv"):
    """
    从指定目录下符合后缀模式的 CSV 文件中提取 id 列，返回合并后的 id 列表。

    Args:
        directory_path (str): CSV 文件所在的目录路径
        suffix_pattern (str, optional): 文件后缀模式，例如 '*_group.csv' 或 '*_matrix.csv'，默认为 '*.csv'

    Returns:
        list: 包含所有符合条件 CSV 文件中 id 的列表
    """
    # 存储所有 id 的列表
    id_list = []
    
    # 检查目录是否存在
    if not os.path.isdir(directory_path):
        print(f"目录 {directory_path} 不存在")
        return id_list
    
    # 获取目录下所有文件
    all_files = os.listdir(directory_path)
    
    # 过滤出符合后缀模式的 CSV 文件
    csv_files = [f for f in all_files if fnmatch(f, suffix_pattern)]
    
    if not csv_files:
        print(f"目录 {directory_path} 中没有符合模式 '{suffix_pattern}' 的 CSV 文件")
        return id_list
    
    # 遍历匹配的 CSV 文件
    for csv_file in csv_files:
        csv_file_path = os.path.join(directory_path, csv_file)
        try:
            # 读取 CSV 文件
            df = pd.read_csv(csv_file_path)
            
            # 确保 'id' 列存在
            if 'id' not in df.columns:
                print(f"文件 {csv_file_path} 中缺少 'id' 列，跳过")
                continue
            
            # 提取 id 列，添加到 id_list
            file_ids = df['id'].tolist()
            id_list.extend(file_ids)
            print(f"从 {csv_file_path} 提取 {len(file_ids)} 个 id: {file_ids}")
        
        except FileNotFoundError:
            print(f"文件 {csv_file_path} 不存在，跳过")
        except pd.errors.EmptyDataError:
            print(f"文件 {csv_file_path} 为空，跳过")
        except Exception as e:
            print(f"读取文件 {csv_file_path} 时发生错误: {e}，跳过")
    
    # 打印最终结果
    print("总计提取的 id 数量:", len(id_list))
    return id_list

def get_ids_from_csv_directory_with_coverage(directory_path, coverage_threshold, suffix_pattern="*.csv"):
    """
    从指定目录下符合后缀模式的 CSV 文件中提取 id 列，并过滤 coverage >= coverage_threshold 的记录，返回合并后的 id 列表。

    Args:
        directory_path (str): CSV 文件所在的目录路径
        coverage_threshold (float): coverage 阈值，只保留 coverage 大于等于此值的记录
        suffix_pattern (str, optional): 文件后缀模式，例如 '*_group.csv' 或 '*_matrix.csv'，默认为 '*.csv'

    Returns:
        list: 包含所有符合条件 CSV 文件中 id 的列表（满足 coverage 条件）
    """
    # 存储所有 id 的列表
    id_list = []
    
    # 检查目录是否存在
    if not os.path.isdir(directory_path):
        print(f"目录 {directory_path} 不存在")
        return id_list
    
    # 获取目录下所有文件
    all_files = os.listdir(directory_path)
    
    # 过滤出符合后缀模式的 CSV 文件
    csv_files = [f for f in all_files if fnmatch(f, suffix_pattern)]
    
    if not csv_files:
        print(f"目录 {directory_path} 中没有符合模式 '{suffix_pattern}' 的 CSV 文件")
        return id_list
    
    # 遍历匹配的 CSV 文件
    for csv_file in csv_files:
        csv_file_path = os.path.join(directory_path, csv_file)
        try:
            # 读取 CSV 文件
            df = pd.read_csv(csv_file_path)
            
            # 确保 'id' 列存在
            if 'id' not in df.columns:
                print(f"文件 {csv_file_path} 中缺少 'id' 列，跳过")
                continue

            # 检查是否有 'coverage' 列
            if 'coverage' not in df.columns:
                print(f"文件 {csv_file_path} 中缺少 'coverage' 列，跳过")
                continue

            # 过滤 coverage >= coverage_threshold 的记录
            df_filtered = df[df['coverage'] >= coverage_threshold]
            
            # 提取 id 列，添加到 id_list
            file_ids = df_filtered['id'].tolist()
            id_list.extend(file_ids)
            print(f"从 {csv_file_path} 提取 {len(file_ids)} 个 id (coverage>={coverage_threshold}): {file_ids}")
        
        except FileNotFoundError:
            print(f"文件 {csv_file_path} 不存在，跳过")
        except pd.errors.EmptyDataError:
            print(f"文件 {csv_file_path} 为空，跳过")
        except Exception as e:
            print(f"读取文件 {csv_file_path} 时发生错误: {e}，跳过")
    
    # 打印最终结果
    print("总计提取的 id 数量 (满足coverage条件):", len(id_list))
    return id_list

def filter_list_b_by_csv(csv_file_path, list_b):
    """
    从 CSV 文件中提取 id 列，生成 list_a，用 list_a 过滤 list_b，返回过滤结果，并打印数量。

    Args:
        csv_file_path (str): CSV 文件路径
        list_b (list): 需要过滤的列表

    Returns:
        list: 过滤后的 list_b
    """
    try:
        # 读取 CSV 文件
        df = pd.read_csv(csv_file_path)
        
        # 确保 'id' 列存在
        if 'id' not in df.columns:
            raise KeyError("CSV 文件中缺少 'id' 列")
        
        # 提取 id 列，生成 list_a
        list_a = df['id'].tolist()
        
        # 打印 list_a（可选，用于验证）
        print("list_a:", list_a)
        
        # 打印过滤前数量
        print("过滤前 list_b 数量:", len(list_b))
        
        # 过滤 list_b，只保留存在于 list_a 的元素
        filtered_list_b = [item for item in list_b if item in list_a]
        
        # 打印过滤后数量
        print("过滤后 list_b 数量:", len(filtered_list_b))
        
        return filtered_list_b
    
    except FileNotFoundError:
        print(f"文件 {csv_file_path} 不存在")
        return []
    except KeyError as e:
        print(e)
        return []
    except Exception as e:
        print(f"发生错误: {e}")
        return []

def extract_unique_sorted_args(string_list):
    """
    从包含 'function_name(arg1 ,arg2)' 形式的字符串列表中提取参数，返回去重并排序的列表。
    支持 subtract、add、divide 等函数名。

    Args:
        string_list (list): 包含类似 'subtract(anl14_high_cfps_fp3 ,anl14_high_div_fp3)' 的字符串列表

    Returns:
        list: 去重并排序后的参数列表
    """
    # 存储所有提取的参数
    args_set = set()

    # 正则表达式：匹配 function_name(arg1 ,arg2) 中的 arg1 和 arg2
    # function_name 可以是 subtract、add、divide 等
    pattern = r'^(subtract|add|divide|ts_corr)\(([^,]+)\s*,\s*([^)]+)\)'

    # 遍历字符串列表
    for s in string_list:
        # 使用正则表达式匹配
        match = re.match(pattern, s.strip())
        if match:
            # 提取 arg1 和 arg2，去除首尾空格
            arg1 = match.group(2).strip()
            arg2 = match.group(3).strip()
            # 添加到集合中（自动去重）
            args_set.add(arg1)
            args_set.add(arg2)
        else:
            print(f"字符串 '{s}' 格式不正确，跳过")

    # 转换为列表并排序
    unique_sorted_args = sorted(list(args_set))
    return unique_sorted_args


def concat_datafields(df):

    datafields = []
    datafields += df[df['type'] == "MATRIX"]["id"].tolist()
    datafields += get_vec_fields(df[df['type'] == "VECTOR"]["id"].tolist())
    return datafields

def process_list_datafields(df_list):
    return ["winsorize(ts_backfill(%s, 120), std=4)"%field for field in df_list]


def read_txt_to_list(file_path="data/datafield.txt") -> list:
    """
    将 txt 文件中的内容读取到列表，每行作为一个元素。

    Args:
        file_path (str): txt 文件路径，默认为 "data/datafield.txt"。

    Returns:
        list: 包含文件每行内容的列表。

    Raises:
        FileNotFoundError: 如果文件不存在，抛出异常并返回空列表。
    """
    result = []  # 初始化空列表，用于存储文件内容

    try:
        # 以读取模式打开文件
        with open(file_path, 'r', encoding='utf-8') as file:
            # 逐行读取文件内容
            for line in file:
                # 去除每行首尾的空白字符（如换行符 \n），并添加到列表
                result.append(line.strip())

        print(f"Successfully read {len(result)} lines from {file_path}")
        return result

    except FileNotFoundError:
        # 处理文件不存在的情况
        print(f"Error: File {file_path} not found.")
        return []
    except Exception as e:
        # 处理其他可能的异常
        print(f"Error occurred while reading {file_path}: {e}")
        return []


def merge_csv_files(file_a_path, file_b_path, mode='prepend'):
    """
    合并两个 CSV 文件，并将结果保存为 A 的文件名。

    Args:
        file_a_path (str): 文件 A 的路径。
        file_b_path (str): 文件 B 的路径。
        mode (str): 合并模式，'prepend' 表示 B 插在 A 前面，'append' 表示 B 追加在 A 后面。默认为 'prepend'。

    Raises:
        FileNotFoundError: 如果文件 A 或文件 B 不存在。
        ValueError: 如果 mode 参数值无效。
        RuntimeError: 如果合并过程中发生其他错误。
    """
    # 验证 mode 参数
    if mode not in ['prepend', 'append']:
        raise ValueError("mode 参数必须是 'prepend' 或 'append'")

    # 检查文件是否存在
    if not os.path.exists(file_a_path):
        raise FileNotFoundError(f"文件 A 不存在：{file_a_path}")
    if not os.path.exists(file_b_path):
        raise FileNotFoundError(f"文件 B 不存在：{file_b_path}")

    # 临时文件路径（与 A 文件同目录）
    temp_file_path = os.path.join(os.path.dirname(file_a_path), 'temp_merged.csv')

    try:
        # 打开文件 A 和文件 B
        with open(file_a_path, 'r', newline='') as file_a, \
             open(file_b_path, 'r', newline='') as file_b, \
             open(temp_file_path, 'w', newline='') as temp_file:

            # 创建 CSV 读取器和写入器
            reader_a = csv.DictReader(file_a)
            reader_b = csv.DictReader(file_b)
            fieldnames = reader_a.fieldnames  # 获取表头

            # 验证文件 B 的表头是否与文件 A 一致
            if reader_b.fieldnames != fieldnames:
                raise ValueError("文件 A 和文件 B 的表头不一致")

            writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
            writer.writeheader()  # 写入表头

            # 根据 mode 决定写入顺序
            if mode == 'prepend':
                # 先写入 B 的内容
                for row in reader_b:
                    writer.writerow(row)
                # 再写入 A 的内容
                for row in reader_a:
                    writer.writerow(row)
            else:  # mode == 'append'
                # 先写入 A 的内容
                for row in reader_a:
                    writer.writerow(row)
                # 再写入 B 的内容
                for row in reader_b:
                    writer.writerow(row)

        # 删除原文件 A
        os.remove(file_a_path)

        # 将临时文件重命名为 A 的文件名
        os.rename(temp_file_path, file_a_path)

        print(f"文件合并完成（模式：{mode}），结果已保存为：{file_a_path}")

    except Exception as e:
        # 如果发生错误，删除临时文件
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        raise RuntimeError(f"文件合并失败：{e}")


def fetch_data(session, params):
    """
    使用 session.get 方法调用 API 获取数据集，并返回 DataFrame 格式的 results 数据。

    Args:
        session: requests.Session 对象，用于发送 HTTP 请求。
        params (dict): API 请求参数字典，例如 {'category': 'analyst', 'instrumentType': 'EQUITY', ...}。

    Returns:
        pd.DataFrame: API 返回的 results 数据，转换为 DataFrame 格式。如果请求失败或无数据，返回空的 DataFrame。
    """
    base_url = "https://api.worldquantbrain.com/data-sets"
    try:
        # 使用 session.get 发送请求，params 自动转换为查询字符串
        response = session.get(base_url, params=params)
        response.raise_for_status()  # 检查请求是否成功，若失败抛出异常
        data = response.json()
        results = data.get('results', [])
        if not results:
            print(f"类别 '{params.get('category', 'unknown')}' 未返回数据")
            return pd.DataFrame()  # 返回空的 DataFrame
        return pd.DataFrame(results)
    except requests.exceptions.RequestException as e:
        print(f"获取数据失败，错误: {e}")
        return pd.DataFrame()

def write_to_csv(data, filename):
    """
    将 DataFrame 数据追加写入 CSV 文件。

    Args:
        data (pd.DataFrame): 要写入的数据。
        filename (str): 目标 CSV 文件名。
    """
    if data.empty:
        return
    # 转换为字典列表以写入 CSV
    data_dict = data.to_dict('records')
    with open(filename, 'a', newline='', encoding='utf-8') as file:
        fieldnames = data.columns.tolist()
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        # 如果文件为空，写入表头
        if os.path.getsize(filename) == 0:
            writer.writeheader()
        for row in data_dict:
            writer.writerow(row)

import csv
import ast

def get_alphas_from_csv(csv_file_path, min_sharpe, min_fitness, mode="track", region_filter=None, single_data_set_filter=None):
    """
    Process CSV file to generate alpha records in the format:
    [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
    
    Args:
        csv_file_path: Path to the CSV file containing alpha data
        min_sharpe: Minimum sharpe ratio threshold (e.g. 1.0)
        min_fitness: Minimum fitness threshold (e.g. 0.5)
        mode: Filter mode - 'submit' or 'track' (default: 'track')
        region_filter: Optional region filter (e.g. "USA"). If None, no region filtering.
        single_data_set_filter: Optional boolean to filter Single Data Set Alphas. If None, no filtering.
    
    Returns:
        List of filtered alpha records

    Examples:
        # Basic usage - filter by sharpe and fitness only
        results = get_alphas_from_csv("output/simulated_alphas.csv", 1.0, 0.5)
        
        # Filter for USA region only
        results = get_alphas_from_csv("output/simulated_alphas.csv", 1.0, 0.5, 
                                    region_filter="USA")
                                    
        # Filter for Single Data Set Alphas only
        results = get_alphas_from_csv("output/simulated_alphas.csv", 1.0, 0.5,
                                    single_data_set_filter=True)
                                    
        # Combined filter - USA region and Single Data Set Alphas
        results = get_alphas_from_csv("output/simulated_alphas.csv", 1.0, 0.5,
                                    region_filter="USA", single_data_set_filter=True)
    """
    output = []
    
    with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            try:
                # Parse the necessary fields from the row
                alpha_id = row['id']
                
                # Parse the settings dictionary
                settings = ast.literal_eval(row['settings'])
                decay = settings.get('decay', 0)
                
                # Parse the regular code (expression)
                regular = ast.literal_eval(row['regular'])
                exp = fix_newline_expression(regular.get('code', ''))
                operatorCount = regular.get('operatorCount', 0)
                
        
                # Parse the is dictionary
                is_data = ast.literal_eval(row['is'])
                sharpe = is_data.get('sharpe', 0)
                fitness = is_data.get('fitness', 0)
                turnover = is_data.get('turnover', 0)
                margin = is_data.get('margin', 0)
                longCount = is_data.get('longCount', 0)
                shortCount = is_data.get('shortCount', 0)
                checks = is_data.get('checks', [])
                
                dateCreated = row['dateCreated']
                has_failed_checks = any(check['result'] == 'FAIL' for check in checks)
                # Apply region filter if specified
                if region_filter is not None and settings.get('region') != region_filter:
                    continue
                    
                # Apply single data set filter if specified
                if single_data_set_filter is not None:
                    classifications = ast.literal_eval(row.get('classifications', '[]'))
                    is_single_data_set = any(
                        classification.get('id') == 'DATA_USAGE:SINGLE_DATA_SET' 
                        for classification in classifications
                    )
                    if single_data_set_filter != is_single_data_set:
                        continue
                    elif operatorCount > 8:
                        continue
                
                # Apply other filters
                if (longCount + shortCount) > 100:
                    if mode == "submit":
                        if (sharpe >= min_sharpe and fitness >= min_fitness) and not has_failed_checks:
                            if sharpe is not None and sharpe < 0:
                                exp = negate_expression(exp)
                            # Create the record
                            rec = [
                                alpha_id,
                                exp,
                                sharpe,
                                turnover,
                                fitness,
                                margin,
                                dateCreated,
                                decay
                            ]
                            
                            # Extract pyramids info from checks if available
                            pyramid_checks = [check for check in checks if check.get('name') == 'MATCHES_PYRAMID']
                            if pyramid_checks and 'pyramids' in pyramid_checks[0]:
                                pyramids = pyramid_checks[0]['pyramids']
                                rec.insert(7, pyramids)  # Insert after dateCreated
                            
                            # Add additional decay modifier based on turnover
                            if turnover > 0.7:
                                rec.append(decay * 4)
                            elif turnover > 0.6:
                                rec.append(decay * 3 + 3)
                            elif turnover > 0.5:
                                rec.append(decay * 3)
                            elif turnover > 0.4:
                                rec.append(decay * 2)
                            elif turnover > 0.35:
                                rec.append(decay + 4)
                            elif turnover > 0.3:
                                rec.append(decay + 2)
                            
                            output.append(rec)
                    else:  # track mode
                        if (sharpe >= min_sharpe and fitness >= min_fitness) or (sharpe <= min_sharpe * -1.0 and fitness <= min_fitness * -1.0):
                            if sharpe is not None and sharpe < 0:
                                exp = negate_expression(exp)
                            # Create the record
                            rec = [
                                alpha_id,
                                exp,
                                sharpe,
                                turnover,
                                fitness,
                                margin,
                                dateCreated,
                                decay
                            ]
                            
                            # Extract pyramids info from checks if available
                            pyramid_checks = [check for check in checks if check.get('name') == 'MATCHES_PYRAMID']
                            if pyramid_checks and 'pyramids' in pyramid_checks[0]:
                                pyramids = pyramid_checks[0]['pyramids']
                                rec.insert(7, pyramids)  # Insert after dateCreated
                            
                            # Add additional decay modifier based on turnover
                            if turnover > 0.7:
                                rec.append(decay * 4)
                            elif turnover > 0.6:
                                rec.append(decay * 3 + 3)
                            elif turnover > 0.5:
                                rec.append(decay * 3)
                            elif turnover > 0.4:
                                rec.append(decay * 2)
                            elif turnover > 0.35:
                                rec.append(decay + 4)
                            elif turnover > 0.3:
                                rec.append(decay + 2)
                            
                            output.append(rec)
                    
            except (ValueError, SyntaxError, KeyError) as e:
                # Skip rows with parsing errors or missing required fields
                continue
                
    return output


def filter_csv_by_keywords(csv_path, keywords):
    """
    过滤CSV文件，去除包含任何关键词的行，将结果保存为*_filter.csv文件。
    
    Args:
        csv_path (str): 要过滤的CSV文件路径
        keywords (list): 用于过滤的关键词列表
        
    Returns:
        str: 生成的过滤后文件路径
        
    Raises:
        FileNotFoundError: 如果输入文件不存在
        ValueError: 如果关键词列表为空
    """
    if not keywords:
        raise ValueError("关键词列表不能为空")
    
    # 生成输出文件名
    dir_path, filename = os.path.split(csv_path)
    base_name, ext = os.path.splitext(filename)
    output_path = os.path.join(dir_path, f"{base_name}_filter{ext}")
    
    # 读取输入文件并过滤行
    with open(csv_path, 'r', encoding='utf-8') as infile, \
         open(output_path, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # 写入表头
        header = next(reader)
        writer.writerow(header)
        
        # 过滤数据行
        for row in reader:
            # 检查行中是否包含任何关键词
            row_text = ','.join(row).lower()
            if not any(keyword.lower() in row_text for keyword in keywords):
                writer.writerow(row)
    
    return output_path

def split_csv(csv_path, num_splits, output_dir=None):
    """
    将 CSV 文件拆分为多个文件，每个文件包含除表头外的平均行数，表头与原文件一致。

    Args:
        csv_path (str): 要拆分的 CSV 文件路径
        num_splits (int): 要拆分的文件数量
        output_dir (str, optional): 输出目录路径。如果未指定，则使用原文件所在目录。
    """
    import math
    import os

    # 设置输出目录
    if output_dir is None:
        output_dir = os.path.dirname(csv_path)
    else:
        os.makedirs(output_dir, exist_ok=True)

    # 读取原 CSV 文件
    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        header = next(reader)  # 读取表头
        rows = list(reader)  # 读取所有行

    # 计算每个文件应包含的行数
    rows_per_file = math.ceil(len(rows) / num_splits)

    # 获取文件名和扩展名
    base_name = os.path.basename(csv_path)
    base_name, ext = os.path.splitext(base_name)

    # 拆分文件
    for i in range(num_splits):
        # 计算当前文件的起始和结束行
        start = i * rows_per_file
        end = start + rows_per_file

        # 生成新文件名
        new_file = os.path.join(output_dir, f"{base_name}_part{i+1}{ext}")

        # 写入新文件
        with open(new_file, 'w', encoding='utf-8', newline='') as out_file:
            writer = csv.writer(out_file)
            writer.writerow(header)  # 写入表头
            writer.writerows(rows[start:end])  # 写入行


def extract_id_description(csv_path, output_dir=None):
    """
    从 CSV 文件中提取 id 和 description 列，生成新的 CSV 文件。

    Args:
        csv_path (str): 要处理的 CSV 文件路径
        output_dir (str, optional): 输出目录路径。如果未指定，则使用原文件所在目录。
    """
    import os

    # 设置输出目录
    if output_dir is None:
        output_dir = os.path.dirname(csv_path)
    else:
        os.makedirs(output_dir, exist_ok=True)

    # 读取原 CSV 文件
    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    # 获取文件名和扩展名
    base_name = os.path.basename(csv_path)
    base_name, ext = os.path.splitext(base_name)

    # 生成新文件名
    new_file = os.path.join(output_dir, f"{base_name}_id_description{ext}")

    # 写入新文件
    with open(new_file, 'w', encoding='utf-8', newline='') as out_file:
        writer = csv.DictWriter(out_file, fieldnames=['id', 'description'])
        writer.writeheader()
        for row in rows:
            writer.writerow({'id': row['id'], 'description': row['description']})


def view_alphas_margin(gold_bag):
    s = login()
    sharp_list = []
    for gold, pc in gold_bag:

        triple = locate_alpha(s, gold)
        info = [triple[0], triple[2], triple[3], triple[4], triple[5], triple[6], triple[1]]
        info.append(pc)
        sharp_list.append(info)

    sharp_list.sort(reverse=True, key = lambda x : x[4]) # x[4] 是 margin 的位置
    for i in sharp_list:
        print(i)


def parse_alpha_blocks(file_path,block_size=3):
    """
    Read a file containing alpha expression blocks and return them as a list.
    Each block must contain exactly 3 lines in the format:
    1. financial_data definition
    2. group definition 
    3. ts operation
    
    Args:
        file_path (str): Path to the input text file
        
    Returns:
        list: List of valid alpha blocks (each as a string with 3 lines)
    """
    with open(file_path, 'r') as f:
        lines = [line.strip() for line in f if line.strip()]
    
    # Group lines into blocks of block_size
    blocks = []
    for i in range(0, len(lines), block_size):
        block_lines = lines[i:i+block_size]
        if len(block_lines) == block_size:
            blocks.append('\n'.join(block_lines))
        else:
            print(f"Skipping incomplete block at line {i+1}")
    
    print(f"Found {len(blocks)} valid alpha blocks in {file_path}")
    return blocks


def fix_newline_expression(expression):
    """
    修复表达式中的";n"问题（应该是";\n"被错误转换）
    
    Args:
        expression (str): 输入表达式字符串
        
    Returns:
        str: 修复后的表达式（如果发现问题），否则返回原表达式
    """
    # 检查是否存在";n"问题
    if ";n" in expression:
        # 替换所有";n"为";\n"
        fixed_expression = expression.replace(";n", ";\n")
        return fixed_expression
    return expression


def negate_expression(expression):
    """
    为表达式添加负号
    
    Args:
        expression (str): 输入表达式字符串
        
    Returns:
        str: 处理后的表达式
            如果是单行表达式，直接在前面加负号
            如果是多行表达式，在最后一行前加负号
    """
    if "\n" not in expression:
        # 单行表达式
        return f"-({expression})"
    else:
        # 多行表达式
        lines = expression.split("\n")
        last_line = lines[-1].strip()
        # 在最后一行前加负号
        lines[-1] = f"-({last_line})"
        return "\n".join(lines)


def generate_atom_expressions(datafield: str, region: str, day: int = 10) -> list:
    """
    根据datafield、region和day生成表达式列表
    
    参数:
        datafield (str): 数据字段名
        region (str): 地区，必须是'USA', 'ASI', 'EUR', 'GLB'或'CHN'（大写）
        day (int): 时间窗口d的值，默认为10
        
    返回:
        list: 包含所有表达式变体的字符串列表
    """
    # 定义各地区的group集合
    usa_atom_group = ["market", "sector", "industry", "subindustry", "exchange"]
    
    asi_atom_group = ["market", "sector", "industry", "subindustry", "exchange", "country",
                     "group_cartesian_product(country, market)", 
                     "group_cartesian_product(country, industry)", 
                     "group_cartesian_product(country, subindustry)", 
                     "group_cartesian_product(country, exchange)",
                     "group_cartesian_product(country, sector)"]
    
    eur_atom_group = asi_atom_group.copy()
    glb_atom_group = asi_atom_group.copy()
    
    chn_atom_group = ["market", "sector", "industry", "subindustry", "exchange"]
    
    # 根据region选择对应的group集合（直接匹配大写）
    if region == 'USA':
        groups = usa_atom_group
    elif region == 'ASI':
        groups = asi_atom_group
    elif region == 'EUR':
        groups = eur_atom_group
    elif region == 'GLB':
        groups = glb_atom_group
    elif region == 'CHN':
        groups = chn_atom_group
    else:
        raise ValueError(f"无效的region: {region}，必须是'USA', 'ASI', 'EUR', 'GLB'或'CHN'（大写）")
    
    expressions = []
    
    # 1. x (只有1个)
    expressions.append(f"{datafield}")
    
    # 2-5. 涉及单个group的表达式 (每个group生成一个)
    for group in groups:
        # 2. x - group_mean(x, 1, group)
        expressions.append(f"{datafield} - group_mean({datafield}, 1, densify({group}))")
        
        # 3. x / group_mean(x, 1, group)
        expressions.append(f"{datafield} / group_mean({datafield}, 1, densify({group}))")
        
        # 4. ts_corr(x, group_mean(x, 1, group), day)
        expressions.append(f"ts_corr({datafield}, group_mean({datafield}, 1, densify({group})), {day})")
        
        # 5. ts_regression(x, group_mean(x, 1, group), day, rettype = 0)
        expressions.append(f"ts_regression({datafield}, group_mean({datafield}, 1, densify({group})), {day}, rettype = 0)")
    
    # 6-11. 涉及两个不同group的表达式 (遍历所有group1 != group2的组合)
    for i in range(len(groups)):
        for j in range(len(groups)):
            if i != j:
                group1, group2 = groups[i], groups[j]
                
                # 6. group_rank(x, group1) - group_rank(x, group2)
                expressions.append(f"group_rank({datafield}, densify({group1})) - group_rank({datafield}, densify({group2}))")
                
                # 7. group_rank(x, group1) / group_rank(x, group2)
                expressions.append(f"group_rank({datafield}, densify({group1})) / group_rank({datafield}, densify({group2}))")
                
                # 8. ts_corr(group_rank(x, group1), group_rank(x, group2), day)  op超长，暂时不使用
                expressions.append(f"ts_corr(group_rank({datafield}, densify({group1})), group_rank({datafield}, densify({group2})), {day})")
                
                # 9. ts_regression(group_rank(x, group1), group_rank(x, group2), day, rettype = 0) op超长，暂时不使用
                expressions.append(f"ts_regression(group_rank({datafield}, densify({group1})), group_rank({datafield}, densify({group2})), {day}, rettype = 0)")
                
                # 10. group_rank(x, group1) - group_rank(group_mean(x, 1, group1),  group2) op超长，暂时不使用
                expressions.append(f"group_rank({datafield}, densify({group1})) - group_rank(group_mean({datafield}, 1, densify({group1})),  densify({group2}))")
                
                # 11. group_rank(x, group1) / group_rank(group_mean(x, 1, group1),  group2) op超长，暂时不使用
                expressions.append(f"group_rank({datafield}, densify({group1})) / group_rank(group_mean({datafield}, 1, densify({group1})), densify({group2}))")
    
    return expressions

def generate_std_expressions(datafield: str, region: str) -> list:
    """
    根据datafield、region和day生成表达式列表
    
    参数:
        datafield (str): 数据字段名
        region (str): 地区，必须是'USA', 'ASI', 'EUR', 'GLB'或'CHN'（大写）
        day (int): 时间窗口d的值，默认为10
        
    返回:
        list: 包含所有表达式变体的字符串列表
    """
    # 定义各地区的group集合
    usa_atom_group = ["sector"]
    
    asi_atom_group = ["sector"]
    
    eur_atom_group = asi_atom_group.copy()
    glb_atom_group = asi_atom_group.copy()
    
    chn_atom_group = ["sector"]
    
    # 根据region选择对应的group集合（直接匹配大写）
    if region == 'USA':
        groups = usa_atom_group
    elif region == 'ASI':
        groups = asi_atom_group
    elif region == 'EUR':
        groups = eur_atom_group
    elif region == 'GLB':
        groups = glb_atom_group
    elif region == 'CHN':
        groups = chn_atom_group
    else:
        raise ValueError(f"无效的region: {region}，必须是'USA', 'ASI', 'EUR', 'GLB'或'CHN'（大写）")
    
    expressions = []
    
    # 1. x (只有1个)
    expressions.append(f"{datafield}")
    
    # 2-5. 涉及单个group的表达式 (每个group生成一个)
    for group in groups:
        # 1. group_zscore(x)
        expressions.append(f"group_zscore({datafield}, densify({group}))")
        
        # 2. group_neutralize(x)
        expressions.append(f"group_neutralize({datafield}, densify({group}))")
        
        # 4. rank(x)
        expressions.append(f"rank({datafield})")

        # 5. zscore(x)
        expressions.append(f"zscore({datafield})")

        # 4. group_rank(x)
        expressions.append(f"group_rank({datafield}, densify({group}))")
        
    return expressions

def generate_velocity_acceleration(datafields: List[str], days: List[int]) -> List[str]:
    """
    生成基于速度与加速度模板的alpha表达式列表
    
    参数:
        datafields (List[str]): datafield列表
        days (List[int]): day列表
        
    返回:
        List[str]: 生成的alpha表达式列表
    """
    expressions = []
    for x in datafields:
        # 模板1: ts_delta(ts_delta(x, d), d)
        expr1 = f"ts_delta(ts_delta({x}, {days[0]}), {days[1]})"
        # 模板2: ts_delta(x, d)
        expr2 = f"ts_delta({x}, {days[0]})"
        expressions.append(expr1)
        expressions.append(expr2)
    return expressions

def ts_factory_with_day(op, field, days = None):
    output = []
    #days = [3, 5, 10, 20, 60, 120, 240]
    if days is None:
        days = [5, 22, 66, 120, 240]
    for day in days:
        alpha = "%s(%s, %d)"%(op, field, day)
        output.append(alpha)
    
    return output

def first_order_factory_with_day(fields, ops_set, days = None):
    alpha_set = []
    #for field in fields:
    for field in fields:
        #reverse op does the work
        #alpha_set.append(field)
        #alpha_set.append("-%s"%field)
        for op in ops_set:
            if op == "ts_percentage":
 
                alpha_set += ts_comp_factory(op, field, "percentage", [0.5])
 
            elif op == "ts_decay_exp_window":
 
                alpha_set += ts_comp_factory(op, field, "factor", [0.5])
 
            elif op == "ts_moment":
 
                alpha_set += ts_comp_factory(op, field, "k", [2, 3, 4])
 
            elif op == "ts_entropy":
 
                alpha_set += ts_comp_factory(op, field, "buckets", [10])
 
            elif op.startswith("ts_") or op == "inst_tvr":
 
                alpha_set += ts_factory_with_day(op, field, days)
 
            elif op.startswith("vector"):
 
                alpha_set += vector_factory(op, field)
 
            elif op == "signed_power":
 
                alpha = "%s(%s, 2)"%(op, field)
                alpha_set.append(alpha)
 
            else:
                alpha = "%s(%s)"%(op, field)
                alpha_set.append(alpha)
 
    return alpha_set

def sort_csv_by_description(input_path, output_path):
    try:
        # 存储有效行和对应的description（用于排序）
        rows = []
        descriptions = []
        error_lines = []
        
        with open(input_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            
            # 检查description列是否存在
            if 'description' not in headers:
                print(f"错误：文件 {input_path} 中不存在 'description' 列")
                print(f"可用列名：{', '.join(headers)}")
                return 1
                
            desc_index = headers.index('description')
            
            # 处理每一行
            for i, row in enumerate(reader, start=2):  # 行号从2开始（标题行是第1行）
                if len(row) != len(headers):
                    error_lines.append(i)
                    continue
                # 提取description，并去除首尾的双引号
                desc = row[desc_index].strip('"')
                rows.append(row)
                descriptions.append(desc)
        
        # 打印跳过行的警告
        if error_lines:
            print(f"警告：跳过 {len(error_lines)} 行格式错误（行号: {', '.join(map(str, error_lines))}）")
        
        # 根据descriptions对rows进行排序
        sorted_rows = [row for _, row in sorted(zip(descriptions, rows), key=lambda x: x[0])]
        
        # 写入输出文件
        with open(output_path, 'w', encoding='utf-8', newline='') as out_f:
            writer = csv.writer(out_f)
            writer.writerow(headers)
            writer.writerows(sorted_rows)
            
        print(f"文件已排序并保存至: {output_path}")
        return 0
    except Exception as e:
        print(f"处理过程中出错: {str(e)}")
        return 1
