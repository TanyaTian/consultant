
from machine_lib import *

import os
import csv
from typing import List, Tuple
import re
from fnmatch import fnmatch

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
    # 将 list_b 转换为集合，便于快速查找
    list_b_set = set(list_b)
    
    # 正则表达式 1：匹配函数式表达式 function_name(arg1 ,arg2)
    func_pattern = r'^(subtract|add|divide|ts_corr)\(([^,]+)\s*,\s*([^)]+)\)'
    
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

        # 遍历 alpha_pool 中的每个 alpha 和 decay
        for x, (alpha, decay) in enumerate(alpha_pool):
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
                    'visualization': False,  # 不启用可视化
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

def get_alphas_from_csv(csv_file_path, min_sharpe, min_fitness):
    """
    Process CSV file to generate alpha records in the format:
    [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
    
    Args:
        csv_file_path: Path to the CSV file containing alpha data
    
    Returns:
        List of filtered alpha records
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
                exp = regular.get('code', '')
                
                # Parse the is dictionary
                is_data = ast.literal_eval(row['is'])
                sharpe = is_data.get('sharpe', 0)
                fitness = is_data.get('fitness', 0)
                turnover = is_data.get('turnover', 0)
                margin = is_data.get('margin', 0)
                longCount = is_data.get('longCount', 0)
                shortCount = is_data.get('shortCount', 0)
                
                dateCreated = row['dateCreated']
                
                # Apply filter
                if (longCount + shortCount) > 100:
                    if (sharpe >= min_sharpe and fitness >= min_fitness) or (sharpe <= min_sharpe * -1.0 and fitness <= min_fitness * -1.0):
                    #if (sharpe >= min_sharpe and fitness >= min_fitness):
                        if sharpe is not None and sharpe < 0:
                            exp = f"-{exp}"
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