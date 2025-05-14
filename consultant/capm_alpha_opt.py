import re
from itertools import product
import hashlib

# 输入数据
alphas = [
    ['L57a0d2', "\n            target_data = winsorize(ts_backfill(implied_volatility_put_90, 63), std=4.0);\n            alpha_2088_basic = ts_regression(target_data, group_mean(target_data, log(ts_mean(cap, 21)), densify(bucket(rank(cap), range='0.1, 1, 0.1'))), 756, rettype=2);\n            alpha = rank(alpha_2088_basic);\n            alpha\n        ", 1.35, 0.0175, 0.9, 0.00635, 'TOP3000', 'USA', '2025-04-05T09:19:57-04:00', 6],
    ['XRv6Q01', "\n            target_data = winsorize(ts_backfill(call_breakeven_720, 63), std=4.0);\n            alpha_6515_basic = ts_regression(target_data, group_mean(target_data, log(ts_mean(cap, 21)), densify(bucket(rank(ts_std_dev(returns, 20)), range='0.1, 1, 0.1'))), 504, rettype=2);\n            alpha = rank(alpha_6515_basic);\n            alpha\n        ", 1.3, 0.0178, 0.9, 0.00671, 'TOP3000', 'USA', '2025-04-05T09:20:00-04:00', 6]
]

# 去重Alpha
def deduplicate_alphas(alphas, metric='sharpe'):
    """
    去重Alpha，保留指定指标表现最好的版本。
    
    参数：
    - alphas: Alpha列表，每个元素为 [alpha_id, exp, sharpe, turnover, fitness, margin, universe, region, dateCreated, decay]
    - metric: 用于比较的指标，'sharpe' 或 'fitness'，默认 'sharpe'
    
    返回：
    - deduped_alphas: 去重后的Alpha列表
    """
    target_data_map = {}
    
    # 选择比较指标的索引
    metric_index = 2 if metric == 'sharpe' else 4  # sharpe: 2, fitness: 4
    
    # 第一次遍历：记录每个target_data的最佳Alpha
    for alpha in alphas:
        match = re.search(r'target_data = (.*?);', alpha[1], re.DOTALL)
        if match:
            target_data = match.group(1).strip()
            if target_data not in target_data_map:
                target_data_map[target_data] = alpha
            else:
                # 如果已有该target_data，比较指标值，保留更好的
                current_best = target_data_map[target_data]
                if alpha[metric_index] > current_best[metric_index]:
                    target_data_map[target_data] = alpha
    
    # 收集去重后的Alpha
    deduped_alphas = list(target_data_map.values())
    return deduped_alphas
# 改造函数
def transform_target_data(original_exp, data_types=['implied_volatility_put'], 
                         vol_terms=['90', '180'], return_type='ts_mean(returns, 63)', 
                         type_weights=None, term_weights=None, return_weight=0.5, 
                         backfill_days=63, winsorize_std=3.5):
    patterns = {
        'implied_volatility_put': r'implied_volatility_put_(\d+)',
        'call_breakeven': r'call_breakeven_(\d+)',
        'put_breakeven': r'put_breakeven_(\d+)',
        'forward_price': r'forward_price_(\d+)'
    }
    
    match = re.search(r'target_data = (.*?);', original_exp, re.DOTALL)
    if not match:
        return None
    
    original_target_data = match.group(1).strip()
    original_type = None
    original_term = None
    
    for dtype, pattern in patterns.items():
        match = re.search(pattern, original_target_data)
        if match:
            original_type = dtype
            original_term = match.group(1)
            break
    
    if not original_type or original_type not in data_types or original_term != vol_terms[0]:
        return None

    if type_weights is None:
        type_weights = [1.0 / len(data_types)] * len(data_types)
    if term_weights is None:
        term_weights = [1.0 / len(vol_terms)] * len(vol_terms)

    components = []
    for dtype, type_w in zip(data_types, type_weights):
        for term, term_w in zip(vol_terms, term_weights):
            comp = f"{type_w * term_w} * ts_backfill({dtype}_{term}, {backfill_days})"
            components.append(comp)
    
    vol_expr = " + ".join(components)
    if return_type is None:
        combined_expr = vol_expr
    else:
        return_expr = f"{return_weight} * ts_backfill({return_type}, {backfill_days})"
        combined_expr = f"({vol_expr}) * ({return_expr})"
    
    standardized_expr = f"ts_zscore({combined_expr}, {backfill_days})"
    new_target_data = f"winsorize({standardized_expr}, std={winsorize_std})"
    
    # 处理后续行，清理多余分号
    lines = original_exp.strip().split('\n')
    subsequent_lines = [line.strip().rstrip(';').strip() for line in lines[1:] if line.strip()]
    
    # 拼接时每行末尾加单个分号，最后一行不加
    new_lines = ["target_data = " + new_target_data] + subsequent_lines
    new_exp = ";\n        ".join(new_lines[:-1]) + ";\n        " + new_lines[-1]
    return new_exp

# 生成改造后的Alpha代码
def generate_transformed_alpha(alpha_data, **transform_params):
    original_exp = alpha_data[1]
    alpha_id = alpha_data[0]
    
    new_exp = transform_target_data(original_exp, **transform_params)
    if new_exp:
        result = {
            'alpha_id': alpha_id,
            'transformed_exp': new_exp,
            'original_sharpe': alpha_data[2],
            'original_fitness': alpha_data[4],
            'params': transform_params,
            'exp_hash': hashlib.md5(new_exp.encode()).hexdigest()
        }
        return result
    else:
        return None

# 生成参数组合
def generate_param_combinations(data_types_set, vol_terms_set, return_type_set, return_weight_set, backfill_days_set, winsorize_std_set, 
                                type_weights_set, term_weights_set):
    combinations = []
    for data_types, vol_terms, return_type, return_weight, backfill_days, winsorize_std in product(
        data_types_set, vol_terms_set, return_type_set, return_weight_set, backfill_days_set, winsorize_std_set
    ):
        type_weights_options = type_weights_set[len(data_types)]
        term_weights_options = term_weights_set[len(vol_terms)]
        
        for type_weights, term_weights in product(type_weights_options, term_weights_options):
            params = {
                'data_types': data_types,
                'vol_terms': vol_terms,
                'return_type': return_type,
                'type_weights': type_weights,
                'term_weights': term_weights,
                'return_weight': return_weight if return_type is not None else 0.0,  # 无return_type时权重为0
                'backfill_days': backfill_days,
                'winsorize_std': winsorize_std
            }
            combinations.append(params)
    return combinations

# 去重改造结果
def deduplicate_transformed_alphas(transformed_alphas):
    seen_hashes = set()
    unique_alphas = []
    for ta in transformed_alphas:
        exp_hash = ta['exp_hash']
        if exp_hash not in seen_hashes:
            seen_hashes.add(exp_hash)
            unique_alphas.append(ta)
    return unique_alphas