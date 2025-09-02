import requests
from os import environ
from time import sleep
import time
import json
import pandas as pd
import random
import pickle
from urllib.parse import urljoin
from itertools import product
from itertools import combinations
from collections import defaultdict
import pickle
 
 
 
basic_ops = ["reverse", "inverse", "rank", "zscore", "quantile", "normalize"]
 
ts_ops = ["ts_rank", "ts_zscore",  "ts_sum", "ts_delta", "ts_delay", 
          "ts_std_dev", "ts_mean",  "ts_arg_min", "ts_arg_max","ts_scale", "ts_quantile"]
 
ops_set = basic_ops + ts_ops 

def login():
    username = ""
    password = ""
 
    # Create a session to persistently store the headers
    s = requests.Session()
 
    # Save credentials into session
    s.auth = (username, password)
 
    # Send a POST request to the /authentication API
    response = s.post('https://api.worldquantbrain.com/authentication')
    print(response.content)
    return s  


def get_datasets(
    s,
    instrument_type: str = 'EQUITY',
    region: str = 'USA',
    delay: int = 1,
    universe: str = 'TOP3000'
):
    url = "https://api.worldquantbrain.com/data-sets?" +\
        f"instrumentType={instrument_type}&region={region}&delay={str(delay)}&universe={universe}"
    result = s.get(url)
    datasets_df = pd.DataFrame(result.json()['results'])
    return datasets_df


def get_datafields(
    s,
    instrument_type: str = 'EQUITY',
    region: str = 'USA',
    delay: int = 1,
    universe: str = 'TOP3000',
    dataset_id: str = '',
    search: str = ''
):
    if len(search) == 0:
        url_template = "https://api.worldquantbrain.com/data-fields?" +\
            f"&instrumentType={instrument_type}" +\
            f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" +\
            "&offset={x}"
        count = s.get(url_template.format(x=0)).json()['count'] 
        
    else:
        url_template = "https://api.worldquantbrain.com/data-fields?" +\
            f"&instrumentType={instrument_type}" +\
            f"&region={region}&delay={str(delay)}&universe={universe}&limit=50" +\
            f"&search={search}" +\
            "&offset={x}"
        count = 100
    
    datafields_list = []
    for x in range(0, count, 50):
        datafields = s.get(url_template.format(x=x))
        datafields_list.append(datafields.json()['results'])
 
    datafields_list_flat = [item for sublist in datafields_list for item in sublist]
 
    datafields_df = pd.DataFrame(datafields_list_flat)
    return datafields_df

def get_vec_fields(fields):

    # 请在此处添加获得权限的Vector操作符
    vec_ops = ["vec_avg", "vec_sum"]
    vec_fields = []
 
    for field in fields:
        for vec_op in vec_ops:
            if vec_op == "vec_choose":
                vec_fields.append("%s(%s, nth=-1)"%(vec_op, field))
                vec_fields.append("%s(%s, nth=0)"%(vec_op, field))
            else:
                vec_fields.append("%s(%s)"%(vec_op, field))
 
    return(vec_fields)

def process_datafields(df):

    datafields = []
    datafields += df[df['type'] == "MATRIX"]["id"].tolist()
    datafields += get_vec_fields(df[df['type'] == "VECTOR"]["id"].tolist())
    return ["winsorize(ts_backfill(%s, 120), std=4)"%field for field in datafields]


def ts_factory(op, field):
    output = []
    #days = [3, 5, 10, 20, 60, 120, 240]
    days = [5, 22, 66, 120, 240]
    
    for day in days:
    
        alpha = "%s(%s, %d)"%(op, field, day)
        output.append(alpha)
    
    return output

def first_order_factory(fields, ops_set):
    alpha_set = []
    #for field in fields:
    for field in fields:
        #reverse op does the work
        alpha_set.append(field)
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
 
                alpha_set += ts_factory(op, field)
 
            elif op.startswith("vector"):
 
                alpha_set += vector_factory(op, field)
 
            elif op == "signed_power":
 
                alpha = "%s(%s, 2)"%(op, field)
                alpha_set.append(alpha)
 
            else:
                alpha = "%s(%s)"%(op, field)
                alpha_set.append(alpha)
 
    return alpha_set


def load_task_pool(alpha_list, limit_of_children_simulations, limit_of_multi_simulations):
    '''
    Input:
        alpha_list : list of (alpha, decay) tuples
        limit_of_multi_simulations : number of children simulation in a multi-simulation
        limit_of_multi_simulations : number of simultaneous multi-simulations
    Output:
        task : [10 * (alpha, decay)] for a multi-simulation
        pool : [10 * [10 * (alpha, decay)]] for simultaneous multi-simulations
        pools : [[10 * [10 * (alpha, decay)]]]

    '''
    tasks = [alpha_list[i:i + limit_of_children_simulations] for i in range(0, len(alpha_list), limit_of_children_simulations)]
    pools = [tasks[i:i + limit_of_multi_simulations] for i in range(0, len(tasks), limit_of_multi_simulations)]
    return pools

def multi_simulate(alpha_pools, neut, region, universe, start):

    s = login()

    brain_api_url = 'https://api.worldquantbrain.com'

    for x, pool in enumerate(alpha_pools):
        if x < start: continue
        progress_urls = []
        for y, task in enumerate(pool):
            # 10 tasks, 10 alpha in each task
            sim_data_list = generate_sim_data(task, region, universe, neut)
            try:
                simulation_response = s.post('https://api.worldquantbrain.com/simulations', json=sim_data_list)
                print(f"sim_data_list:{sim_data_list}")
                simulation_progress_url = simulation_response.headers['Location']
                print(simulation_progress_url)
                progress_urls.append(simulation_progress_url)
            except:
                print("location key error: %s"%simulation_response.content)
                sleep(600)
                s = login()

        print("pool %d task %d post done"%(x,y))

        for j, progress in enumerate(progress_urls):
            try:
                while True:
                    simulation_progress = s.get(progress)
                    if simulation_progress.headers.get("Retry-After", 0) == 0:
                        break
                    #print("Sleeping for " + simulation_progress.headers["Retry-After"] + " seconds")
                    sleep(float(simulation_progress.headers["Retry-After"]))

                status = simulation_progress.json().get("status", 0)
                print(f"status:{status}")
                if status != "COMPLETE":
                    print("Not complete : %s"%(progress))
                #alpha_id = simulation_progress.json()["alpha"]
                children = simulation_progress.json().get("children", 0)
                children_list = []
                for child in children:
                    child_progress = s.get(brain_api_url + "/simulations/" + child)
                    child_progress.raise_for_status()
                    alpha_id = child_progress.json()["alpha"]
                    children_list.append(alpha_id)
                print(f"children_list:{children_list}")
            except KeyError:
                print("look into: %s"%progress)
            except Exception as e:
                print(f"other error: {e}")


        print("pool %d task %d simulate done"%(x, j))
    
    print("Simulate done")

def generate_sim_data(alpha_list, region, uni, neut):
    sim_data_list = []
    for alpha, decay in alpha_list:
        simulation_data = {
            'type': 'REGULAR',
            'settings': {
                'instrumentType': 'EQUITY',
                'region': region,
                'universe': uni,
                'delay': 1,
                'decay': decay,
                'neutralization': neut,
                'truncation': 0.08,
                'pasteurization': 'ON',
                'testPeriod': 'P0Y',
                'unitHandling': 'VERIFY',
                'nanHandling': 'ON',
                'language': 'FASTEXPR',
                'visualization': False,
            },
            'regular': alpha}

        sim_data_list.append(simulation_data)
    return sim_data_list

def set_alpha_properties(
    s,
    alpha_id,
    name: str = None,
    color: str = None,
    selection_desc: str = "None",
    combo_desc: str = "None",
    tags: str = ["ace_tag"],
):
    """
    Function changes alpha's description parameters
    """
 
    params = {
        "color": color,
        "name": name,
        "tags": tags,
        "category": None,
        "regular": {"description": None},
        "combo": {"description": combo_desc},
        "selection": {"description": selection_desc},
    }
    response = s.patch(
        "https://api.worldquantbrain.com/alphas/" + alpha_id, json=params
    )

def get_alphas(start_date, end_date, sharpe_th, fitness_th, region, alpha_num, usage):
    s = login()
    output = []
    # 3E large 3C less
    count = 0
    for i in range(0, alpha_num, 100):
        print(i)
        url_e = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d"%(i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2025-" + start_date  \
                + "T00:00:00-04:00&dateCreated%3C2025-" + end_date \
                + "T00:00:00-04:00&is.fitness%3E" + str(fitness_th) + "&is.sharpe%3E" \
                + str(sharpe_th) + "&settings.region=" + region + "&order=-is.sharpe&hidden=false&type!=SUPER"
        url_c = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d"%(i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2025-" + start_date  \
                + "T00:00:00-04:00&dateCreated%3C2025-" + end_date \
                + "T00:00:00-04:00&is.fitness%3C-" + str(fitness_th) + "&is.sharpe%3C-" \
                + str(sharpe_th) + "&settings.region=" + region + "&order=is.sharpe&hidden=false&type!=SUPER"
        urls = [url_e]
        if usage != "submit":
            urls.append(url_c)
        for url in urls:
            response = s.get(url)
            #print(response.json())
            try:
                alpha_list = response.json()["results"]
                #print(response.json())
                for j in range(len(alpha_list)):
                    print(alpha_list[j])
                    alpha_id = alpha_list[j]["id"]
                    name = alpha_list[j]["name"]
                    dateCreated = alpha_list[j]["dateCreated"]
                    sharpe = alpha_list[j]["is"]["sharpe"]
                    fitness = alpha_list[j]["is"]["fitness"]
                    turnover = alpha_list[j]["is"]["turnover"]
                    margin = alpha_list[j]["is"]["margin"]
                    longCount = alpha_list[j]["is"]["longCount"]
                    shortCount = alpha_list[j]["is"]["shortCount"]
                    decay = alpha_list[j]["settings"]["decay"]
                    exp = alpha_list[j]['regular']['code']
                    count += 1
                    #if (sharpe > 1.2 and sharpe < 1.6) or (sharpe < -1.2 and sharpe > -1.6):
                    if (longCount + shortCount) > 100:
                        if sharpe < -sharpe_th:
                            exp = "-%s"%exp
                        rec = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
                        print(rec)
                        if turnover > 0.7:
                            rec.append(decay*4)
                        elif turnover > 0.6:
                            rec.append(decay*3+3)
                        elif turnover > 0.5:
                            rec.append(decay*3)
                        elif turnover > 0.4:
                            rec.append(decay*2)
                        elif turnover > 0.35:
                            rec.append(decay+4)
                        elif turnover > 0.3:
                            rec.append(decay+2)
                        output.append(rec)
            except:
                print("%d finished re-login"%i)
                s = login()

    print("count: %d"%count)
    return output

def digest(next_alpha_recs):
    output = []
    for rec in next_alpha_recs:
        decay = rec[-1]
        exp = rec[1]
        output.append([exp,decay])
    return output   

def prune(next_alpha_recs, prefix, keep_num):
    # prefix is the datafield prefix, fnd6, mdl175 ...
    # keep_num is the num of top sharpe same-datafield alpha
    output = []
    num_dict = defaultdict(int)
    for rec in next_alpha_recs:
        exp = rec[1]
        field = exp.split(prefix)[-1].split(",")[0]
        sharpe = rec[2]
        if sharpe < 0:
            field = "-%s"%field
        if num_dict[field] < keep_num:
            num_dict[field] += 1
            decay = rec[-1]
            exp = rec[1]
            output.append([exp,decay])
    return output

def prune_with_universe(next_alpha_recs, prefix, keep_num, universe):
    # prefix is the datafield prefix, fnd6, mdl175 ...
    # keep_num is the num of top sharpe same-datafield alpha
    output = []
    num_dict = defaultdict(int)
    for rec in next_alpha_recs:
        if rec[6] != universe:
            continue
        exp = rec[1]
        field = exp.split(prefix)[-1].split(",")[0]
        sharpe = rec[2]
        if sharpe < 0:
            field = "-%s"%field
        if num_dict[field] < keep_num:
            num_dict[field] += 1
            decay = rec[-1]
            exp = rec[1]
            output.append([exp,decay])
    return output

def transform_expressions(alpha_pool):
    """
    处理 alpha_pool 列表，将表达式 exp 替换为 signed_power(2.71828, exp)。

    Args:
        alpha_pool (list): 包含 [expression, decay] 元组的列表。

    Returns:
        list: 处理后的列表，表达式已替换。
    """
    transformed_pool = []
    for expression, decay in alpha_pool:
        # 将表达式包装为 signed_power(2.71828, exp)
        transformed_expression = f"signed_power(2.71828, {expression})"
        transformed_pool.append([transformed_expression, decay])
    return transformed_pool

def get_group_second_order_factory(first_order, group_ops, region):
    second_order = []
    for fo in first_order:
        for group_op in group_ops:
            second_order += group_factory(group_op, fo, region)
    return second_order

def get_group_second_order_factory_for_multi_line(first_order, group_ops, region):
    second_order = []
    for fo in first_order:
        for group_op in group_ops:
            second_order += group_factory_for_multi_line(group_op, fo, region)
    return second_order

def get_group_second_order_factory_for_multi_line_by_groups(first_order, group_ops, groups, region):
    second_order = []
    for fo in first_order:
        for group_op in group_ops:
            second_order += group_factory_for_multi_line_by_groups(group_op, fo, region, groups)
    return second_order

def group_factory_for_multi_line_by_groups(op, field, region, groups):
    """
    根据操作符、字段、地区和分组生成 alpha 表达式列表。

    Args:
        op (str): 操作符，例如 'group_rank'。
        field (str): 字段或表达式（可以是多行字符串）。
        region (str): 地区（此处仅作为占位参数，未使用）。
        groups (list): 分组列表，作为入参传入。

    Returns:
        list: 包含生成的 alpha 表达式的列表。
    """
    output = []
    vectors = ["cap"]

    # 提取 field 的结果变量（最后一行）
    field_lines = field.strip().split('\n')
    field_result = field_lines[-1].strip()  # 最后一行，例如 "alpha_76_residual"
    field_body = '\n'.join(field_lines[:-1])  # 除最后一行外的所有行

    # 为每个 group 生成 alpha 表达式
    for group in groups:
        # 根据 op 类型生成 alpha 表达式
        if op.startswith("group_vector"):
            for vector in vectors:
                alpha_op = f"{op}({field_result},{vector},densify({group}))"
                alpha = f"{field_body}\n            alpha = {alpha_op};\n            alpha"
                output.append(alpha)
        elif op.startswith("group_percentage"):
            alpha_op = f"{op}({field_result},densify({group}),percentage=0.5)"
            alpha = f"{field_body}\n            alpha = {alpha_op};\n            alpha"
            output.append(alpha)
        else:
            alpha_op = f"{op}({field_result},densify({group}))"
            alpha = f"{field_body}\n            alpha = {alpha_op};\n            alpha"
            output.append(alpha)

    return output

def group_factory_for_multi_line(op, field, region):
    """
    根据操作符、字段和地区生成 alpha 表达式列表。

    Args:
        op (str): 操作符，例如 'group_rank'。
        field (str): 字段或表达式（可以是多行字符串）。
        region (str): 地区，用于选择分组。

    Returns:
        list: 包含生成的 alpha 表达式的列表。
    """
    output = []
    vectors = ["cap"] 
    
    chn_group_13 = ['pv13_h_min2_sector', 'pv13_di_6l', 'pv13_rcsed_6l', 'pv13_di_5l', 'pv13_di_4l', 
                        'pv13_di_3l', 'pv13_di_2l', 'pv13_di_1l', 'pv13_parent', 'pv13_level']
    
    
    chn_group_1 = ['sta1_top3000c30','sta1_top3000c20','sta1_top3000c10','sta1_top3000c2','sta1_top3000c5']
    
    chn_group_2 = ['sta2_top3000_fact4_c10','sta2_top2000_fact4_c50','sta2_top3000_fact3_c20']
    
    hkg_group_13 = ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 
                    'pv13_2_minvol_1m_sector', 'pv13_5_minvol_1m_sector', 'pv13_1l_scibr', 'pv13_3l_scibr',
                    'pv13_2l_scibr', 'pv13_4l_scibr', 'pv13_5l_scibr']
    
    hkg_group_1 = ['sta1_allc50','sta1_allc5','sta1_allxjp_513_c20','sta1_top2000xjp_513_c5']
    
    hkg_group_2 = ['sta2_all_xjp_513_all_fact4_c10','sta2_top2000_xjp_513_top2000_fact3_c10',
                   'sta2_allfactor_xjp_513_13','sta2_top2000_xjp_513_top2000_fact3_c20']
    
    twn_group_13 = ['pv13_2_minvol_1m_sector','pv13_20_minvol_1m_sector','pv13_10_minvol_1m_sector',
                    'pv13_5_minvol_1m_sector','pv13_10_f3_g2_minvol_1m_sector','pv13_5_f3_g2_minvol_1m_sector',
                    'pv13_2_f4_g3_minvol_1m_sector']
    
    twn_group_1 = ['sta1_allc50','sta1_allxjp_513_c50','sta1_allxjp_513_c20','sta1_allxjp_513_c2',
                   'sta1_allc20','sta1_allxjp_513_c5','sta1_allxjp_513_c10','sta1_allc2','sta1_allc5']
    
    twn_group_2 = ['sta2_allfactor_xjp_513_0','sta2_all_xjp_513_all_fact3_c20',
                   'sta2_all_xjp_513_all_fact4_c20','sta2_all_xjp_513_all_fact4_c50']
    
    usa_group_13 = ['pv13_h_min2_3000_sector','pv13_r2_min20_3000_sector','pv13_r2_min2_3000_sector',
                    'pv13_r2_min2_3000_sector', 'pv13_h_min2_focused_pureplay_3000_sector']
    
    usa_group_1 = ['sta1_top3000c50','sta1_allc20','sta1_allc10','sta1_top3000c20','sta1_allc5']
    
    usa_group_2 = ['sta2_top3000_fact3_c50','sta2_top3000_fact4_c20','sta2_top3000_fact4_c10']
    
    usa_group_6 = ['mdl10_group_name']
    
    asi_group_13 = ['pv13_20_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector', 'pv13_10_f3_g2_minvol_1m_sector',
                    'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_5_minvol_1m_sector']
    
    asi_group_1 = ['sta1_allc50', 'sta1_allc10', 'sta1_minvol1mc50','sta1_minvol1mc20',
                   'sta1_minvol1m_normc20', 'sta1_minvol1m_normc50']
    
    jpn_group_1 = ['sta1_alljpn_513_c5', 'sta1_alljpn_513_c50', 'sta1_alljpn_513_c2', 'sta1_alljpn_513_c20']
    
    jpn_group_2 = ['sta2_top2000_jpn_513_top2000_fact3_c20', 'sta2_all_jpn_513_all_fact1_c5',
                   'sta2_allfactor_jpn_513_9', 'sta2_all_jpn_513_all_fact1_c10']
    
    jpn_group_13 = ['pv13_2_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector',
                    'pv13_10_f3_g2_minvol_1m_sector', 'pv13_all_delay_1_parent', 'pv13_all_delay_1_level']
    
    kor_group_13 = ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_5_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector',
                    'pv13_2_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector']
    
    kor_group_1 = ['sta1_allc20','sta1_allc50','sta1_allc2','sta1_allc10','sta1_minvol1mc50',
                   'sta1_allxjp_513_c10', 'sta1_top2000xjp_513_c50']
    
    kor_group_2 =['sta2_all_xjp_513_all_fact1_c50','sta2_top2000_xjp_513_top2000_fact2_c50',
                  'sta2_all_xjp_513_all_fact4_c50','sta2_all_xjp_513_all_fact4_c5']
    
    eur_group_13 = ['pv13_5_sector', 'pv13_2_sector', 'pv13_v3_3l_scibr', 'pv13_v3_2l_scibr', 'pv13_2l_scibr',
                    'pv13_52_sector', 'pv13_v3_6l_scibr', 'pv13_v3_4l_scibr', 'pv13_v3_1l_scibr']
    
    eur_group_1 = ['sta1_allc10', 'sta1_allc2', 'sta1_top1200c2', 'sta1_allc20', 'sta1_top1200c10']
    
    eur_group_2 = ['sta2_top1200_fact3_c50','sta2_top1200_fact3_c20','sta2_top1200_fact4_c50']
    
    glb_group_1 = ['sta1_allc20', 'sta1_allc10', 'sta1_allc50', 'sta1_allc5']

    glb_group_17 = ['sta3_pvgroup2_sector', 'sta3_pvgroup3_sector']
    
    
    glb_group_13 = ['pv13_2_sector', 'pv13_10_sector', 'pv13_3l_scibr', 'pv13_2l_scibr', 'pv13_1l_scibr',
                    'pv13_52_minvol_1m_all_delay_1_sector','pv13_52_minvol_1m_sector'] 
    
    amr_group_13 = ['pv13_4l_scibr', 'pv13_1l_scibr', 'pv13_hierarchy_min51_f1_sector',
                    'pv13_hierarchy_min2_600_sector', 'pv13_r2_min2_sector', 'pv13_h_min20_600_sector']
    
    #bps_group = "bucket(rank(fnd28_value_05480), range='0.1, 1, 0.1')"
    #pb_group = "bucket(rank(close/fnd28_value_05480), range='0.1, 1, 0.1')"
    cap_group = "bucket(rank(cap), range='0.1, 1, 0.1')"
    asset_group = "bucket(rank(assets),range='0.1, 1, 0.1')"
    sector_cap_group = "bucket(group_rank(cap, sector),range='0.1, 1, 0.1')"
    sector_asset_group = "bucket(group_rank(assets, sector),range='0.1, 1, 0.1')"

    vol_group = "bucket(rank(ts_std_dev(returns,20)),range = '0.1, 1, 0.1')"

    liquidity_group = "bucket(rank(close*volume),range = '0.1, 1, 0.1')"

    groups = ["market","sector", "industry", "subindustry",
            cap_group, asset_group, sector_cap_group, sector_asset_group, vol_group, liquidity_group]
    
    glb_groups = ["market","sector", "industry", "subindustry",
            cap_group, sector_cap_group, vol_group, liquidity_group]
    
    country_group = ["country"]

    if region == "CHN":
        groups += chn_group_13 + chn_group_1 + chn_group_2  
    if region == "TWN":
        groups += twn_group_13 + twn_group_1 + twn_group_2 
    if region == "ASI":
        groups += asi_group_13 + asi_group_1 + country_group
    if region == "USA":
        groups += usa_group_13 + usa_group_1 + usa_group_2  
    if region == "HKG":
        groups += hkg_group_13 + hkg_group_1 + hkg_group_2 
    if region == "KOR":
        groups += kor_group_13 + kor_group_1 + kor_group_2 
    if region == "EUR": 
        groups += eur_group_13 + eur_group_1 + eur_group_2 + country_group
    if region == "GLB":
        groups = glb_groups + glb_group_13 + glb_group_1 + glb_group_17 + country_group
    if region == "AMR":
        groups += amr_group_13 
    if region == "JPN":
        groups += jpn_group_1 + jpn_group_2 + jpn_group_13 

    # 提取 field 的结果变量（最后一行）
    field_lines = field.strip().split('\n')
    field_result = field_lines[-1].strip()  # 最后一行，例如 "alpha_76_residual"
    field_body = '\n'.join(field_lines[:-1])  # 除最后一行外的所有行

    # 为每个 group 生成 alpha 表达式
    for group in groups:
        # 根据 op 类型生成 alpha 表达式
        if op.startswith("group_vector"):
            for vector in vectors:
                alpha_op = f"{op}({field_result},{vector},densify({group}))"
                alpha = f"{field_body}\n            {alpha_op}"
                output.append(alpha)
        elif op.startswith("group_percentage"):
            alpha_op = f"{op}({field_result},densify({group}),percentage=0.5)"
            alpha = f"{field_body}\n            {alpha_op}"
            output.append(alpha)
        else:
            alpha_op = f"{op}({field_result},densify({group}))"
            alpha = f"{field_body}\n            {alpha_op}"
            output.append(alpha)

    return output


def group_factory(op, field, region):
    output = []
    vectors = ["cap"] 
    
    chn_group_13 = ['pv13_h_min2_sector', 'pv13_di_6l', 'pv13_rcsed_6l', 'pv13_di_5l', 'pv13_di_4l', 
                        'pv13_di_3l', 'pv13_di_2l', 'pv13_di_1l', 'pv13_parent', 'pv13_level']
    
    
    chn_group_1 = ['sta1_top3000c30','sta1_top3000c20','sta1_top3000c10','sta1_top3000c2','sta1_top3000c5']
    
    chn_group_2 = ['sta2_top3000_fact4_c10','sta2_top2000_fact4_c50','sta2_top3000_fact3_c20']
    
    hkg_group_13 = ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 
                    'pv13_2_minvol_1m_sector', 'pv13_5_minvol_1m_sector', 'pv13_1l_scibr', 'pv13_3l_scibr',
                    'pv13_2l_scibr', 'pv13_4l_scibr', 'pv13_5l_scibr']
    
    hkg_group_1 = ['sta1_allc50','sta1_allc5','sta1_allxjp_513_c20','sta1_top2000xjp_513_c5']
    
    hkg_group_2 = ['sta2_all_xjp_513_all_fact4_c10','sta2_top2000_xjp_513_top2000_fact3_c10',
                   'sta2_allfactor_xjp_513_13','sta2_top2000_xjp_513_top2000_fact3_c20']
    
    twn_group_13 = ['pv13_2_minvol_1m_sector','pv13_20_minvol_1m_sector','pv13_10_minvol_1m_sector',
                    'pv13_5_minvol_1m_sector','pv13_10_f3_g2_minvol_1m_sector','pv13_5_f3_g2_minvol_1m_sector',
                    'pv13_2_f4_g3_minvol_1m_sector']
    
    twn_group_1 = ['sta1_allc50','sta1_allxjp_513_c50','sta1_allxjp_513_c20','sta1_allxjp_513_c2',
                   'sta1_allc20','sta1_allxjp_513_c5','sta1_allxjp_513_c10','sta1_allc2','sta1_allc5']
    
    twn_group_2 = ['sta2_allfactor_xjp_513_0','sta2_all_xjp_513_all_fact3_c20',
                   'sta2_all_xjp_513_all_fact4_c20','sta2_all_xjp_513_all_fact4_c50']
    
    usa_group_13 = ['pv13_h_min2_3000_sector','pv13_r2_min20_3000_sector','pv13_r2_min2_3000_sector',
                    'pv13_r2_min2_3000_sector', 'pv13_h_min2_focused_pureplay_3000_sector']
    
    usa_group_1 = ['sta1_top3000c50','sta1_allc20','sta1_allc10','sta1_top3000c20','sta1_allc5']
    
    usa_group_2 = ['sta2_top3000_fact3_c50','sta2_top3000_fact4_c20','sta2_top3000_fact4_c10']
    
    usa_group_6 = ['mdl10_group_name']
    
    asi_group_13 = ['pv13_20_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector', 'pv13_10_f3_g2_minvol_1m_sector',
                    'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_5_minvol_1m_sector']
    
    asi_group_1 = ['sta1_allc50', 'sta1_allc10', 'sta1_minvol1mc50','sta1_minvol1mc20',
                   'sta1_minvol1m_normc20', 'sta1_minvol1m_normc50']
    
    jpn_group_1 = ['sta1_alljpn_513_c5', 'sta1_alljpn_513_c50', 'sta1_alljpn_513_c2', 'sta1_alljpn_513_c20']
    
    jpn_group_2 = ['sta2_top2000_jpn_513_top2000_fact3_c20', 'sta2_all_jpn_513_all_fact1_c5',
                   'sta2_allfactor_jpn_513_9', 'sta2_all_jpn_513_all_fact1_c10']
    
    jpn_group_13 = ['pv13_2_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector',
                    'pv13_10_f3_g2_minvol_1m_sector', 'pv13_all_delay_1_parent', 'pv13_all_delay_1_level']
    
    kor_group_13 = ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_5_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector',
                    'pv13_2_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector']
    
    kor_group_1 = ['sta1_allc20','sta1_allc50','sta1_allc2','sta1_allc10','sta1_minvol1mc50',
                   'sta1_allxjp_513_c10', 'sta1_top2000xjp_513_c50']
    
    kor_group_2 =['sta2_all_xjp_513_all_fact1_c50','sta2_top2000_xjp_513_top2000_fact2_c50',
                  'sta2_all_xjp_513_all_fact4_c50','sta2_all_xjp_513_all_fact4_c5']
    
    eur_group_13 = ['pv13_5_sector', 'pv13_2_sector', 'pv13_v3_3l_scibr', 'pv13_v3_2l_scibr', 'pv13_2l_scibr',
                    'pv13_52_sector', 'pv13_v3_6l_scibr', 'pv13_v3_4l_scibr', 'pv13_v3_1l_scibr']
    
    eur_group_1 = ['sta1_allc10', 'sta1_allc2', 'sta1_top1200c2', 'sta1_allc20', 'sta1_top1200c10']
    
    eur_group_2 = ['sta2_top1200_fact3_c50','sta2_top1200_fact3_c20','sta2_top1200_fact4_c50']
    
    glb_group_1 = ['sta1_allc20', 'sta1_allc10', 'sta1_allc50', 'sta1_allc5']

    glb_group_17 = ['sta3_pvgroup2_sector', 'sta3_pvgroup3_sector']
    
    
    glb_group_13 = ['pv13_2_sector', 'pv13_10_sector', 'pv13_3l_scibr', 'pv13_2l_scibr', 'pv13_1l_scibr',
                    'pv13_52_minvol_1m_all_delay_1_sector','pv13_52_minvol_1m_sector'] 
    
    amr_group_13 = ['pv13_4l_scibr', 'pv13_1l_scibr', 'pv13_hierarchy_min51_f1_sector',
                    'pv13_hierarchy_min2_600_sector', 'pv13_r2_min2_sector', 'pv13_h_min20_600_sector']
    
    #bps_group = "bucket(rank(fnd28_value_05480), range='0.1, 1, 0.1')"
    #pb_group = "bucket(rank(close/fnd28_value_05480), range='0.1, 1, 0.1')"
    cap_group = "bucket(rank(cap), range='0.1, 1, 0.1')"
    asset_group = "bucket(rank(assets),range='0.1, 1, 0.1')"
    sector_cap_group = "bucket(group_rank(cap, sector),range='0.1, 1, 0.1')"
    sector_asset_group = "bucket(group_rank(assets, sector),range='0.1, 1, 0.1')"

    vol_group = "bucket(rank(ts_std_dev(returns,20)),range = '0.1, 1, 0.1')"

    liquidity_group = "bucket(rank(close*volume),range = '0.1, 1, 0.1')"

    groups = ["market","sector", "industry", "subindustry",
            cap_group, asset_group, sector_cap_group, sector_asset_group, vol_group, liquidity_group]
    
    glb_groups = ["market","sector", "industry", "subindustry",
            cap_group, sector_cap_group, vol_group, liquidity_group]
    
    country_group = ["country"]

    if region == "CHN":
        groups += chn_group_13 + chn_group_1 + chn_group_2  
    if region == "TWN":
        groups += twn_group_13 + twn_group_1 + twn_group_2 
    if region == "ASI":
        groups += asi_group_13 + asi_group_1 + country_group
    if region == "USA":
        groups += usa_group_13 + usa_group_1 + usa_group_2  
    if region == "HKG":
        groups += hkg_group_13 + hkg_group_1 + hkg_group_2 
    if region == "KOR":
        groups += kor_group_13 + kor_group_1 + kor_group_2 
    if region == "EUR": 
        groups += eur_group_13 + eur_group_1 + eur_group_2 + country_group
    if region == "GLB":
        groups = glb_groups + glb_group_13 + glb_group_1 + glb_group_17 + country_group
    if region == "AMR":
        groups += amr_group_13 
    if region == "JPN":
        groups += jpn_group_1 + jpn_group_2 + jpn_group_13 
        
    for group in groups:
        if op.startswith("group_vector"):
            for vector in vectors:
                alpha = "%s(%s,%s,densify(%s))"%(op, field, vector, group)
                output.append(alpha)
        elif op.startswith("group_percentage"):
            alpha = "%s(%s,densify(%s),percentage=0.5)"%(op, field, group)
            output.append(alpha)
        else:
            alpha = "%s(%s,densify(%s))"%(op, field, group)
            output.append(alpha)
        
    return output

def get_group_by_region(region):
    
    chn_group_13 = ['pv13_h_min2_sector', 'pv13_di_6l', 'pv13_rcsed_6l', 'pv13_di_5l', 'pv13_di_4l', 
                        'pv13_di_3l', 'pv13_di_2l', 'pv13_di_1l', 'pv13_parent', 'pv13_level']
    
    
    chn_group_1 = ['sta1_top3000c30','sta1_top3000c20','sta1_top3000c10','sta1_top3000c2','sta1_top3000c5']
    
    chn_group_2 = ['sta2_top3000_fact4_c10','sta2_top2000_fact4_c50','sta2_top3000_fact3_c20']
    
    hkg_group_13 = ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 
                    'pv13_2_minvol_1m_sector', 'pv13_5_minvol_1m_sector', 'pv13_1l_scibr', 'pv13_3l_scibr',
                    'pv13_2l_scibr', 'pv13_4l_scibr', 'pv13_5l_scibr']
    
    hkg_group_1 = ['sta1_allc50','sta1_allc5','sta1_allxjp_513_c20','sta1_top2000xjp_513_c5']
    
    hkg_group_2 = ['sta2_all_xjp_513_all_fact4_c10','sta2_top2000_xjp_513_top2000_fact3_c10',
                   'sta2_allfactor_xjp_513_13','sta2_top2000_xjp_513_top2000_fact3_c20']
    
    twn_group_13 = ['pv13_2_minvol_1m_sector','pv13_20_minvol_1m_sector','pv13_10_minvol_1m_sector',
                    'pv13_5_minvol_1m_sector','pv13_10_f3_g2_minvol_1m_sector','pv13_5_f3_g2_minvol_1m_sector',
                    'pv13_2_f4_g3_minvol_1m_sector']
    
    twn_group_1 = ['sta1_allc50','sta1_allxjp_513_c50','sta1_allxjp_513_c20','sta1_allxjp_513_c2',
                   'sta1_allc20','sta1_allxjp_513_c5','sta1_allxjp_513_c10','sta1_allc2','sta1_allc5']
    
    twn_group_2 = ['sta2_allfactor_xjp_513_0','sta2_all_xjp_513_all_fact3_c20',
                   'sta2_all_xjp_513_all_fact4_c20','sta2_all_xjp_513_all_fact4_c50']
    
    usa_group_13 = ['pv13_h_min2_3000_sector','pv13_r2_min20_3000_sector','pv13_r2_min2_3000_sector',
                    'pv13_r2_min2_3000_sector', 'pv13_h_min2_focused_pureplay_3000_sector']
    
    usa_group_1 = ['sta1_top3000c50','sta1_allc20','sta1_allc10','sta1_top3000c20','sta1_allc5']
    
    usa_group_2 = ['sta2_top3000_fact3_c50','sta2_top3000_fact4_c20','sta2_top3000_fact4_c10']
    
    usa_group_6 = ['mdl10_group_name']
    
    asi_group_13 = ['pv13_20_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector', 'pv13_10_f3_g2_minvol_1m_sector',
                    'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_5_minvol_1m_sector']
    
    asi_group_1 = ['sta1_allc50', 'sta1_allc10', 'sta1_minvol1mc50','sta1_minvol1mc20',
                   'sta1_minvol1m_normc20', 'sta1_minvol1m_normc50']
    
    jpn_group_1 = ['sta1_alljpn_513_c5', 'sta1_alljpn_513_c50', 'sta1_alljpn_513_c2', 'sta1_alljpn_513_c20']
    
    jpn_group_2 = ['sta2_top2000_jpn_513_top2000_fact3_c20', 'sta2_all_jpn_513_all_fact1_c5',
                   'sta2_allfactor_jpn_513_9', 'sta2_all_jpn_513_all_fact1_c10']
    
    jpn_group_13 = ['pv13_2_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector',
                    'pv13_10_f3_g2_minvol_1m_sector', 'pv13_all_delay_1_parent', 'pv13_all_delay_1_level']
    
    kor_group_13 = ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_5_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector',
                    'pv13_2_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector']
    
    kor_group_1 = ['sta1_allc20','sta1_allc50','sta1_allc2','sta1_allc10','sta1_minvol1mc50',
                   'sta1_allxjp_513_c10', 'sta1_top2000xjp_513_c50']
    
    kor_group_2 =['sta2_all_xjp_513_all_fact1_c50','sta2_top2000_xjp_513_top2000_fact2_c50',
                  'sta2_all_xjp_513_all_fact4_c50','sta2_all_xjp_513_all_fact4_c5']
    
    eur_group_13 = ['pv13_5_sector', 'pv13_2_sector', 'pv13_v3_3l_scibr', 'pv13_v3_2l_scibr', 'pv13_2l_scibr',
                    'pv13_52_sector', 'pv13_v3_6l_scibr', 'pv13_v3_4l_scibr', 'pv13_v3_1l_scibr']
    
    eur_group_1 = ['sta1_allc10', 'sta1_allc2', 'sta1_top1200c2', 'sta1_allc20', 'sta1_top1200c10']
    
    eur_group_2 = ['sta2_top1200_fact3_c50','sta2_top1200_fact3_c20','sta2_top1200_fact4_c50']
    
    glb_group_1 = ['sta1_allc20', 'sta1_allc10', 'sta1_allc50', 'sta1_allc5']

    glb_group_17 = ['sta3_pvgroup2_sector', 'sta3_pvgroup3_sector']
    
    
    glb_group_13 = ['pv13_2_sector', 'pv13_10_sector', 'pv13_3l_scibr', 'pv13_2l_scibr', 'pv13_1l_scibr',
                    'pv13_52_minvol_1m_all_delay_1_sector','pv13_52_minvol_1m_sector'] 
    amr_group_13 = ['pv13_4l_scibr', 'pv13_1l_scibr', 'pv13_hierarchy_min51_f1_sector',
                    'pv13_hierarchy_min2_600_sector', 'pv13_r2_min2_sector', 'pv13_h_min20_600_sector']
    
    #bps_group = "bucket(rank(fnd28_value_05480), range='0.1, 1, 0.1')"
    #pb_group = "bucket(rank(close/fnd28_value_05480), range='0.1, 1, 0.1')"
    cap_group = "bucket(rank(cap), range='0.1, 1, 0.1')"
    asset_group = "bucket(rank(assets),range='0.1, 1, 0.1')"
    sector_cap_group = "bucket(group_rank(cap, sector),range='0.1, 1, 0.1')"
    sector_asset_group = "bucket(group_rank(assets, sector),range='0.1, 1, 0.1')"

    vol_group = "bucket(rank(ts_std_dev(returns,20)),range = '0.1, 1, 0.1')"

    liquidity_group = "bucket(rank(close*volume),range = '0.1, 1, 0.1')"

    groups = ["market","sector", "industry", "subindustry",
            cap_group, asset_group, sector_cap_group, sector_asset_group, vol_group, liquidity_group]
    
    glb_groups = ["market","sector", "industry", "subindustry",
            cap_group, sector_cap_group, vol_group, liquidity_group]
    
    if region == "CHN":
        groups += chn_group_1
    if region == "TWN":
        groups += twn_group_13 + twn_group_1 + twn_group_2 
    if region == "ASI":
        groups += asi_group_13 + asi_group_1 
    if region == "USA":
        groups += usa_group_13 + usa_group_1 + usa_group_2  
    if region == "HKG":
        groups += hkg_group_13 + hkg_group_1 + hkg_group_2 
    if region == "KOR":
        groups += kor_group_13 + kor_group_1 + kor_group_2 
    if region == "EUR": 
        groups += eur_group_13 + eur_group_1 + eur_group_2 
    if region == "GLB":
        groups = glb_groups + glb_group_13 + glb_group_1 + glb_group_17
    if region == "AMR":
        groups += amr_group_13 
    if region == "JPN":
        groups += jpn_group_1 + jpn_group_2 + jpn_group_13
        
    return groups

def trade_when_factory_for_multi_line(op, field, region):
    """
    根据操作符、字段和地区生成 trade_alpha 表达式列表。

    Args:
        op (str): 操作符，例如 'trade_when'。
        field (str): 字段或表达式（可以是多行字符串）。
        region (str): 地区，用于选择事件。

    Returns:
        list: 包含生成的 trade_alpha 表达式的列表。
    """
    # 提取 field 的结果变量（最后一行）
    field_lines = field.strip().split('\n')
    field_result = field_lines[-1].strip()  # 最后一行，例如 "alpha_76_residual"
    field_body = '\n'.join(field_lines[:-1])  # 除最后一行外的所有行

    output = []

    # 定义 open_events，使用提取出的 field_result 替换占位符
    open_events = [
        "ts_arg_max(volume, 5) == 0",
        "ts_corr(close, volume, 20) < 0",
        "ts_corr(close, volume, 5) < 0",
        "ts_mean(volume,10)>ts_mean(volume,60)",
        "group_rank(ts_std_dev(returns,60), sector) > 0.7",
        "ts_zscore(returns,60) > 2",
        "ts_arg_min(volume, 5) > 3",
        "ts_std_dev(returns, 5) > ts_std_dev(returns, 20)",
        "ts_arg_max(close, 5) == 0",
        "ts_arg_max(close, 20) == 0",
        "ts_corr(close, volume, 5) > 0",
        "ts_corr(close, volume, 5) > 0.3",
        "ts_corr(close, volume, 5) > 0.5",
        "ts_corr(close, volume, 20) > 0",
        "ts_corr(close, volume, 20) > 0.3",
        "ts_corr(close, volume, 20) > 0.5",
        f"ts_regression(returns, {field_result}, 5, lag = 0, rettype = 2) > 0",
        f"ts_regression(returns, {field_result}, 20, lag = 0, rettype = 2) > 0",
        "ts_regression(returns, ts_step(20), 20, lag = 0, rettype = 2) > 0",
        "ts_regression(returns, ts_step(5), 5, lag = 0, rettype = 2) > 0"
    ]

    exit_events = ["abs(returns) > 0.1", "-1"]

    usa_events = [
        "rank(rp_css_business) > 0.8",
        "ts_rank(rp_css_business, 22) > 0.8",
        "rank(vec_avg(mws82_sentiment)) > 0.8",
        "ts_rank(vec_avg(mws82_sentiment),22) > 0.8",
        "rank(vec_avg(nws48_ssc)) > 0.8",
        "ts_rank(vec_avg(nws48_ssc),22) > 0.8",
        "rank(vec_avg(mws50_ssc)) > 0.8",
        "ts_rank(vec_avg(mws50_ssc),22) > 0.8",
        "ts_rank(vec_sum(scl12_alltype_buzzvec),22) > 0.9",
        "pcr_oi_270 < 1",
        "pcr_oi_270 > 1"
    ]

    asi_events = [
        "rank(vec_avg(mws38_score)) > 0.8",
        "ts_rank(vec_avg(mws38_score),22) > 0.8"
    ]

    eur_events = [
        "rank(rp_css_business) > 0.8",
        "ts_rank(rp_css_business, 22) > 0.8",
        "rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos)) > 0.8",
        "ts_rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos),22) > 0.8",
        "rank(vec_avg(mws84_sentiment)) > 0.8",
        "ts_rank(vec_avg(mws84_sentiment),22) > 0.8",
        "rank(vec_avg(mws85_sentiment)) > 0.8",
        "ts_rank(vec_avg(mws85_sentiment),22) > 0.8",
        "rank(mdl110_analyst_sentiment) > 0.8",
        "ts_rank(mdl110_analyst_sentiment, 22) > 0.8",
        "rank(vec_avg(nws3_scores_posnormscr)) > 0.8",
        "ts_rank(vec_avg(nws3_scores_posnormscr),22) > 0.8",
        "rank(vec_avg(mws36_sentiment_words_positive)) > 0.8",
        "ts_rank(vec_avg(mws36_sentiment_words_positive),22) > 0.8"
    ]

    glb_events = [
        "rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
        "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
        "rank(vec_avg(nws20_ssc)) > 0.8",
        "ts_rank(vec_avg(nws20_ssc),22) > 0.8",
        "vec_avg(nws20_ssc) > 0",
        "rank(vec_avg(nws20_bee)) > 0.8",
        "ts_rank(vec_avg(nws20_bee),22) > 0.8",
        "rank(vec_avg(nws20_qmb)) > 0.8",
        "ts_rank(vec_avg(nws20_qmb),22) > 0.8"
    ]

    chn_events = [
        "rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform)) > 0.8",
        "ts_rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform),22) > 0.8",
        "rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform)) > 0.8",
        "ts_rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform),22) > 0.8",
        "rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform)) > 0.8",
        "ts_rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform),22) > 0.8"
    ]

    kor_events = [
        "rank(vec_avg(mdl110_analyst_sentiment)) > 0.8",
        "ts_rank(vec_avg(mdl110_analyst_sentiment),22) > 0.8",
        "rank(vec_avg(mws38_score)) > 0.8",
        "ts_rank(vec_avg(mws38_score),22) > 0.8"
    ]

    twn_events = [
        "rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
        "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
        "rank(rp_ess_business) > 0.8",
        "ts_rank(rp_ess_business,22) > 0.8"
    ]

    # 根据 region 添加特定事件
    if region == "USA":
        open_events += usa_events
    if region == "ASI":
        open_events += asi_events
    if region == "EUR":
        open_events += eur_events
    if region == "GLB":
        open_events += glb_events
    if region == "CHN":
        open_events += chn_events
    if region == "KOR":
        open_events += kor_events
    if region == "TWN":
        open_events += twn_events

    # 为每个 open_event 和 exit_event 组合生成 trade_alpha 表达式
    for oe in open_events:
        for ee in exit_events:
            trade_alpha_op = f"{op}({oe}, {field_result}, {ee})"
            trade_alpha = f"{field_body}\n   {trade_alpha_op}"
            output.append(trade_alpha)

    return output


def trade_when_factory(op,field,region):
    output = []
    open_events = ["ts_arg_max(volume, 5) == 0", "ts_corr(close, volume, 20) < 0",
                   "ts_corr(close, volume, 5) < 0", "ts_mean(volume,10)>ts_mean(volume,60)",
                   "group_rank(ts_std_dev(returns,60), sector) > 0.7", "ts_zscore(returns,60) > 2",
                   "ts_arg_min(volume, 5) > 3",
                   "ts_std_dev(returns, 5) > ts_std_dev(returns, 20)",
                   "ts_arg_max(close, 5) == 0", "ts_arg_max(close, 20) == 0",
                   "ts_corr(close, volume, 5) > 0", "ts_corr(close, volume, 5) > 0.3", "ts_corr(close, volume, 5) > 0.5",
                   "ts_corr(close, volume, 20) > 0", "ts_corr(close, volume, 20) > 0.3", "ts_corr(close, volume, 20) > 0.5",
                   "ts_regression(returns, %s, 5, lag = 0, rettype = 2) > 0"%field,
                   "ts_regression(returns, %s, 20, lag = 0, rettype = 2) > 0"%field,
                   "ts_regression(returns, ts_step(20), 20, lag = 0, rettype = 2) > 0",
                   "ts_regression(returns, ts_step(5), 5, lag = 0, rettype = 2) > 0"]

    exit_events = ["abs(returns) > 0.1", "-1"]

    usa_events = ["rank(rp_css_business) > 0.8", "ts_rank(rp_css_business, 22) > 0.8", "rank(vec_avg(mws82_sentiment)) > 0.8",
                  "ts_rank(vec_avg(mws82_sentiment),22) > 0.8", "rank(vec_avg(nws48_ssc)) > 0.8",
                  "ts_rank(vec_avg(nws48_ssc),22) > 0.8", "rank(vec_avg(mws50_ssc)) > 0.8", "ts_rank(vec_avg(mws50_ssc),22) > 0.8",
                  "ts_rank(vec_sum(scl12_alltype_buzzvec),22) > 0.9", "pcr_oi_270 < 1", "pcr_oi_270 > 1",]

    asi_events = ["rank(vec_avg(mws38_score)) > 0.8", "ts_rank(vec_avg(mws38_score),22) > 0.8"]

    eur_events = ["rank(rp_css_business) > 0.8", "ts_rank(rp_css_business, 22) > 0.8",
                  "rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos)) > 0.8",
                  "ts_rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos),22) > 0.8",
                  "rank(vec_avg(mws84_sentiment)) > 0.8", "ts_rank(vec_avg(mws84_sentiment),22) > 0.8",
                  "rank(vec_avg(mws85_sentiment)) > 0.8", "ts_rank(vec_avg(mws85_sentiment),22) > 0.8",
                  "rank(mdl110_analyst_sentiment) > 0.8", "ts_rank(mdl110_analyst_sentiment, 22) > 0.8",
                  "rank(vec_avg(nws3_scores_posnormscr)) > 0.8",
                  "ts_rank(vec_avg(nws3_scores_posnormscr),22) > 0.8",
                  "rank(vec_avg(mws36_sentiment_words_positive)) > 0.8",
                  "ts_rank(vec_avg(mws36_sentiment_words_positive),22) > 0.8"]

    glb_events = ["rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
                  "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
                  "rank(vec_avg(nws20_ssc)) > 0.8",
                  "ts_rank(vec_avg(nws20_ssc),22) > 0.8",
                  "vec_avg(nws20_ssc) > 0",
                  "rank(vec_avg(nws20_bee)) > 0.8",
                  "ts_rank(vec_avg(nws20_bee),22) > 0.8",
                  "rank(vec_avg(nws20_qmb)) > 0.8",
                  "ts_rank(vec_avg(nws20_qmb),22) > 0.8"]

    chn_events = ["rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform),22) > 0.8",
                  "rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform),22) > 0.8",
                  "rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform),22) > 0.8"]

    kor_events = ["rank(vec_avg(mdl110_analyst_sentiment)) > 0.8",
                  "ts_rank(vec_avg(mdl110_analyst_sentiment),22) > 0.8",
                  "rank(vec_avg(mws38_score)) > 0.8",
                  "ts_rank(vec_avg(mws38_score),22) > 0.8"]

    twn_events = ["rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
                  "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
                  "rank(rp_ess_business) > 0.8",
                  "ts_rank(rp_ess_business,22) > 0.8"]
    # 根据 region 添加特定事件
    if region == "USA":
        open_events += usa_events
    if region == "ASI":
        open_events += asi_events
    if region == "EUR":
        open_events += eur_events
    if region == "GLB":
        open_events += glb_events
    if region == "CHN":
        open_events += chn_events
    if region == "KOR":
        open_events += kor_events
    if region == "TWN":
        open_events += twn_events


    for oe in open_events:
        for ee in exit_events:
            alpha = "%s(%s, %s, %s)"%(op, oe, field, ee)
            output.append(alpha)
    return output


def check_submission(alpha_bag, gold_bag, start):
    depot = []
    s = login()
    for idx, g in enumerate(alpha_bag):
        if idx < start:
            continue
        if idx % 5 == 0:
            print(idx)
        if idx % 200 == 0:
            s = login()
        #print(idx)
        pc = get_check_submission(s, g)
        if pc == "sleep":
            sleep(100)
            s = login()
            alpha_bag.append(g)
        elif pc != pc:
            # pc is nan
            print("check self-corrlation error")
            sleep(100)
            alpha_bag.append(g)
        elif pc == "fail":
            continue
        elif pc == "error":
            depot.append(g)
        else:
            print(g)
            gold_bag.append((g, pc))
    print(depot)
    return gold_bag

def get_check_submission(s, alpha_id):
    while True:
        result = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id + "/check")
        if "retry-after" in result.headers:
            time.sleep(float(result.headers["Retry-After"]))
        else:
            break
    try:
        if result.json().get("is", 0) == 0:
            print("logged out")
            return "sleep"
        checks_df = pd.DataFrame(
                result.json()["is"]["checks"]
        )
        pc = checks_df[checks_df.name == "PROD_CORRELATION"]["value"].values[0]
        if not any(checks_df["result"] == "FAIL"):
            return pc
        else:
            return "fail"
    except:
        print("catch: %s"%(alpha_id))
        return "error"
    

def view_alphas(gold_bag):
    s = login()
    sharp_list = []
    for gold, pc in gold_bag:

        triple = locate_alpha(s, gold)
        info = [triple[0], triple[2], triple[3], triple[4], triple[5], triple[6], triple[1]]
        info.append(pc)
        sharp_list.append(info)

    sharp_list.sort(reverse=True, key = lambda x : x[1])
    for i in sharp_list:
        print(i)
 
def locate_alpha(s, alpha_id):
    while True:
        alpha = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id)
        if "retry-after" in alpha.headers:
            time.sleep(float(alpha.headers["Retry-After"]))
        else:
            break
    string = alpha.content.decode('utf-8')
    metrics = json.loads(string)
    #print(metrics["regular"]["code"])
    
    dateCreated = metrics["dateCreated"]
    sharpe = metrics["is"]["sharpe"]
    fitness = metrics["is"]["fitness"]
    turnover = metrics["is"]["turnover"]
    margin = metrics["is"]["margin"]
    decay = metrics["settings"]["decay"]
    exp = metrics['regular']['code']
    
    triple = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
    return triple



# some factory for other operators 
def vector_factory(op, field):
    output = []
    vectors = ["cap"]
    
    for vector in vectors:
    
        alpha = "%s(%s, %s)"%(op, field, vector)
        output.append(alpha)
    
    return output
 
 
def ts_comp_factory(op, field, factor, paras):
    output = []
    #l1, l2 = [3, 5, 10, 20, 60, 120, 240], paras
    l1, l2 = [5, 22, 66, 240], paras
    comb = list(product(l1, l2))
    
    for day,para in comb:
        
        if type(para) == float:
            alpha = "%s(%s, %d, %s=%.1f)"%(op, field, day, factor, para)
        elif type(para) == int:
            alpha = "%s(%s, %d, %s=%d)"%(op, field, day, factor, para)
            
        output.append(alpha)
    
    return output
 
def twin_field_factory(op, field, fields):
    
    output = []
    #days = [3, 5, 10, 20, 60, 120, 240]
    days = [5, 22, 66, 240]
    outset = list(set(fields) - set([field]))
    
    for day in days:
        for counterpart in outset:
            alpha = "%s(%s, %s, %d)"%(op, field, counterpart, day)
            output.append(alpha)
    
    return output
 
def login_hk():
    
    username = ""
    password = ""
    
    # Create a session to persistently store the headers
    s = requests.Session()
    
    # Save credentials into session
    s.auth = (username, password)
    
    # Send a POST request to the /authentication API
    response = s.post('https://api.worldquantbrain.com/authentication')
    
    if response.status_code == requests.codes.unauthorized:
        # Check if biometrics is required
        if response.headers.get("WWW-Authenticate") == "persona":
            print(
                "Complete biometrics authentication by scanning your face. Follow the link: \n"
                + urljoin(response.url, response.headers["Location"]) + "\n"
            )
            input("Press any key after you complete the biometrics authentication.")
            
            # Retry the authentication after biometrics
            biometrics_response = s.post(urljoin(response.url, response.headers["Location"]))
            
            while biometrics_response.status_code != 201:
                input("Biometrics authentication is not complete. Please try again and press any key when completed.")
                biometrics_response = s.post(urljoin(response.url, response.headers["Location"]))
                
            print("Biometrics authentication completed.")
        else:
            print("\nIncorrect username or password. Please check your credentials.\n")
    else:
        print("Logged in successfully.")
    
    return s 

def get_alphas_with_universe_region(start_date, end_date, sharpe_th, fitness_th, region, alpha_num, usage):
    s = login()
    output = []
    # 3E large 3C less
    count = 0
    for i in range(0, alpha_num, 100):
        print(i)
        url_e = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d"%(i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2025-" + start_date  \
                + "T00:00:00-04:00&dateCreated%3C2025-" + end_date \
                + "T00:00:00-04:00&is.fitness%3E" + str(fitness_th) + "&is.sharpe%3E" \
                + str(sharpe_th) + "&settings.region=" + region + "&order=-is.sharpe&hidden=false&type!=SUPER"
        url_c = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d"%(i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2025-" + start_date  \
                + "T00:00:00-04:00&dateCreated%3C2025-" + end_date \
                + "T00:00:00-04:00&is.fitness%3C-" + str(fitness_th) + "&is.sharpe%3C-" \
                + str(sharpe_th) + "&settings.region=" + region + "&order=is.sharpe&hidden=false&type!=SUPER"
        urls = [url_e]
        if usage != "submit":
            urls.append(url_c)
        for url in urls:
            response = s.get(url)
            #print(response.json())
            try:
                alpha_list = response.json()["results"]
                #print(response.json())
                for j in range(len(alpha_list)):
                    alpha_id = alpha_list[j]["id"]
                    name = alpha_list[j]["name"]
                    dateCreated = alpha_list[j]["dateCreated"]
                    sharpe = alpha_list[j]["is"]["sharpe"]
                    fitness = alpha_list[j]["is"]["fitness"]
                    turnover = alpha_list[j]["is"]["turnover"]
                    margin = alpha_list[j]["is"]["margin"]
                    longCount = alpha_list[j]["is"]["longCount"]
                    shortCount = alpha_list[j]["is"]["shortCount"]
                    decay = alpha_list[j]["settings"]["decay"]
                    exp = alpha_list[j]['regular']['code']
                    universe = alpha_list[j]['settings']['universe']
                    region = alpha_list[j]['settings']['region']
                    count += 1
                    #if (sharpe > 1.2 and sharpe < 1.6) or (sharpe < -1.2 and sharpe > -1.6):
                    if (longCount + shortCount) > 100:
                        if sharpe < -sharpe_th:
                            exp = "-%s"%exp
                        rec = [alpha_id, exp, sharpe, turnover, fitness, margin, universe, region, dateCreated, decay]
                        if turnover > 0.7:
                            rec.append(decay*4)
                        elif turnover > 0.6:
                            rec.append(decay*3+3)
                        elif turnover > 0.5:
                            rec.append(decay*3)
                        elif turnover > 0.4:
                            rec.append(decay*2)
                        elif turnover > 0.35:
                            rec.append(decay+4)
                        elif turnover > 0.3:
                            rec.append(decay+2)
                        output.append(rec)
                        print(rec)
            except:
                print("%d finished re-login"%i)
                s = login()

    print("count: %d"%count)
    return output
