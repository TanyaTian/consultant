import requests
import pandas as pd
import logging
import time
from typing import Optional, Tuple, Dict, List, Union
from concurrent.futures import ThreadPoolExecutor
import pickle
from collections import defaultdict
import numpy as np
from pathlib import Path
from dataclasses import dataclass

# 定义配置类
@dataclass
class Config:
    data_path: Path = Path("data")

cfg = Config()

def sign_in(username: str, password: str) -> Optional[requests.Session]:
    """
    与 WorldQuant Brain API 进行身份验证，创建并返回一个已验证的会话。
    """
    s = requests.Session()
    s.auth = (username, password)
    try:
        response = s.post('https://api.worldquantbrain.com/authentication')
        response.raise_for_status()
        logging.info("成功登录")
        return s
    except requests.exceptions.RequestException as e:
        logging.error(f"登录失败: {e}")
        return None

def save_obj(obj: object, name: str) -> None:
    """
    将 Python 对象序列化并保存到 pickle 文件中。
    """
    # 确保目录存在
    Path(name).parent.mkdir(parents=True, exist_ok=True)
    with open(name + '.pickle', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name: str) -> object:
    """
    从 pickle 文件中加载并反序列化 Python 对象。
    """
    with open(name + '.pickle', 'rb') as f:
        return pickle.load(f)

def wait_get(url: str, sess: requests.Session, max_retries: int = 10) -> requests.Response:
    """
    执行带有重试逻辑和限流处理的 GET 请求。
    """
    retries = 0
    while retries < max_retries:
        while True:
            simulation_progress = sess.get(url)
            retry_after = simulation_progress.headers.get("Retry-After", 0)
            if retry_after == 0:
                break
            time.sleep(float(retry_after))
        if simulation_progress.status_code < 400:
            break
        time.sleep(2 ** retries)
        retries += 1
    return simulation_progress

def _get_alpha_pnl(alpha_id: str, sess: requests.Session) -> pd.DataFrame:
    """
    从 WorldQuant Brain API 获取特定 alpha 的盈亏（PnL）数据。
    """
    pnl = wait_get(f"https://api.worldquantbrain.com/alphas/{alpha_id}/recordsets/pnl", sess).json()
    df = pd.DataFrame(pnl['records'], columns=[item['name'] for item in pnl['schema']['properties']])
    df = df.rename(columns={'date': 'Date', 'pnl': alpha_id})
    df = df[['Date', alpha_id]]
    return df

def get_alpha_pnls(
    alphas: List[Dict],
    sess: requests.Session,
    alpha_pnls: Optional[pd.DataFrame] = None,
    alpha_ids: Optional[Dict[str, List]] = None
) -> Tuple[Dict[str, List], pd.DataFrame]:
    """
    获取多个 alpha 的盈亏数据，并按区域对 alpha ID 进行分类。
    """
    if alpha_ids is None:
        alpha_ids = defaultdict(list)
    if alpha_pnls is None:
        alpha_pnls = pd.DataFrame()

    new_alphas = [item for item in alphas if item['id'] not in alpha_pnls.columns]
    if not new_alphas:
        return alpha_ids, alpha_pnls

    for item_alpha in new_alphas:
        alpha_ids[item_alpha['settings']['region']].append(item_alpha['id'])

    fetch_pnl_func = lambda alpha_id: _get_alpha_pnl(alpha_id, sess).set_index('Date')
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(fetch_pnl_func, [item['id'] for item in new_alphas])
    alpha_pnls = pd.concat([alpha_pnls] + list(results), axis=1)
    alpha_pnls.sort_index(inplace=True)
    return alpha_ids, alpha_pnls

def get_os_alphas(sess: requests.Session, limit: int = 100, get_first: bool = False) -> List[Dict]:
    """
    从 WorldQuant Brain API 获取处于 OS（开放提交）阶段的 alpha 列表。
    """
    fetched_alphas = []
    offset = 0
    total_alphas = 100
    while len(fetched_alphas) < total_alphas:
        print(f"从偏移 {offset} 到 {offset + limit} 获取 alpha")
        url = (f"https://api.worldquantbrain.com/users/self/alphas?"
               f"stage=OS&limit={limit}&offset={offset}&order=-dateSubmitted")
        res = wait_get(url, sess).json()
        if offset == 0:
            total_alphas = res['count']
        alphas = res["results"]
        fetched_alphas.extend(alphas)
        if len(alphas) < limit:
            break
        offset += limit
        if get_first:
            break
    return fetched_alphas[:total_alphas]

def calc_self_corr(
    alpha_id: str,
    sess: requests.Session,
    os_alpha_rets: Optional[pd.DataFrame] = None,
    os_alpha_ids: Optional[Dict[str, List]] = None,
    alpha_result: Optional[Dict] = None,
    return_alpha_pnls: bool = False,
    alpha_pnls: Optional[pd.DataFrame] = None
) -> Union[float, Tuple[float, pd.DataFrame]]:
    """
    计算目标 alpha 与同一区域内其他 alpha 的最大相关性。
    """
    if alpha_result is None:
        alpha_result = wait_get(f"https://api.worldquantbrain.com/alphas/{alpha_id}", sess).json()
    
    if alpha_pnls is not None and len(alpha_pnls) == 0:
        alpha_pnls = None
    
    if alpha_pnls is None:
        _, alpha_pnls = get_alpha_pnls([alpha_result], sess)
        alpha_pnls = alpha_pnls[alpha_id]

    alpha_rets = alpha_pnls - alpha_pnls.ffill().shift(1)
    alpha_rets = alpha_rets[pd.to_datetime(alpha_rets.index) > 
                           pd.to_datetime(alpha_rets.index).max() - pd.DateOffset(years=4)]

    region = alpha_result['settings']['region']
    if os_alpha_rets is None or os_alpha_ids is None or region not in os_alpha_ids or not os_alpha_ids[region]:
        logging.warning(f"无法计算相关性：缺少数据或区域 {region} 无 alpha")
        return (0.0, alpha_pnls) if return_alpha_pnls else 0.0

    corr_series = os_alpha_rets[os_alpha_ids[region]].corrwith(alpha_rets).sort_values(ascending=False).round(4)
    print(corr_series)
    # 确保保存目录存在
    cfg.data_path.mkdir(parents=True, exist_ok=True)
    corr_series.to_csv(str(cfg.data_path / 'os_alpha_corr.csv'))

    self_corr = corr_series.max()
    if np.isnan(self_corr):
        self_corr = 0.0

    return (self_corr, alpha_pnls) if return_alpha_pnls else self_corr

def download_data(sess: requests.Session, flag_increment: bool = True) -> None:
    """
    从 WorldQuant Brain API 下载 alpha 数据并保存到磁盘。
    """
    if flag_increment:
        try:
            os_alpha_ids = load_obj(str(cfg.data_path / 'os_alpha_ids'))
            os_alpha_pnls = load_obj(str(cfg.data_path / 'os_alpha_pnls'))
            ppac_alpha_ids = load_obj(str(cfg.data_path / 'ppac_alpha_ids'))
            exist_alpha = [alpha for ids in os_alpha_ids.values() for alpha in ids]
        except Exception as e:
            logging.error(f"无法加载现有数据: {e}")
            os_alpha_ids = None
            os_alpha_pnls = None
            exist_alpha = []
            ppac_alpha_ids = []
    else:
        os_alpha_ids = None
        os_alpha_pnls = None
        exist_alpha = []
        ppac_alpha_ids = []

    if os_alpha_ids is None:
        alphas = get_os_alphas(sess, limit=100, get_first=False)
    else:
        alphas = get_os_alphas(sess, limit=30, get_first=True)

    alphas = [item for item in alphas if item['id'] not in exist_alpha]
    ppac_alpha_ids += [item['id'] for item in alphas 
                      for item_match in item['classifications'] 
                      if item_match['name'] == 'Power Pool Alpha']

    os_alpha_ids, os_alpha_pnls = get_alpha_pnls(alphas, sess, alpha_pnls=os_alpha_pnls, alpha_ids=os_alpha_ids)
    save_obj(os_alpha_ids, str(cfg.data_path / 'os_alpha_ids'))
    save_obj(os_alpha_pnls, str(cfg.data_path / 'os_alpha_pnls'))
    save_obj(ppac_alpha_ids, str(cfg.data_path / 'ppac_alpha_ids'))
    print(f'新下载的 alpha 数量: {len(alphas)}, 总 alpha 数量: {os_alpha_pnls.shape[1]}')

def load_data(tag: Optional[str] = None) -> Tuple[Dict[str, List], pd.DataFrame]:
    """
    从磁盘加载 alpha 数据并根据指定标签进行过滤。
    """
    os_alpha_ids = load_obj(str(cfg.data_path / 'os_alpha_ids'))
    os_alpha_pnls = load_obj(str(cfg.data_path / 'os_alpha_pnls'))
    ppac_alpha_ids = load_obj(str(cfg.data_path / 'ppac_alpha_ids'))

    if tag == 'PPAC':
        for item in os_alpha_ids:
            os_alpha_ids[item] = [alpha for alpha in os_alpha_ids[item] if alpha in ppac_alpha_ids]
    elif tag == 'SelfCorr':
        for item in os_alpha_ids:
            os_alpha_ids[item] = [alpha for alpha in os_alpha_ids[item] if alpha not in ppac_alpha_ids]

    exist_alpha = [alpha for ids in os_alpha_ids.values() for alpha in ids]
    os_alpha_pnls = os_alpha_pnls[exist_alpha]
    os_alpha_rets = os_alpha_pnls - os_alpha_pnls.ffill().shift(1)
    os_alpha_rets = os_alpha_rets[pd.to_datetime(os_alpha_rets.index) > 
                                 pd.to_datetime(os_alpha_rets.index).max() - pd.DateOffset(years=4)]
    return os_alpha_ids, os_alpha_rets