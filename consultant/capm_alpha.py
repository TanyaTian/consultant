# 定义组合
target_list = [
    "returns",
    "(close - ts_delay(close, 1)) / ts_delay(close, 1)",
    "(vwap - ts_delay(vwap, 1)) / ts_delay(vwap, 1)",
    "high - low",
    "(volume - ts_delay(volume, 1)) / ts_delay(volume, 1)"
]
market_list = [
    "pv53_logreturns5_normalized",
    "pv53_beta_vix_normalized",
    "adv20"
]
sector_list = ["sector", "industry", "subindustry", "sta1_allc10", "sta2_all_fact1_c10"]

cap = "cap"

days = [252, 504, 756]

base_market_list = ['any']

base_target_list4 = [
    # 组合字段（需要计算）
    "close / dividend",                                # 市盈率
    "close / (cap / sharesout)"]

base_target_list = [
    # 组合字段（需要计算）
    "close / dividend",                                # 市盈率
    "close / (cap / sharesout)",           # 市净率
    "dividend / close",              # 股息收益率
    "returns",                         # 日收益率（直接用 returns）
    "(close - ts_delay(close, 1)) / ts_delay(close, 1)",  # 日收益率（计算版）
    "volume / cap",                 # 流动性比率
    "returns / parkinson_volatility_120",  # 波动调整收益率

    # 可选字段（直接使用）
    # 历史波动率 (option8)
    "historical_volatility_10",
    "historical_volatility_20",
    "historical_volatility_30",
    "historical_volatility_60",
    "historical_volatility_90",
    "historical_volatility_120",
    "historical_volatility_150",
    "historical_volatility_180",

    # 隐含波动率 - 看涨期权 (option8)
    "implied_volatility_call_10",
    "implied_volatility_call_20",
    "implied_volatility_call_30",
    "implied_volatility_call_60",
    "implied_volatility_call_90",
    "implied_volatility_call_120",
    "implied_volatility_call_150",
    "implied_volatility_call_180",
    "implied_volatility_call_270",
    "implied_volatility_call_360",
    "implied_volatility_call_720",
    "implied_volatility_call_1080",

    # 隐含波动率 - 看跌期权 (option8)
    "implied_volatility_put_10",
    "implied_volatility_put_20",
    "implied_volatility_put_30",
    "implied_volatility_put_60",
    "implied_volatility_put_90",
    "implied_volatility_put_120",
    "implied_volatility_put_150",
    "implied_volatility_put_180",
    "implied_volatility_put_270",
    "implied_volatility_put_360",
    "implied_volatility_put_720",
    "implied_volatility_put_1080",

    # Parkinson 波动率 (option8)
    "parkinson_volatility_10",
    "parkinson_volatility_20",
    "parkinson_volatility_30",
    "parkinson_volatility_60",
    "parkinson_volatility_90",
    "parkinson_volatility_120",
    "parkinson_volatility_150",
    "parkinson_volatility_180",

    # 看跌/看涨比率 - 成交量 (option9)
    "pcr_vol_10",
    "pcr_vol_20",
    "pcr_vol_30",
    "pcr_vol_60",
    "pcr_vol_90",
    "pcr_vol_120",
    "pcr_vol_150",
    "pcr_vol_180",
    "pcr_vol_270",
    "pcr_vol_360",
    "pcr_vol_720",
    "pcr_vol_1080",
    "pcr_vol_all",

    # 看跌/看涨比率 - 持仓量 (option9)
    "pcr_oi_10",
    "pcr_oi_20",
    "pcr_oi_30",
    "pcr_oi_60",
    "pcr_oi_90",
    "pcr_oi_120",
    "pcr_oi_150",
    "pcr_oi_180",
    "pcr_oi_270",
    "pcr_oi_360",
    "pcr_oi_720",
    "pcr_oi_1080",
    "pcr_oi_all",

    # 远期价格 (option9)
    "forward_price_10",
    "forward_price_20",
    "forward_price_30",
    "forward_price_60",
    "forward_price_90",
    "forward_price_120",
    "forward_price_150",
    "forward_price_180",
    "forward_price_270",
    "forward_price_360",
    "forward_price_720",
    "forward_price_1080",

    # 期权盈亏平衡价格 - 看涨 (option9)
    "call_breakeven_10",
    "call_breakeven_20",
    "call_breakeven_30",
    "call_breakeven_60",
    "call_breakeven_90",
    "call_breakeven_120",
    "call_breakeven_150",
    "call_breakeven_180",
    "call_breakeven_270",
    "call_breakeven_360",
    "call_breakeven_720",
    "call_breakeven_1080",

    # 期权盈亏平衡价格 - 看跌 (option9)
    "put_breakeven_10",
    "put_breakeven_20",
    "put_breakeven_30",
    "put_breakeven_60",
    "put_breakeven_90",
    "put_breakeven_120",
    "put_breakeven_150",
    "put_breakeven_180",
    "put_breakeven_270",
    "put_breakeven_360",
    "put_breakeven_720",
    "put_breakeven_1080",

    # 系统性风险 (model51)
    "beta_last_30_days_spy",
    "beta_last_60_days_spy",
    "beta_last_90_days_spy",
    "beta_last_360_days_spy"
]

base_target_list2 = [
    # 1. 可直接作为 target 的字段
    # analyst11
    "anl11_2_3tic",
    "anl11_2_2e",
    "anl11_2_3pme",
    "anl11_cit_totalcor",
    "anl11_e_totalcor",
    "anl11_cit1regsubsecrnk",
    "anl11_e2regsubsecperc",
    # analyst15
    "anl15_eps_forecast_fy1",
    "anl15_eps_forecast_fy2",
    "anl15_eps_revision_3m",
    # analyst69
    "anl69_revenue_forecast",
    "anl69_profit_margin_forecast",
    "anl69_ebitda_forecast",
    # analyst35
    "anl35_esg_score",
    # fundamental13
    "fnd13_net_income",
    "fnd13_total_assets",
    "fnd13_cash_flow_ops",
    # fundamental17
    "fnd17_pe_ratio",
    "fnd17_pb_ratio",
    # fundamental65
    "fnd65_us5000_cusip_sighc",
    "fnd65_us5000_cusip_vniu",
    "fnd65_us5000_cusip_salegstdev",
    "fnd65_us5000_cusip_slope4qeps3y",
    "fnd65_us5000_cusip_y3speq4vc",
    "fnd65_us5000_cusip_w15tvp",
    "fnd65_us5000_cusip_yoychgdistr",

    # 2. 通过组合运算生成的 target
    # analyst11 内部
    "anl11_2_3tic / anl11_2_2e",
    "anl11_cit_totalcor - anl11_e_totalcor",
    # analyst15 内部
    "anl15_eps_forecast_fy2 / anl15_eps_forecast_fy1",
    "anl15_eps_revision_3m * anl15_eps_forecast_fy1",
    # analyst69 内部
    "anl69_revenue_forecast / anl69_ebitda_forecast",
    "anl69_profit_margin_forecast - anl69_ebitda_forecast",
    # analyst35 与其他
    "anl35_esg_score / anl11_2_gse",
    # fundamental13 内部
    "fnd13_net_income / fnd13_total_assets",
    "fnd13_cash_flow_ops / fnd13_net_income",
    # fundamental17 内部
    "fnd17_pe_ratio / fnd17_pb_ratio",
    "fnd17_pe_ratio - fnd17_pb_ratio",
    # fundamental65 内部
    "fnd65_us5000_cusip_sighc / fnd65_us5000_cusip_vniu",
    "fnd65_us5000_cusip_slope4qeps3y * fnd65_us5000_cusip_y3speq4vc",
    # 跨数据集组合
    "anl11_2_3tic * fnd65_us5000_cusip_roe",
    "anl15_eps_forecast_fy1 / fnd17_pe_ratio",
    "anl69_revenue_forecast / fnd13_total_assets",
    "anl35_esg_score * fnd65_us5000_cusip_sue",
    "fnd13_cash_flow_ops / fnd65_us5000_cusip_tobinq"
]

base_target_list3 = [
    # 1. 可直接作为 target 的字段
    # analyst11
    "anl11_2_1e",
    "anl11_2_1g",
    "anl11_2_2g",
    "anl11_2_3e",
    "anl11_emp_totalcor",
    "anl11_g_totalcor",
    "anl11_e1regsubsecrnk",
    "anl11_g3reg_industryperc",
    # model26
    "mdl26_smrtst_grwth_ths_yrlst_yr_btd",
    "mdl26_smrtst_grwth_f12mt12m_rnngs",
    "mdl26_surprise_pct_last_y_ebitda",
    "mdl26_srprs_pct_lst_y_rnngs",
    "mdl26_stdv_rvsnclstr_nlysts_fq3_rnngs",
    "mdl26_stdv_stm_nlysts_fq2_rnngs",
    # model29
    "star_ebitda_smart_estimate_fq1",
    "star_ebitda_smart_estimate_12m",
    "star_ebitda_surprise_prediction_fq2",
    # model30
    "mdl30_smartestimate_fq1_eps",
    "mdl30_smartestimate_fy2_eps",
    "mdl30_psprise_pct_fq2_eps",
    # model51
    "beta_last_30_days_spy",
    "beta_last_360_days_spy",
    "systematic_risk_last_60_days",
    "unsystematic_risk_last_90_days",
    # 假设字段
    "fnd28_operating_margin",
    "fnd6_gross_profit",
    "anl14_eps_forecast_fq1",
    "news18_sentiment_score",
    "pv1_price",

    # 2. 通过组合运算生成的 target
    # analyst11 内部
    "anl11_2_1e / anl11_2_3e",
    "anl11_g_totalcor - anl11_emp_totalcor",
    # model26 内部
    "mdl26_smrtst_grwth_nxt_yrths_yr_rnngs / mdl26_smrtst_grwth_ths_yrlst_yr_rnngs",
    "mdl26_srprs_pct_lst_q_rnngs * mdl26_surprise_pct_last_y_ebitda",
    # model29 与 model30
    "star_ebitda_smart_estimate_fy1 / mdl30_smartestimate_fy1_eps",
    "star_ebitda_surprise_prediction_fq1 - mdl30_psprise_pct_fq1_eps",
    # model51 内部
    "beta_last_60_days_spy / systematic_risk_last_60_days",
    "unsystematic_risk_last_30_days - unsystematic_risk_last_360_days",
    # 跨数据集
    "anl11_2_1g * mdl26_smrtst_grwth_f12mt12m_rnngs",
    "mdl30_psprise_pct_fy2_eps / beta_last_90_days_spy",
    "star_ebitda_smart_estimate_12m / pv1_price",
    "news18_sentiment_score * anl11_emp_totalcor",
    "fnd6_gross_profit / mdl26_srprs_pct_lst_y_rvn"
]

grouping_fields = [
    # 直接分组字段 (pv1)
    "market",
    "sector",
    "subindustry",
    "country",
    "exchange",
    "industry",

    # 自定义分组字段 (groups)
    "bucket(rank(cap), range='0.1, 1, 0.1')",
    "bucket(rank(assets), range='0.1, 1, 0.1')",
    "bucket(group_rank(cap, sector), range='0.1, 1, 0.1')",
    "bucket(group_rank(assets, sector), range='0.1, 1, 0.1')",
    "bucket(rank(ts_std_dev(returns, 20)), range='0.1, 1, 0.1')",
    "bucket(rank(close * volume), range='0.1, 1, 0.1')",

    # 特定分组字段 (usa_group_13, usa_group_1, usa_group_2)
    "pv13_h_min2_3000_sector",
    "pv13_r2_min20_3000_sector",
    "pv13_r2_min2_3000_sector",
    "pv13_h_min2_focused_pureplay_3000_sector",
    "sta1_top3000c50",
    "sta1_allc20",
    "sta1_allc10",
    "sta1_top3000c20",
    "sta1_allc5",
    "sta2_top3000_fact3_c50",
    "sta2_top3000_fact4_c20",
    "sta2_top3000_fact4_c10"
]

grouping_fields = [
    # 直接分组字段 (pv1)
    "market",
    "sector",
    "subindustry",
    "country",
    "exchange",
    "industry",

    # 自定义分组字段 (groups)
    "bucket(rank(cap), range='0.1, 1, 0.1')",
    "bucket(rank(assets), range='0.1, 1, 0.1')",
    "bucket(group_rank(cap, sector), range='0.1, 1, 0.1')",
    "bucket(group_rank(assets, sector), range='0.1, 1, 0.1')",
    "bucket(rank(ts_std_dev(returns, 20)), range='0.1, 1, 0.1')",
    "bucket(rank(close * volume), range='0.1, 1, 0.1')",

    # 特定分组字段 (usa_group_13, usa_group_1, usa_group_2)
    "pv13_h_min2_3000_sector",
    "pv13_r2_min20_3000_sector",
    "pv13_r2_min2_3000_sector",
    "pv13_h_min2_focused_pureplay_3000_sector",
    "sta1_top3000c50",
    "sta1_allc20",
    "sta1_allc10",
    "sta1_top3000c20",
    "sta1_allc5",
    "sta2_top3000_fact3_c50",
    "sta2_top3000_fact4_c20",
    "sta2_top3000_fact4_c10"
]

cap_grouping_fields = [


    # 自定义分组字段 (groups)
    "bucket(rank(cap), range='0.1, 1, 0.1')",
    "bucket(rank(assets), range='0.1, 1, 0.1')",
    "bucket(group_rank(cap, sector), range='0.1, 1, 0.1')",
    "bucket(group_rank(assets, sector), range='0.1, 1, 0.1')",
    "bucket(rank(ts_std_dev(returns, 20)), range='0.1, 1, 0.1')",
    "bucket(rank(close * volume), range='0.1, 1, 0.1')"
]

eur_target_list = [
    # 直接字段
    "anl14_actvalue_ebit_fp0",
    "anl14_actvalue_ebitda_fp0",
    "anl14_actvalue_eps_fp0",
    "anl14_actvalue_revenue_fp0",
    "anl14_high_eps_fp1",
    "anl14_mean_eps_fp1",
    "anl14_high_revenue_fp1",
    "anl14_mean_revenue_fp1",
    "rtk_ptg_mean",
    "returns",
    "close",
    "vwap",
    "cap",
    "dividend",
    "fnd6_newq_ebitq",
    "fnd6_newq_niq",
    "fnd6_newq_saleq",
    "fnd6_pisa",
    "rp_css_earnings",
    "rp_ess_earnings",
    "rp_nip_earnings",
    "rp_css_revenue",

    # 组合字段
    "anl14_actvalue_eps_fp0 / anl14_actvalue_revenue_fp0",
    "anl14_high_eps_fp1 / anl14_high_revenue_fp1",
    "fnd6_newq_niq / fnd6_newq_saleq",
    "anl14_mean_eps_fp1 / rtk_ptg_mean",
    "anl14_high_eps_fp1 - anl14_high_eps_fp2",
    "anl14_mean_revenue_fp1 - anl14_mean_revenue_fp2",
    "anl14_high_ebit_fp1 - anl14_actvalue_ebit_fp0",
    "log(anl14_actvalue_revenue_fp0)",
    "log(cap)",
    "log(fnd6_newq_saleq)",
    "anl14_actvalue_ebitda_fp0 + anl14_actvalue_revenue_fp0",
    "anl14_high_eps_fp1 + rtk_ptg_mean",
    "fnd6_newq_ebitq + fnd6_newq_niq",
    "rp_css_earnings + rp_ess_earnings",
    "rp_css_earnings * returns",
    "rp_nip_revenue * vwap"
]

eur_sector_list = [
    # 直接使用的 GROUP 字段（高覆盖率）
    "country",
    "exchange",
    "industry",
    "market",
    "sector",
    "subindustry",
    "sta1_allc10",
    "sta1_allc20",
    "sta1_top1200c10",
    "pv13_2_sector",
    "pv13_52_sector",
    "pv13_5_sector",
    "pv13_di_5l",
    "pv13_di_6l",
    "pv13_rcsed_6l",

    # 直接使用的 AI/ML 增强字段
    "oth455_competitor_n2v_p10_q200_w1_kmeans_cluster_10",
    "oth455_competitor_n2v_p10_q200_w1_pca_fact1_cluster_10",
    "oth455_relation_roam_w5_pca_fact3_cluster_10",
    "oth455_relation_roam_w5_pca_fact3_cluster_20",

    # 桶分组
    "bucket(rank(cap), range='0.1, 1, 0.1')",
    "bucket(rank(ts_std_dev(returns, 20)), range='0.1, 1, 0.1')",
    "bucket(rank(close * volume), range='0.1, 1, 0.1')",

    # 组内排名分组
    "bucket(group_rank(cap, sector), range='0.1, 1, 0.1')",
    "bucket(group_rank(cap, industry), range='0.1, 1, 0.1')",
    "bucket(group_rank(ts_std_dev(returns, 20), sector), range='0.1, 1, 0.1')",
    "bucket(group_rank(close * volume, sector), range='0.1, 1, 0.1')",
    "bucket(group_rank(cap, oth455_competitor_n2v_p10_q200_w1_kmeans_cluster_10), range='0.1, 1, 0.1')"
]


def generate_alpha_list(target_list, market_list, sector_list, cap, days, templates=None):
    """
    生成 alpha 表达式列表，支持指定模板。

    Args:
        target_list (list): 目标数据字段列表。
        market_list (list): 市场数据字段列表。
        sector_list (list): 行业分类字段列表。
        cap (str): 市值字段。
        days (list): 回归天数列表。
        templates (list, optional): 需要生成的 alpha 模板名称列表，例如 ['basic_alpha', 'residual_alpha']。
                                   如果为 None，则生成所有模板。

    Returns:
        list: 包含生成的 alpha 表达式的列表。
    """
    # 定义所有 alpha 模板
    alpha_templates = {
        'base_alpha': """
            target_data = winsorize(ts_backfill({target}, 63), std=4.0);
            alpha_{count}_basic = ts_regression(target_data, group_mean(target_data, log(ts_mean({cap}, 21)), densify({sector})), {day}, rettype=2);
            alpha_{count}_basic
        """,
        'base_rank_alpha': """
            target_data = winsorize(ts_backfill({target}, 63), std=4.0);
            alpha_{count}_basic = ts_regression(target_data, group_mean(target_data, log(ts_mean({cap}, 21)), densify({sector})), {day}, rettype=2);
            alpha = rank(alpha_{count}_basic);
            alpha
        """,
        'basic_alpha': """
            target_data = winsorize(ts_backfill({target}, 63), std=4.0);
            market_data = winsorize(ts_backfill({market}, 63), std=4.0);
            alpha_{count}_basic = ts_regression(target_data, group_mean(market_data, log(ts_mean({cap}, 21)), densify({sector})), {day}, rettype=2);
            alpha_{count}_basic
        """,
        'residual_alpha': """
            target_data = winsorize(ts_backfill({target}, 63), std=4.0);
            market_data = winsorize(ts_backfill({market}, 63), std=4.0);
            beta = ts_regression(target_data, group_mean(market_data, log(ts_mean({cap}, 21)), densify({sector})), {day}, rettype=2);
            predicted_return = beta * market_data;
            alpha_{count}_residual = - target_data + predicted_return;
            alpha_{count}_residual
        """,
        'beta_alpha': """
            target_data = winsorize(ts_backfill({target}, 63), std=4.0);
            market_data = winsorize(ts_backfill({market}, 63), std=4.0);
            alpha_{count}_beta = rank(ts_regression(target_data, group_mean(market_data, log(ts_mean({cap}, 21)), densify({sector})), {day}, rettype=2));
            alpha_{count}_beta
        """,
        'beta_change_alpha': """
            target_data = winsorize(ts_backfill({target}, 63), std=4.0);
            market_data = winsorize(ts_backfill({market}, 63), std=4.0);
            beta = ts_regression(target_data, group_mean(market_data, log(ts_mean({cap}, 21)), densify({sector})), {day}, rettype=2);
            alpha_{count}_beta_change = rank(ts_delta(beta, 21));
            alpha_{count}_beta_change
        """,
        'residual_trend_alpha': """
            target_data = winsorize(ts_backfill({target}, 63), std=4.0);
            market_data = winsorize(ts_backfill({market}, 63), std=4.0);
            beta = ts_regression(target_data, group_mean(market_data, log(ts_mean({cap}, 21)), densify({sector})), {day}, rettype=2);
            predicted_return = beta * market_data;
            residual = target_data - predicted_return;
            alpha_{count}_residual_trend = rank(ts_mean(residual, 21));
            alpha_{count}_residual_trend
        """
    }

    # 如果 templates 为 None，生成所有模板
    if templates is None:
        templates = list(alpha_templates.keys())

    # 验证 templates 参数
    invalid_templates = [t for t in templates if t not in alpha_templates]
    if invalid_templates:
        raise ValueError(f"无效的模板名称: {invalid_templates}")

    alpha_list = []
    count = 0

    for target in target_list:
        for market in market_list:
            for sector in sector_list:
                for day in days:
                    count += 1
                    # 根据指定的模板生成 alpha 表达式
                    for template_name in templates:
                        alpha_expr = alpha_templates[template_name].format(
                            target=target,
                            market=market,
                            sector=sector,
                            cap=cap,
                            day=day,
                            count=count
                        )
                        alpha_list.append(alpha_expr)

    return alpha_list