import itertools
from machine_lib import *

def generate_alpha_expressions(targets, group1, group2, days):
    """
    Generate all combinations of alpha expressions based on the provided template.
    
    Args:
        targets (list): List of fundamental field IDs or expressions (e.g., ['fnd17_ttmepsincx', '-fnd17_apr2rev']).
        group1 (list): List of group fields for volume_factor, fundamental_factor, and price_momentum (e.g., ['sector', 'industry']).
        group2 (list): List of group fields for group_neutralize (e.g., ['sector', 'market']).
    
    Returns:
        list: List of alpha expression strings for all combinations of targets, group1, and group2.
    """
    # Input validation
    if not isinstance(targets, list) or not targets:
        raise ValueError("targets must be a non-empty list")
    if not isinstance(group1, list) or not group1:
        raise ValueError("group1 must be a non-empty list")
    if not isinstance(group2, list) or not group2:
        raise ValueError("group2 must be a non-empty list")
    
    # Alpha template
    alpha_template = """fundamental_data = winsorize(ts_backfill({target}, 63), std=4.0);
                    my_group = {group1};
                    my_group2 = {group2};
                    volume_ratio = volume / ts_sum(volume, 252);
                    volume_factor = group_rank(ts_decay_linear(volume_ratio, 10), my_group);
                    fundamental_factor = group_rank(ts_rank(fundamental_data, {day}), my_group);
                    price_momentum = group_rank(-ts_delta(close, 5), my_group);
                    alpha = rank(volume_factor * fundamental_factor * price_momentum);
                    trade_signal = trade_when(volume > adv20,group_neutralize(alpha, my_group2),-1);
                    trade_signal"""
    
    # Generate all combinations of targets, group1, and group2
    combinations = list(itertools.product(targets, group1, group2, days))
    
    # Generate alpha expressions for each combination
    expressions = []
    seen = set()  # For deduplication
    for target, g1, g2, d in combinations:
        # Format the expression
        expression = alpha_template.format(
            target=target,
            group1=g1,
            group2=g2,
            day = d
        )
        
        # Deduplicate using expression string
        if expression not in seen:
            seen.add(expression)
            expressions.append(expression)
    
    return expressions

def generate_sentiment_alpha_expressions(targets, groups):
    
    # Input validation
    if not isinstance(targets, list) or not targets:
        raise ValueError("targets must be a non-empty list")
    if not isinstance(groups, list) or not groups:
        raise ValueError("groups must be a non-empty list")
    
    # Alpha template
    alpha_template = """sentiment = ts_backfill(ts_delay({target},1),20);
                        vhat=ts_regression(volume,sentiment,250);
                        ehat=-ts_regression(returns,vhat,750); 
                        alpha=group_rank(ehat,densify({group}));
                        alpha"""
    
    # Generate all combinations of targets, group1, and group2
    combinations = list(itertools.product(targets, groups))
    
    # Generate alpha expressions for each combination
    expressions = []
    seen = set()  # For deduplication
    for target, group in combinations:
        # Format the expression
        expression = alpha_template.format(
            target=target,
            group=group
        )
        
        # Deduplicate using expression string
        if expression not in seen:
            seen.add(expression)
            expressions.append(expression)
    
    return expressions


def generate_group_mean_datafield(df_list):
    """
    Generate group mean datafield expressions from a list of datafields.
    
    Args:
        df_list (list): List of datafield strings (e.g., ['fnd17_ttmepsincx', 'fnd17_apr2rev']).
    
    Returns:
        list: List of expressions in format "group_mean({datafield}, 1, subindustry) - {datafield}"
    """
    # Input validation
    if not isinstance(df_list, list) or not df_list:
        raise ValueError("df_list must be a non-empty list")
    
    expressions = []
    for datafield in df_list:
        expression = f"group_mean(winsorize(ts_backfill({datafield}, 120), std=4), 1, subindustry) - winsorize(ts_backfill({datafield}, 120), std=4)"
        expressions.append(expression)
    
    return expressions

def generate_group_mean_datafield_2(df_list):
    """
    Generate group mean datafield expressions from a list of datafields.
    
    Args:
        df_list (list): List of datafield strings (e.g., ['fnd17_ttmepsincx', 'fnd17_apr2rev']).
    
    Returns:
        list: List of expressions in format "group_mean({datafield}, 1, subindustry) - {datafield}"
    """
    # Input validation
    if not isinstance(df_list, list) or not df_list:
        raise ValueError("df_list must be a non-empty list")
    
    expressions = []
    for datafield in df_list:
        expression = f"winsorize(ts_backfill({datafield}, 120), std=4)/group_mean(winsorize(ts_backfill({datafield}, 120), std=4), 1, subindustry)"
        expressions.append(expression)
    
    return expressions
