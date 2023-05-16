import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.decomposition import NMF

from typing import Type, List, Tuple


def reduce_mem_usage(df: Type[pd.DataFrame], verbose: bool = True):
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    start_mem = df.memory_usage().sum() / 1024 ** 2
    for col in df.columns:
        col_type = df[col].dtypes
        if col_type in numerics:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
    end_mem = df.memory_usage().sum() / 1024 ** 2
    if verbose:
        print('Mem. usage decreased to {:5.2f} Mb ({:.1f}% reduction)'
              .format(end_mem, 100 * (start_mem - end_mem) / start_mem))
    return df


def plot_top_words(model: Type[NMF], feature_names: List[str],
                   n_top_words: int, title: str, rows: int, columns: int,
                   figsize: Tuple[int, int] = (30, 25), fontsize: int = 20):
    """
    Plot top n words per component of NMF/LDA model
    """
    with plt.style.context("seaborn-white"):
        fig, axes = plt.subplots(rows, columns, figsize=figsize, sharex=True)
        axes = axes.flatten()
        for topic_idx, topic in enumerate(model.components_):
            top_features_ind = topic.argsort()[:-n_top_words - 1:-1]
            top_features = [feature_names[i] for i in top_features_ind]
            weights = topic[top_features_ind]

            ax = axes[topic_idx]
            ax.barh(top_features, weights, height=0.7)
            ax.set_title(f'Topic {topic_idx +1}',
                         fontdict={'fontsize': int(fontsize*1.5)})
            ax.invert_yaxis()
            ax.tick_params(axis='both', which='major', labelsize=fontsize)
            for i in 'top right left'.split():
                ax.spines[i].set_visible(False)
            fig.suptitle(title, fontsize=int(fontsize*1.5))

        plt.subplots_adjust(top=0.90, bottom=0.05, wspace=0.90, hspace=0.3)
        plt.show()


def generate_pivot_query(table_name: str, columns_field: str, columns: List[str],
                         value_field: str, group_by: List[str] = None, function: str = "sum"):

    if group_by is not None:
        groups = ",\n\t".join(group_by) + ","
        group_by = f"group by\n\t{groups[0:len(groups)-1]}"
    else:
        groups = ""
        group_by = ""

    case_whens = ",\n\t".join([
        f"""{function}(case when {columns_field} = '{v}' then {value_field} else 0 end) as \"{v}\""""
        for v in columns])

    select_statement = f"""select
        {groups}
        {case_whens}"""

    return f"""
    {select_statement}
    from {table_name}
    {group_by}
    """