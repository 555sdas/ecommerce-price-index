import matplotlib.pyplot as plt
import pandas as pd
from typing import Dict
from config.settings import settings


class PriceIndexVisualizer:
    def plot_indices(self, indices_data: Dict[str, pd.DataFrame], save_path: str = None):
        """绘制价格指数图表"""
        plt.figure(figsize=(12, 6))

        # 绘制Cavallo指数
        plt.plot(
            indices_data["merged"]["date"],
            indices_data["merged"]["cavallo_index"],
            label='Cavallo Index'
        )

        # 绘制Tmall指数
        plt.plot(
            indices_data["merged"]["date"],
            indices_data["merged"]["tmall_index"],
            label='Tmall Index'
        )

        plt.legend()
        plt.title("Online Price Index: Cavallo vs Tmall")
        plt.xlabel("Date")
        plt.ylabel("Index Value")
        plt.grid(True)

        if save_path:
            plt.savefig(save_path)
        else:
            plt.show()