import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt

from reference_model.reference_model_loader import ReferenceModelLoader


class ModelService:

    def __init__(self):
        self.ROOT_PATH = Path(__file__).resolve().parents[2]
        self.reference_model_loader = ReferenceModelLoader()

    def load_model(self, day, plot_model=False):
        reference_model = self.reference_model_loader.load_model(day)
        try:
            print(self.ROOT_PATH.joinpath('data', 'current_model.npy'))
            current_model = np.load(self.ROOT_PATH.joinpath('data', 'current_model.npy'), allow_pickle=False)
        except FileNotFoundError:
            print("Error in ModelService.load_model: Could not find the current model!")
            return
        if plot_model:
            plt.subplot()
            plt.imshow(current_model - reference_model, extent=(0, 2, 0, 1))
            plt.title('Base model')
            plt.gcf().set_size_inches(18, 18)
            plt.show()
        return current_model - reference_model
