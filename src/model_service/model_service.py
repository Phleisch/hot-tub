"""!@brief Interface class to load the temperature difference model.
@details Matrix is always of size (900, 1800). Matrix element (0,0) lies in the top left corner.
@file model_service.py ModelService class file.
@author Martin Schuck
@date 21.10.2020
"""

import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt

from reference_model.reference_model_loader import ReferenceModelLoader


class ModelService:
    """!@brief This class is responsible for loading the global temperature difference model.

    Main functionality lies in the @ref load_model function. Plotting of models is only supported for debugging.
    """
    ROOT_PATH = Path(__file__).resolve().parents[2]

    def __init__(self):
        """!@brief Standard constructor.
        """
        self.reference_model_loader = ReferenceModelLoader()

    def load_model(self, day, plot_model=False):
        """!@brief Loads the global reference temperature matrix from the cache.

        @param day Day of the year to load (from 0 to 364). Type int.
        @param plot_model Flag to plot the model. Plot is given as a seperate matplotlib plot window. Type bool.
        @return Returns the global temperature difference matrix of shape (900,1800).
        """

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
