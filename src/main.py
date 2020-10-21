from model_service.model_service import ModelService


if __name__ == '__main__':
    model_service = ModelService()
    model_service.load_model(0, plot_model=True)
