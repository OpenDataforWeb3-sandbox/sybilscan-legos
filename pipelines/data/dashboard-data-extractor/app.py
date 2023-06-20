import fire
from extract_dashboard_data import DashboardDataExtractor
from db import app_manager

def extract_dashboard_data():
    """ """
    extractor = DashboardDataExtractor()
    extractor.run()


if __name__ == "__main__":
    fire.Fire(extract_dashboard_data)