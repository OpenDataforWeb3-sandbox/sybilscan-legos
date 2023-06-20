import fire
from compute_grant_metrics import GrantMetricsCalculator
from db import grant_manager, app_manager

def run_grant_application_metrics():
    """ """
    #TODO: get only active applications
    applications = grant_manager.get_round_applications_with_votes(chain='ethereum')
    print(len(applications))
    grant_metrics = GrantMetricsCalculator()
    application_metrics = grant_metrics.compute(applications)
    app_manager.update_application_metrics(application_metrics)

if __name__ == "__main__":
    fire.Fire(run_grant_application_metrics)
