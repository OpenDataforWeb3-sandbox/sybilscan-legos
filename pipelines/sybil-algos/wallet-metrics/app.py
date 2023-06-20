import logging
import fire
from db import grant_manager, app_manager
from tagger import Tagger
from config import TAG_CONFIG

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

def run_tagger():
    """
    onchain, farmer, moneymixer,
    """
    #retrieve all the wallets that do not have tags

    wallets = grant_manager.get_grant_wallets(chain='ethereum')
    wallets += grant_manager.get_voter_wallets(chain='ethereum')

    unique_wallets = list(set([w['wallets'] for w in wallets]))

    tagger = Tagger()
    wallet_metrics = tagger.tag(unique_wallets)
    app_manager.update_wallet_metrics(wallet_metrics)
    
if __name__ == "__main__":
    fire.Fire(run_tagger)