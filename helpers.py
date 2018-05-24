"""Helpers"""

# Imports =====================================================================

import requests

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# =============================================================================


def requests_retry_session(retries=10, backoff_factor=1, status_forcelist=(400, 500, 502, 503, 504)):
    """Requests session with retries"""
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# END =========================================================================
