"""Global test fixtures — disable webhooks and use isolated test DB."""

import os
import pytest

# Disable webhooks before any app imports
os.environ["WEBHOOK_ENABLED"] = "false"
os.environ["WEBHOOK_URL"] = ""
os.environ["DRY_RUN"] = "true"
os.environ["AUTO_EXECUTE_THRESHOLD"] = "0.85"
