"""Config package exports.

This file re-exports the Settings instance created in the top-level
`app/config.py` file so consumers can do `from app.config import settings`.
"""

import os
import sys
from pathlib import Path

# Add the parent directory to sys.path to import config.py directly
parent_dir = Path(__file__).parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Import the config module directly
import config as _config_module

# Re-export the settings instance
settings = _config_module.settings

__all__ = ["settings"]
