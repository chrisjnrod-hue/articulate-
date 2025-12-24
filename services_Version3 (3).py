# Compatibility shim: make "import services" resolve to the real services_core module
# so scanners.py (which does `import services as s`) uses the same module object / globals.
#
# Drop this file next to services_core.py and redeploy/restart your service.

import importlib
import sys

# Import the actual implementation module
_services_core = importlib.import_module("services_core")

# Ensure future "import services" returns the services_core module object
sys.modules["services"] = _services_core

# Populate this module namespace with public attributes from services_core
# so code that inspected 'services' attributes works either way.
for name in dir(_services_core):
    if name.startswith("_"):
        continue
    try:
        globals()[name] = getattr(_services_core, name)
    except Exception:
        pass