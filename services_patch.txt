# PATCH for services.py
# Add inside startup_event(), after WS subscriptions:

for sig in active_root_signals.values():
    try:
        asyncio.create_task(notify_alignment_if_ready(sig))
    except Exception as e:
        log("Failed to run alignment check for persisted signal:", sig.get("id"), e)
