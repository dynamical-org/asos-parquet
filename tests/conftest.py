import modal

# modal_app.py refuses to import locally unless Modal targets the `critical`
# environment. Set it the way `modal deploy --env critical` does, before any
# test module imports the app.
modal.config.config.override_locally("environment", "critical")
