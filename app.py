import sentry_sdk

from chalice import Chalice, Rate
from chalicelib.service import run, history
from chalicelib.settings import SCHEDULE_RATE, SENTRY_DSN

from sentry_sdk.integrations.chalice import ChaliceIntegration

sentry_sdk.init(
    dsn=SENTRY_DSN,
    integrations=[ChaliceIntegration()],
    traces_sample_rate=1.0
)

app = Chalice(app_name='sensors-africa-smartcitizen')

# Automatically runs every 10 minutes
@app.schedule(Rate(int(SCHEDULE_RATE), unit=Rate.HOURS))
def periodic_task(event):
    app.log.debug(event.to_dict())
    return run(app)
