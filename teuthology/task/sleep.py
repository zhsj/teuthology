import time

def task(ctx, config):
    assert isinstance(config, dict), 'configuration must be a dict'
    time.sleep(config.get('time', 60))
