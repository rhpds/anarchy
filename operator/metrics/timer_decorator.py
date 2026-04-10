import inspect
import os
import time
from functools import wraps

from .app_metrics import AppMetrics

cluster_domain = os.environ.get('CLUSTER_DOMAIN')

def async_timer(app: str):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            status = 'success'
            try:
                result = await func(*args, **kwargs)
                status = 'success'
            except Exception as e:
                status = 'error'
                raise e
            finally:
                duration = time.time() - start_time
                labels = {
                    'method': func.__name__,
                    'status': status,
                    'app': app,
                    'cluster_domain': cluster_domain,
                }
                AppMetrics.process_time.observe(labels, duration)
            return result
        return wrapper
    return decorator


def sync_timer(app: str):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                status = 'success'
            except Exception as e:
                status = 'error'
                raise e
            finally:
                duration = time.time() - start_time
                labels = {
                    'method': func.__name__,
                    'status': status,
                    'app': app,
                    'cluster_domain': cluster_domain,
                }
                AppMetrics.process_time.observe(labels, duration)
            return result
        return wrapper
    return decorator


class TimerDecoratorMeta(type):
    def __new__(cls, name, bases, dct):
        for attr_name, attr_value in dct.items():
            if isinstance(attr_value, classmethod):
                original_method = attr_value.__func__
                if inspect.iscoroutinefunction(original_method):
                    decorated_method = async_timer(name)(original_method)
                else:
                    decorated_method = sync_timer(name)(original_method)
                dct[attr_name] = classmethod(decorated_method)
            elif callable(attr_value) and not attr_name.startswith("__"):
                if inspect.iscoroutinefunction(attr_value):
                    dct[attr_name] = async_timer(name)(attr_value)
                else:
                    dct[attr_name] = sync_timer(name)(attr_value)
        return super().__new__(cls, name, bases, dct)
