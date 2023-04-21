from functools import wraps

def timeit(**dec_kwargs):
    time_param = dec_kwargs.get('time','')
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            returns = func(*args,  **kwargs)
            time_seconds = time.perf_counter()-start
            if time_param=='seconds':
                print('\t' + f"Total time: {int(time_seconds)} seconds")
            else:
                print('\t' + f"Total time: {int(time_seconds//60)} minutes")
            return returns
        return wrapper
    return decorator