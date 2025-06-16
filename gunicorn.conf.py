from multiprocessing import cpu_count

bind = "0.0.0.0:5002"
workers = max(2, min(4, cpu_count() // 2))
threads = 4

timeout = 120
keepalive = 5

loglevel = "info"
accesslog = "-"
errorlog = "-"

preload_app = True
worker_connections = 1000
max_requests = 500
max_requests_jitter = 50

preload_app = True
