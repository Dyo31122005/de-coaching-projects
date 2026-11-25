import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator


class SparkHealthCheckOperator(BaseOperator):
    """Kiểm tra sức khỏe cụm Spark chạy trên YARN qua ResourceManager REST API.

    Spark trong khóa học chạy `--master yarn` nên:
      - master  = YARN ResourceManager
      - worker  = YARN NodeManager

    1. Master available: /ws/v1/cluster/info, state phải là STARTED
    2. Worker available: /ws/v1/cluster/nodes, đếm node RUNNING
    3. Resource của từng worker: core CPU, RAM đã dùng / còn trống, % sử dụng
    4. Spark application: các app đang RUNNING, app bắt buộc có đang chạy không

    - Master down / không đủ worker => raise => task fail => on_failure_callback
    - Vượt ngưỡng resource, app không chạy => ghi vào `issues`
    """

    def __init__(self, yarn_conn_id, min_running_workers=1, max_cpu_usage_pct=90, max_memory_usage_pct=90,
                 required_apps=None, timeout=10, **kwargs):
        super().__init__(**kwargs)
        self.yarn_conn_id = yarn_conn_id
        self.min_running_workers = min_running_workers
        self.max_cpu_usage_pct = max_cpu_usage_pct
        self.max_memory_usage_pct = max_memory_usage_pct
        self.required_apps = required_apps or []
        self.timeout = timeout

    def execute(self, context):
        conn = BaseHook.get_connection(self.yarn_conn_id)
        self.base_url = f"{conn.host.rstrip('/')}:{conn.port}/ws/v1/cluster"
        issues = []

        master = self._check_master()
        workers = self._check_workers(issues)
        apps = self._check_apps(issues)

        return {'master': master, 'workers': workers, 'running_apps': apps, 'issues': issues}

    def _get(self, path):
        try:
            response = requests.get(f"{self.base_url}{path}", timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise AirflowException(f"Không kết nối được Spark master (YARN ResourceManager): {e}")

    # 1. Master -----------------------------------------------------------------
    def _check_master(self):
        info = self._get('/info')['clusterInfo']
        self.log.info("Master state=%s haState=%s version=%s", info['state'], info['haState'],
                      info['resourceManagerVersion'])
        if info['state'] != 'STARTED':
            raise AirflowException(f"Spark master không hoạt động (state={info['state']})")
        return {'state': info['state'], 'ha_state': info['haState']}

    # 2 + 3. Worker & resource --------------------------------------------------
    def _check_workers(self, issues):
        nodes = (self._get('/nodes').get('nodes') or {}).get('node', [])

        workers = []
        for node in nodes:
            used_cores, free_cores = node.get('usedVirtualCores', 0), node.get('availableVirtualCores', 0)
            used_mem, free_mem = node.get('usedMemoryMB', 0), node.get('availMemoryMB', 0)
            total_cores, total_mem = used_cores + free_cores, used_mem + free_mem
            worker = {
                'host': node['nodeHostName'],
                'state': node['state'],
                'cores_used': used_cores,
                'cores_total': total_cores,
                'cpu_usage_pct': round(used_cores * 100 / total_cores, 1) if total_cores else 0,
                'memory_used_mb': used_mem,
                'memory_total_mb': total_mem,
                'memory_usage_pct': round(used_mem * 100 / total_mem, 1) if total_mem else 0,
            }
            workers.append(worker)
            self.log.info("Worker %(host)s state=%(state)s cpu=%(cores_used)s/%(cores_total)s cores "
                          "(%(cpu_usage_pct)s%%) ram=%(memory_used_mb)s/%(memory_total_mb)s MB "
                          "(%(memory_usage_pct)s%%)", worker)

            if worker['state'] != 'RUNNING':
                issues.append(f"Worker {worker['host']} không hoạt động (state={worker['state']})")
                continue
            if worker['cpu_usage_pct'] > self.max_cpu_usage_pct:
                issues.append(f"Worker {worker['host']} dùng {worker['cpu_usage_pct']}% CPU "
                              f"(ngưỡng {self.max_cpu_usage_pct}%)")
            if worker['memory_usage_pct'] > self.max_memory_usage_pct:
                issues.append(f"Worker {worker['host']} dùng {worker['memory_usage_pct']}% RAM "
                              f"(ngưỡng {self.max_memory_usage_pct}%)")

        running = sum(1 for w in workers if w['state'] == 'RUNNING')
        if running < self.min_running_workers:
            raise AirflowException(f"Chỉ có {running}/{len(workers)} worker RUNNING "
                                   f"(tối thiểu {self.min_running_workers})")
        return workers

    # 4. Spark application ------------------------------------------------------
    def _check_apps(self, issues):
        apps = (self._get('/apps?states=RUNNING&applicationTypes=SPARK').get('apps') or {}).get('app', [])
        running = [app['name'] for app in apps]
        self.log.info("Running Spark apps (%d): %s", len(running), running)

        for name in self.required_apps:
            if name not in running:
                issues.append(f"Spark app '{name}' không chạy trên cụm")
        return running
