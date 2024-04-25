from typing import Generator
from jupyter_server.serverapp import list_running_servers
import requests
import logging
from datetime import datetime, timedelta, timezone
import argparse
import socket
import sched

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS, WritePrecision

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s')
log = logging.getLogger("KernelStatus")

def report_kernel_status() -> Generator[str, bool, datetime]:
    """
    Yields all notebooks on this machine, their active status, and when they were last active.
    """
    # Get all the servers that are running
    servers = list_running_servers()
    for server in servers:
        log.debug("Server at %s", server['url'])
        # For each server, get the list of sessions and their status
        url = f"{server['url']}api/sessions"
        log.debug("Url %s", url)
        token = server['token']
        log.debug("Token: %s", token)

        try:
            token_payload = {"token": token}
            response = requests.get(url, params=token_payload)
        except Exception as e:
            log.error(f"Connection failed: {e}")
        else:
            if response.status_code == 200: # Server responded properly, otherwise assume offline
                sessions = response.json()
                for session in sessions: # Extract data for every session
                    notebook_path = session['notebook']['path']
                    active = session['kernel']['execution_state'] != "idle"
                    last_activity = datetime.strptime(session['kernel']['last_activity'], "%Y-%m-%dT%H:%M:%S.%f%z")
                    # If the kernel is active, set the last activity to now
                    if active:
                        last_activity = datetime.now()
                    yield (notebook_path, active, last_activity)

def report_to_influx(config: dict, machine: str, path: str, active_data: datetime):
    """
    Report, for the given machine, the last active file, and when it was last seen active.
    """

    client = InfluxDBClient(url=config['url'], token=config['token'], org=config['org'])

    datum = Point.measurement("kernel_status")\
        .tag("machine", machine)\
        .field("active_notebook", path)\
        .time(active_data, WritePrecision.NS)
    
    write_api = client.write_api(write_options=SYNCHRONOUS)

    write_api.write(bucket = config['bucket'], record = datum)


def periodic_report(scheduler: sched.scheduler, config: dict):
    # Reenter the scheduler
    scheduler.enter(float(config['every']), 1, periodic_report, (scheduler, config))

    # Get the reports
    log.info("Fetching kernel status...")
    reports = sorted(list(report_kernel_status()), key=lambda x: x[2].timestamp(), reverse=True)
    if len(reports) > 0:
        log.info("%s Kernel(s) found. Last active:", len(reports))
        path, active, last_seen = reports[0]
        log.info(f"{path}\t{'active' if active else f'idle'}\t{last_seen}")
        if datetime.now(tz=timezone.utc) - last_seen < timedelta(seconds=config['every']*1.5):
            # Fresh, so report it
            report_to_influx(config['influx'], socket.gethostname(), path, last_seen)
            log.info("Reported to InfluxDB")
        else:
            log.info("Not reporting to InfluxDB, too old")
    else:
        log.info("No kernels found")


def main():
    parser = argparse.ArgumentParser(prog="ganymed", description='Report the status of all jupyter kernels')
    parser.add_argument('--server-config', type=str, help='The path to the server configuration file', required=True)
    args = parser.parse_args()

    log.info("Starting Ganymed...")
    log.info("Reading configuration from %s", args.server_config)

    import tomli
    config = tomli.load(open(args.server_config, 'rb'))
    
    scheduler = sched.scheduler()
    periodic_report(scheduler, config)
    scheduler.run()

if __name__ == "__main__":
    main()