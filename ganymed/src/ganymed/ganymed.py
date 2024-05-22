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
            response = requests.get(url, params=token_payload, timeout=5)
        except Exception as e:
            log.error(f"Connection failed: {e}")
        else:
            if response.status_code == 200: # Server responded properly, otherwise assume offline
                sessions = response.json()
                for session in sessions: # Extract data for every session
                    if session['type'] != 'notebook':
                        continue
                    notebook_path = session['notebook']['path']
                    active = session['kernel']['execution_state'] != "idle"
                    last_activity = datetime.strptime(session['kernel']['last_activity'], "%Y-%m-%dT%H:%M:%S.%f%z")
                    yield (notebook_path, active, last_activity)

def report_to_influx(config: dict, machine: str, currently_active: bool):
    """
    Report for the given machine when it was last seen active.
    """
    log.info("Reporting: %s is %s", machine, "active" if currently_active else "idle")
    client = InfluxDBClient(url=config['url'], token=config['token'], org=config['org'])

    datum = Point.measurement("kernel_status")\
        .field("presence", 1 if currently_active else 0)\
        .tag("machine", machine)\
        .time(datetime.now(tz=timezone.utc), WritePrecision.NS)
    
    write_api = client.write_api(write_options=SYNCHRONOUS)

    write_api.write(bucket = config['bucket'], record = datum)
    log.debug("Report sent.")


def periodic_report(scheduler: sched.scheduler, config: dict):
    # Reenter the scheduler
    scheduler.enter(float(config['interval']['every']), 1, periodic_report, (scheduler, config))

    # Get the reports
    log.info("Fetching kernel status...")
    reports = sorted(list(report_kernel_status()), key=lambda x: x[2].timestamp(), reverse=True)
    if len(reports) > 0:
        log.info("%s Kernel(s) found. Last active:", len(reports))
        path, active, last_seen = reports[0]
        log.info(f"{path}\t{'active' if active else f'idle'}\t{last_seen}")
        currently_active = active or (datetime.now(tz=timezone.utc) - last_seen < timedelta(seconds=int(config['interval']['every'])*1.5))
    else:
        log.debug("No kernels found")
        currently_active = False
    
    report_to_influx(config['influx'], socket.gethostname(), currently_active)


def main():
    parser = argparse.ArgumentParser(prog="ganymed", description='Report the status of all jupyter kernels')
    parser.add_argument('--server-config', type=str, help='The path to the server configuration file', required=True)
    args = parser.parse_args()

    log.info("Starting Ganymed...")
    log.info("Reading configuration from %s", args.server_config)

    from configparser import ConfigParser
    config = ConfigParser()
    config.read(args.server_config)
    
    scheduler = sched.scheduler()
    periodic_report(scheduler, config)
    scheduler.run()

if __name__ == "__main__":
    main()