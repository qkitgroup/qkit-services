from typing import AsyncIterator
from jupyter_server.extension.application import ExtensionApp
from jupyter_server.services.sessions.sessionmanager import SessionManager
from traitlets import Unicode, Int
from datetime import datetime, timedelta, timezone
import socket
import asyncio
import traceback

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS, WritePrecision

class GanymedServer(ExtensionApp):
    # Extension Metadata
    name = "ganymed"
    extension_url = "/ganymed/default"
    load_other_extensions = True

    # Configuration Options
    interval = Int(10, config=True, help="Interval for reporting status.")
    influx_address = Unicode("", config=True, help="InfluxDB Address.")
    influx_token = Unicode("", config=True, help="InfluxDB Token.")
    influx_org = Unicode("", config=True, help="InfluxDB Org.")
    influx_bucket = Unicode("ganymed", config=True, help="InfluxDB Bucket.")
    host_name = Unicode(socket.gethostname(), config=True, help="Hostname of the machine.")

    def initialize_settings(self):
        """Initialize settings."""
        self.log.info(f"Config {self.config}")
        self.log.info(f"Reporting to {self.influx_address} org {self.influx_org} in bucket {self.influx_bucket} every {self.interval} seconds.")
    
    def initialize(self):
        super().initialize()
        self.log.info("Starting Scheduler...")
        loop = asyncio.get_event_loop()
        self.task = loop.create_task(self.report_main())
        self.log.info("Scheduler started!")

    async def report_main(self):
        """
        Reporting entry point.
        """
        while True:
            await asyncio.sleep(self.interval)
            try:
                await self._periodic_report()
            except Exception as e:
                self.log.error(f"Failed to report: {e}")
                self.log.error(traceback.format_exc())
        

    def _report_to_influx(self, currently_active: bool):
        """
        Report for the given machine when it was last seen active.
        """
        self.log.info("Reporting: %s is %s", self.host_name, "active" if currently_active else "idle")
        client = InfluxDBClient(url=self.influx_address, token=self.influx_token, org=self.influx_org)

        datum = Point.measurement("kernel_status")\
            .field("presence", 1 if currently_active else 0)\
            .tag("machine", self.host_name)\
            .time(datetime.now(tz=timezone.utc), WritePrecision.NS)
        
        write_api = client.write_api(write_options=SYNCHRONOUS)

        write_api.write(bucket = self.influx_bucket, record = datum)
        self.log.debug("Report sent.")
    
    async def _periodic_report(self):
        self.log.info("Fetching kernel status...")
        reports = sorted(list(await self._get_kernel_status()), key=lambda x: x[2].timestamp(), reverse=True)
        if len(reports) > 0:
            self.log.info("%s Kernel(s) found. Last active:", len(reports))
            path, active, last_seen = reports[0]
            self.log.info(f"{path}\t{'active' if active else f'idle'}\t{last_seen}")
            currently_active = active or (datetime.now(tz=timezone.utc) - last_seen < timedelta(seconds=int(self.interval)*1.5))
        else:
            self.log.debug("No kernels found")
            currently_active = False
        
        try:
            self._report_to_influx(currently_active)
        except Exception as e:
            self.log.error(f"Failed to report: {e}")
    
    async def _get_kernel_status(self) -> list[tuple[str, bool, datetime]]:
        sm: SessionManager = self.serverapp.session_manager
        return [
            (
                session['path'],
                session['kernel']['execution_state'] == "busy",
                datetime.strptime(session['kernel']['last_activity'], "%Y-%m-%dT%H:%M:%S.%f%z")
            )
            for session in await sm.list_sessions() if session['type'] == "notebook"
        ]


main = launch_new_instance = GanymedServer.launch_instance
