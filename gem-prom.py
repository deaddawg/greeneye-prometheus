import argparse
import asyncio
from functools import partial
import logging
import socket
import sys

from greeneye.monitor import Monitors
from aioprometheus import Counter, Gauge, Service

DEFAULT_PORT_GEM = 1461
DEFAULT_PORT_PROM = 1462

LOG = logging.getLogger(__name__)


def register_prom_stats(prom_svr):
    stats = {}
    DEFAULT_LABELS = {"host": socket.gethostname()}

    # Counter for packets received from GEM
    gem_packets_rcvd = Counter(
        "gem_packets_rcvd", "Number of packets", const_labels=DEFAULT_LABELS
    )
    prom_svr.register(gem_packets_rcvd)
    stats['gem_packets_rcvd'] = gem_packets_rcvd

    # Gauge for voltage reading on GEM
    gem_ac_voltage = Gauge(
        "gem_ac_voltage", "AC Voltage", const_labels=DEFAULT_LABELS
    )
    prom_svr.register(gem_ac_voltage)
    stats['gem_ac_voltage'] = gem_ac_voltage

    # Gauge for circuit wattage reading on GEM
    gem_ac_power = Gauge(
        "gem_ac_power", "AC Watts", const_labels=DEFAULT_LABELS
    )
    prom_svr.register(gem_ac_power)
    stats['gem_ac_power'] = gem_ac_power

    return stats


async def gem(gem_port, prom_svr, prom_port):
    stats = register_prom_stats(prom_svr)
    await prom_svr.start(addr="0.0.0.0", port=prom_port)
    LOG.info(f"Serving prometheus metrics on: {prom_svr.metrics_url}")

    monitors = Monitors()
    monitors.add_listener(partial(on_new_monitor, stats))
    async with await monitors.start_server(gem_port):
        while True:
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break


def on_new_monitor(stats, monitor):
    monitor.add_listener(lambda: update_stats(stats, monitor))


def update_stats(stats, monitor):
    stats['gem_packets_rcvd'].inc({"serial": monitor.serial_number})
    stats['gem_ac_voltage'].set({"serial": monitor.serial_number}, monitor.voltage)

    LOG.debug("GEM {0}: Chan {1}, Pulse {2}, Temp {3}, Voltage: {4}".format(
        monitor.serial_number,
        len(monitor.channels),
        len(monitor.pulse_counters),
        len(monitor.temperature_sensors),
        monitor.voltage))

    for channel in monitor.channels:
        # Ignore channels over 32
        if channel.number >= 32:
            break

        # Log stats to gauge, change channel index to +1 to match hardware labels
        stats['gem_ac_power'].set({"serial": monitor.serial_number, "channel": channel.number + 1}, channel.watts)

        LOG.debug("Channel {0}: {1} W (abs={2} kWh, pol={3} kWh)".format(
            channel.number,
            channel.watts,
            channel.absolute_watt_seconds / 3600000,
            channel.polarized_watt_seconds / 3600000))

    # TODO: I need to add temp/pulse counters, don't care right now though

def _handle_debug(debug):
    """Turn on debugging if asked otherwise INFO default"""
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        format="[%(asctime)s] %(levelname)s: %(message)s (%(filename)s:%(lineno)d)",
        level=log_level,
    )

def main():
    parser = argparse.ArgumentParser(
        description="Export greeneye monitor stats for prometheus"
    )
    parser.add_argument(
        "-d", "--debug", action="store_true", help="Verbose debug output"
    )
    parser.add_argument(
        "-p",
        "--prom-port",
        type=int,
        default=DEFAULT_PORT_PROM,
        help=f"Port to run webserver on [Default = {DEFAULT_PORT_PROM}]",
    )
    parser.add_argument(
        "-g",
        "--gem-port",
        type=int,
        default=DEFAULT_PORT_GEM,
        help=f"Port to listen to GEM packets on [Default = {DEFAULT_PORT_GEM}]",
    )
    args = parser.parse_args()
    _handle_debug(args.debug)

    loop = asyncio.get_event_loop()

    prom_svr = Service()
    gem_task = asyncio.ensure_future(gem(args.gem_port, prom_svr, args.prom_port))

    try:
        loop.run_until_complete(gem_task)  
    except KeyboardInterrupt:
        gem_task.cancel()
        try:
            loop.run_until_complete(gem_task)
        except asyncio.CancelledError:
            pass
    finally:
        loop.run_until_complete(prom_svr.stop())
    loop.close()

if __name__ == "__main__":
    main()