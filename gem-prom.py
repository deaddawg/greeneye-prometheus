import argparse
import asyncio
from functools import partial
import logging
import socket
import sys
import yaml

from greeneye.monitor import Monitors
from aioprometheus import Counter, Gauge, Service

DEFAULT_PORT_GEM = 1461
DEFAULT_PORT_PROM = 1462
DEFAULT_CONFIG = "gem-config.yaml"

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


async def gem(gem_port, prom_svr, prom_port, config):
    stats = register_prom_stats(prom_svr)
    await prom_svr.start(addr="0.0.0.0", port=prom_port)
    LOG.info(f"Serving prometheus metrics on: {prom_svr.metrics_url}")

    monitors = Monitors()
    monitors.add_listener(partial(on_new_monitor, stats, config))
    async with await monitors.start_server(gem_port):
        while True:
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break


def on_new_monitor(stats, config, monitor):
    monitor.add_listener(lambda: update_stats(stats, config, monitor))


def update_stats(stats, config, monitor):
    # Inc counter for all packets, can use this to find new devices later if needed
    stats['gem_packets_rcvd'].inc({"serial": monitor.serial_number})

    # Stop here if device is not configured
    if monitor.serial_number not in config:
        LOG.debug(f"Got packet from GEM {monitor.serial_number}, but not found in config, ignoring.")
        return

    # Set default tags for all GEM stats
    location = config[monitor.serial_number].get("location")
    if not location:
        LOG.error(f"Missing location for {monitor.serial_number}")
    gem_tags = {"serial": monitor.serial_number, "location": location}

    # Log voltage reading
    stats['gem_ac_voltage'].set(gem_tags, monitor.voltage)
    LOG.debug(f"GEM {monitor.serial_number}: Voltage: {monitor.voltage}")

    # Go over each channel
    channel_config = config[monitor.serial_number].get('channels')
    for channel in monitor.channels:
        # Ignore channels over 32, hardware doesnt exist right now for this
        if channel.number >= 32:
            break

        # I want to use channel numbers that match hardware, but library is zero-indexed
        chan_num = channel.number + 1

        # Ignore channel if not configured
        if chan_num not in channel_config:
            continue

        # Get local channel tags
        chan_config_tags = channel_config.get(chan_num, {})
        if not chan_config_tags:
            LOG.error(f"Missing channel tags for {monitor.serial_number} channel {chan_num}")

        # Create per channel tags
        chan_tags = {**gem_tags, "channel": chan_num, **chan_config_tags}

        # Log stats to gauge
        stats['gem_ac_power'].set(chan_tags, channel.watts)

        LOG.debug("Channel {0}: {1} W (abs={2} kWh, pol={3} kWh)".format(
            chan_num,
            channel.watts,
            round(channel.absolute_watt_seconds / 3600000, 2),
            round(channel.polarized_watt_seconds / 3600000, 2)))

    # TODO: I need to add temp/pulse counters, don't care right now though

def _get_config(config):
    with open(config) as f:
        c =  yaml.load(f, Loader=yaml.FullLoader)
    LOG.debug(f"Config found: {c}")
    return c

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
    parser.add_argument(
        "-c",
        "--config",
        default=DEFAULT_CONFIG,
        help=f"Config file, yaml [Default = {DEFAULT_CONFIG}]",
    )
    args = parser.parse_args()
    _handle_debug(args.debug)

    try:
        config = _get_config(args.config)
    except FileNotFoundError:
        LOG.error(f"File {args.config} not found.")
        sys.exit(1)

    loop = asyncio.get_event_loop()

    prom_svr = Service()
    gem_task = asyncio.ensure_future(gem(args.gem_port, prom_svr, args.prom_port, config))

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