"""
Tracks circulator activity on a real time basis by tracking start and stop times
ccc_bus_runtimes
busid  | starttime | endtime
"""
import pyodbc  # type: ignore
from typing import Dict

from ridesystems.api import API
from circulator.creds import RIDESYSTEMS_API_KEY

ROUTE_ID = {1: 'Banner',
            2: 'Purple',
            3: 'Orange',
            4: 'Green',
            10: 'Orange',
            11: 'Green',
            12: 'Banner',
            13: 'Purple'}

CONN = pyodbc.connect(r'Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')
CURSOR = CONN.cursor()


def get_active_buses() -> Dict[str, str]:
    """ Get the list of buses that were active the last time the script was run, and do no have an end time"""
    CURSOR.execute("SELECT busid, route FROM ccc_bus_runtimes WHERE endtime IS NULL")
    return {busid.strip(): route for (busid, route) in CURSOR.fetchall()}


def log_active_bus(bus_id: str, route_id: int) -> None:
    """
    Log a bus that is just starting its route

    :param bus_id: Bus identifier
    :param route_id: Route identifier
    """
    CURSOR.execute("INSERT INTO ccc_bus_runtimes (busid, starttime, route) VALUES (?, GETDATE(), ?)",
                   bus_id, ROUTE_ID[route_id])
    CURSOR.commit()


def log_inactive_bus(bus_id: str) -> None:
    """
    Log a bus that was active, and is no longer active

    :param bus_id: Bus identifier
    :type bus_id: str
    """
    CURSOR.execute("UPDATE ccc_bus_runtimes SET endtime = GETDATE() WHERE busid = (?) AND endtime IS NULL", bus_id)
    CURSOR.commit()


def process_vehicles() -> None:
    """Log the bus status in the database"""
    # Get list of open bus schedules
    active_buses = get_active_buses()

    rs_interface = API(RIDESYSTEMS_API_KEY)
    for map_vehicle_point in rs_interface.get_map_vehicle_points():
        if not map_vehicle_point['IsOnRoute']:
            continue

        bus_id: str = map_vehicle_point['Name']
        route_id: int = map_vehicle_point['RouteID']

        # it was already active, and is still active. Do nothing
        if bus_id in active_buses.keys():
            if active_buses[bus_id] != ROUTE_ID[route_id]:
                log_inactive_bus(bus_id)
                log_active_bus(bus_id, route_id)

            del active_buses[bus_id]
            continue

        # it was not previously active, so we need to add an entry
        log_active_bus(bus_id, route_id)

    # Any buses left in this list were not active, and should be ended
    for bus_id in active_buses:
        log_inactive_bus(bus_id)
