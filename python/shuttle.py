'''
The Pitt API, to access workable data of the University of Pittsburgh
Copyright (C) 2015 Ritwik Gupta
This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.
You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
'''
from typing import Dict, Any
import requests

SESS = requests.session()

API_KEY = "8882812681"
BASE_URL = "https://cityofbaltimore.ridesystems.net"

def get_routes_for_map_with_schedule_with_encoded_line():
    """
    Used to retrieve active Routes from Ride Systems. Also contains a link to a schedule for each particular route

    :param api_key: Key used to authorize the request
    :type api_key: str

    :return RouteWithSchedule: List of dictionaries with the following information
        RouteID – Unique identifier for the route
        Description – Description of the route
        TextingKey – Unique string that represents the route when texted to Ride Systems
        MapLineColor – Color of the route on the map
        MapZoom – Default zoom level to set when this route is selected
        MapLatitude – Default latitude to set when this route is selected
        MapLongitude – Default longitude to set when this route is selected
        IsVisibleOnMap – Reflects whether this route should be shown on the main page
        IsCheckedOnMap –Reflects whether this route should be checked by default on the main page
        IsCheckLineOnlyOnMap– Does not show on app
        Order – Where this route falls in order with the other routes
        StopTimesPDFLink – Link to use to get the schedule for this route
        HideRouteLine – Setting on whether to show the route line
        ShowPolygon– Setting to show the route line as a polygon rather than a line
        InfoText– Subline text to use in the map panel for this route
        EncodedPolyline – Google encoded points that make up the polyline
        ETATypeID – What times do we show for the stops: 1) Estimates, 2) Schedules, or 3) SchedulesWithEstimates?
                    1. Estimates – Show the Estimate time (in time of day or minutes to arrival based on
                        EstimateDisplayType)
                    2. Schedules – Show the Scheduled time (in time of day or minutes to arrival based on
                        EstimateDisplayType)
                    3. Schedule With Estimates – Show the later of either the Schedule Time or Estimate time (in time of
                        day or minutes to arrival based on EstimateDisplayType)
        ShowRouteArrows– Setting on whether to show arrows on the Route Line
        UseScheduleTripsInPassengerCounter– Not used
        VehicleMarkerCssClass– CSS Class for Vehicle Marker
        Stops - Name of the stop
        RouteStopID– Unique Identifier for a Stop on a Route
        RouteID – Unique Identifier for a Route
        RouteDescription – Description of the Route
        Description – Description of the Stop
        Order – Order of this stop in comparison with the other stops.
        Heading – GPS Heading taken when the bus moves away from this stop.
        SecondsAtStop – Number of seconds the bus stays at this stop before moving to the next
        SecondsToNextStop – Number of seconds it takes the bus from move from this stop to the next.
        ShowEstimatesOnMap – Reflects setting on whether to show the estimates for this stop on the mapcpanel
        ShowDefaultedOnMap –Reflects whether to show this stop on the map
        TextingKey – Unique string that represents the Stop when texted to Ride Systems
        MaxZoomLevel –Reflects the zoom level to start showing this stop to the user
        SignVerbiage– Alternate Stop Description used by Sign Applications
        MapPoints– Not used
        Landmarks – Misc landmarks associated with this particular route
        LandmarkID – Unique identifier for the Landmark
        Label – Description to be put on the map
        AddressID – Unique ID associated with the address of this Landmark
        Latitude
        Longitude
    :type RouteWithSchedule: list
    """
    return _query('GetRoutesForMapWithScheduleWithEncodedLine')

def get_map_vehicle_points() -> Dict[str, Any]:
    """
    Return the map location for all active vehicles.

    :param api_key: Key used to authorize the request
    :type api_key: str

    :return vehicle: List of dictionaries containing the following information
        VehicleID—Unique Identifier for a Vehicle
        RouteID – Unique Identifier for a Route
        Name – Name of the Vehicle
        Latitude – Latitude of Vehicle’s current position
        Longitude – Longitude of Vehicle’s current position
        GroundSpeed – Speed of Vehicle
        Heading – Heading of Vehicle
        Seconds – Seconds since the vehicle reported its location
        IsOnRoute – Is the vehicle on Route?
        IsDelayed– Is the vehicle Delayed?
    :type vehicle: list
    """
    return _query('GetMapVehiclePoints')


def get_vehicle_route_stop_estimates(vehicle_id: list, quantity: int = 2) -> Dict[str, Any]:
    """Return {quantity} stop estimates for all active vehicles.

    :param vehicle_id: List of integers of Vehicle ID’s to retrieve
    :type vehicle_id: List
    :param quantity: Number of records to return.
    :type quantity: int

    :return vehicleestimates: The following information
        VehicleID—Unique Identifier for a Vehicle
        Estimates[] – Estimates
        RouteStopId – Unique ID of each Route Stop
        Description – Description of Route Stop
        OnRoute – Is the vehicle on Route?
        VehicleId – ID of Vehicle
        Text – Text of Estimate
        Time –Time of Day of expected arrival
        Seconds – Estimated Seconds until Arrival
        IsArriving – Is the vehicle arriving?
        EstimateTime – Estimated arrival Time of Day
        ScheduledTime – Scheduled arrival Time of Day
        OnTimeStatus– 0 – On time, 2 – Early, 3 – Late
    :return type: list of dictionaries

    """
    payload = {"quantity": str(quantity)}

    if vehicle_id:
        payload["vehicleIdStrings"] = ",".join(vehicle_id)
    return _query("GetVehicleRouteStopEstimates", payload)


def get_stop_arrival_times(times_per_stop: int = 1, route_ids: list = None, stop_ids: list = None) -> Dict[str, Any]:
    """
    Return stop arrival times for all vehicles.

    :param api_key: Key used to authorize the request
    :type api_key: str
    :param times_per_stop: Optional, number of scheduled times to return.
    :type times_per_stop: int
    :param route_ids: Optional, to restrict the results to given Route ID(s).
    :type route_ids: list of ints
    :param stop_ids: Optional, to restrict the results to given Stop ID(s)
    :typestop_ids: list of ints

    :return RouteStopArrival: List of dictionaries of the following information
        RouteID – Unique Identifier for a Route
        RouteStopID – Unique identifier for a Route Stop
        Description– Not used
        Times[] –Arrival Times
        VehicleId – ID of Vehicle
        Text – Text of Estimate
        Time –Time of Day of expected arrival
        Seconds – Estimated Seconds till Arrival
        IsArriving – Is the vehicle arriving?
        EstimateTime – Estimated arrival Time of Day
        ScheduledTime – Scheduled arrival or departure Time of Day
        IsDeparted– Has the vehicle departed?
        ScheduledArrivalTime– Scheduled arrival Time of Day
        ScheduledDepartureTime– Scheduled departure Time of Day
        OnTimeStatus– 0 – On time, 2 – Early, 3 – Late
    :type RouteStopArrival: list
    """

    payload = {"timesPerStop": times_per_stop}
    if route_ids:
        payload["routeIDs"] = ", ".join(route_ids)

    if stop_ids:
        payload["routeStopIDs"] = ", ".join(stop_ids)

    return _query("GetStopArrivalTimes", payload)

def get_route_stop_arrivals(times_per_stop: int = 1) -> Dict[str, Any]:
    """
    Return stop arrival times for all vehicles.

    :return:
    """

    payload = {"TimesPerStopString": times_per_stop}
    return _query("GetRouteStopArrivals", payload)

def get_route_schedules(route_id: int):
    """
    Used to return scheduled times for a Route. This is used for cyclical routes, where the route runs twice an hour on
    the exact same path and schedule.

    :param route_id: Optional, to restrict the results to a given Route ID
    :type route_id: int

    :return RouteSchedule:
        RouteScheduleID – Unique ID for each schedule
        RouteID – Unique Identifier for a Route
        StartTime – Time of day the Route Schedule starts
        StartTimeUTC – UTC Time of day the Route Schedule starts
        EndTime – Time of day the Route Schedule ends
        EndTimeUTC – UTC Time of day the Route Schedule ends
        LoopsPerHour – Number of loops run per hour
        StopTimes -
        RouteStopScheduleID – Unique ID for each stop on the schedule
        RouteScheduleID – Unique ID for the schedule
        RouteStopID – Unique Identifier for a Stop on a Route
        MinutesAfterStart – Number of minutes after the start of the loop until arrival at this stop
    :type RouteSchedule: List of dictionaries

    """
    payload = {"routeID": route_id} if route_id else {}
    return _query("GetRouteSchedules", payload)

def get_route_schedule_times(route_id: str = None):
    """
    Used to return times in the day that a Route is active.

    :param route_id: Optional, to restrict the results to a given Route ID.
    :type route_id: str

    :return RouteScheduleTime:
        RouteID – Unique Identifier for a Route
        StartTimeUTC – UTC time of day the Route Schedule starts
        EndTimeUTC – UTC time of day the Route Schedule ends
        StartTime – Time of day (in MST) the Route Schedule starts
        EndTime – Time of day (in MST) the Route Schedule ends
        ServerTime– Current time of day (in MST)
        ServerTimeUTC– UTC Current Time of day
    :type RouteScheduleTime: List of dictionaries
    """
    payload = {"routeID": route_id} if route_id else {}
    return _query("GetRouteScheduleTimes", payload)


def get_routes(route_id: str = None):
    """
    Abbreviated view of all active Routes on Ride Systems. Used for Smart Phones where data size is a limiting factor.

    :param route_id: Optional, to restrict the results to a given Route ID.
    :type route_id: str

    :return SmartPhoneRoute:
        RouteID – Unique Identifier for a Route
        Description – Description of the Route
        MapLineColor – Color of the Route on the Map
        MapZoom – Default Zoom level to set when this route is selected
        MapLatitude – Default Latitude to set when this route is selected
        MapLongitude – Default Longitude to set when this route is selected
        IsVisibleOnMap – Reflects if this route should be shown on the main page
        StopTimesPDFLink – Link to use to get the Schedule for this Route
        HideRouteLine – Setting on whether to show the Route Line
        UseScheduleTripsInPassengerCounter– Not used
    :type SmartPhoneRoute: List of dictionaries
    """
    payload = {"routeID": route_id} if route_id else {}
    return _query("GetRoutes", payload)


def get_stops(route_id: str = None):
    """
    Abbreviated view of all active Stops on a route. Used for Smart Phones where data size is a limiting factor.

    :param route_id: Optional, to restrict the results to a given Route ID.
    :type route_id: str

    :return SmartPhoneRouteStop:
        RouteStopID – Unique Identifier for a Stop on a Route
        RouteID – Unique Identifier for a Route
        Description – Description of the Stop
        Longitude
        Latitude
        TextingKey – Unique string that represents the Stop when Texted to Ride Systems
        MaxZoomLevel –Reflects the zoom level to start showing this stop to the user
        ShowEstimatesOnMap – Reflects setting on whether to show the estimates for this stop on the map
        panel
        ShowDefaultedOnMap –Reflects whether to show this stop on the map
        MapPoints – GPS points that make up the path to the next stop
        Latitude
        Longitude
        Heading- Not used
    :type SmartPhoneRouteStop: List of dictionaries
    """
    payload = {"routeID": route_id} if route_id else {}
    return _query("GetStops", payload)


def get_markers(route_id: str = None):
    """
    Abbreviated view of all active Landmarks on a route. Used for Smart Phones where data size is a limiting factor.

    :param route_id: Optional, to restrict the results to a given Route ID.
    :type route_id: str

    :return SmartPhoneLandmark:
        RouteID – Unique Identifier for a Route
        LandmarkID – Unique identifier for the Landmark
        Label – Description to be put on the map
        Latitude
        Longitude
    :type SmartPhoneLandmark: List of dictionaries
    """
    payload = {"routeID": route_id} if route_id else {}
    return _query("GetMarkers", payload)


def get_map_config():
    """
    Returns settings that are used for laying out the map

    :return MapConfig: See docs for return information
    :type Mapconfig: List of dictionaries
    """
    return _query("GetMapConfig")


def get_routes_for_map() -> Dict[str, Any]:
    """Return the routes with Vehicle Route Name, Vehicle ID, and all stops, etc."""
    return _query("GetRoutesForMap")

def get_ridership_data(start_date, end_date):
    """
    Return the ridership from an APC

    :param start_date: The date to begin the range (either in the format 2020-03-01 or 2020-03-01 1:00 PM)
    :type start_date: str
    :param end_date: The date to end the range (same format as start_date)
    :type end_date: str

    :return Ridership: The following data
        ClientTime
        Counter
        CounterType
        Entries
        Exits
        Latitude
        LongBadgeNumber
        Longitude
        Route
        RouteID
        RouteStop
        RouteStoopID
        ShortBadgeNumberUTC
        Time
        Vehicle
        VehicleID
    :type Ridership: List of dictionaries
    """
    payload = {"StartDate": start_date, "EndDate": end_date}
    return _query("GetRidershipData", payload)

def _query(method_name, params=None):
    payload = {"ApiKey": API_KEY}
    if params:
        payload.update(params)

    response = SESS.get("{}/Services/JSONPRelay.svc/{}".format(BASE_URL, method_name), params=payload)
    return response.json()
