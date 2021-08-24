import os
import grpc
import json

from datetime import datetime

from app.udaconnect.models import Connection, Location
from app.udaconnect.schemas import (
    ConnectionSchema,
)
from shapely.geometry import Point
from geoalchemy2.shape import from_shape
from app.udaconnect.proto.connection_pb2 import ConnectionRequest, ConnectionData 
from app.udaconnect.proto.connection_pb2_grpc import ConnectionServiceStub
from flask import request
from flask_accepts import accepts, responds
from flask_restx import Namespace, Resource
from typing import Optional, List

DATE_FORMAT = "%Y-%m-%d"

api = Namespace("UdaConnect", description="Connections via geolocation.")  # noqa

GRPC_PORT = os.getenv('GRPC_PORT_CONNECTION', '7005')
GRPC_HOST = os.getenv('GRPC_HOST', 'localhost')

print(':'.join([GRPC_HOST, GRPC_PORT]))
GRPC_CHANNEL = grpc.insecure_channel(':'.join([GRPC_HOST, GRPC_PORT]), options=(('grpc.enable_http_proxy', 0),))
grpc_stub = ConnectionServiceStub(GRPC_CHANNEL)


@api.route("/persons/<person_id>/connection")
@api.param("start_date", "Lower bound of date range", _in="query")
@api.param("end_date", "Upper bound of date range", _in="query")
@api.param("distance", "Proximity to a given user in meters", _in="query")
class ConnectionDataResource(Resource):
    @responds(schema=ConnectionSchema, many=True)
    def get(self, person_id) -> ConnectionSchema:
        #start_date: datetime = datetime.strptime(request.args["start_date"], DATE_FORMAT)
        #end_date: datetime = datetime.strptime(request.args["end_date"], DATE_FORMAT)
        start_date = request.args["start_date"]
        end_date = request.args["end_date"]
        distance: Optional[int] = request.args.get("distance", 5)

        # Get from grpc server
        results = grpc_stub.Get(ConnectionRequest(
            person_id=int(person_id),
            start_date=start_date,
            end_date=end_date,
            meters=distance
        ))
        # Float timestamp needs to be converted to datetime here.
        # Could be improved
        ret_list = []
        for result in results.connections:
            loc = Location()
            loc.person_id = result.location.person_id
            loc.coordinate = from_shape(Point(result.location.longitude, result.location.latitude))
            loc.creation_time = datetime.fromtimestamp(result.location.creation_time)
            ret_list.append(Connection(location=loc, person=result.person))
        return ret_list
