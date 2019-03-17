import sys
import json
import requests
import math

from cloudant.client import CouchDB
from cloudant.result import Result, ResultByKey
from cloudant.design_document import DesignDocument

EarthRadius = 6371.0

#Store the Airport, distance pair for the final evaluation
class Index:
    def __init__(self, name, distance):
        self.name = name
        self.distance = distance
    def Distance(self):
        return self.distance

#Make Angle abstract, because both Degrees and Radians are used in the code
class Angle:
    def __init__(self, **kwargs):
        if kwargs.get('degrees') != None:
            self.degrees = kwargs.get('degrees')
            return
        if kwargs.get('radians') != None:
            self.degrees = kwargs.get('radians') / math.pi * 180.0
            return
        self.degrees = 0.0
    def ToDegrees(self):
        return self.degrees
    def ToRadians(self):
        return self.degrees / 180.0 * math.pi
    def FromDegrees(self, degrees):
        self.degrees = degrees
    def FromRadians(self, radians):
        self.degrees = radians / math.pi * 180.0
    def __str__(self):
        return str(self.degrees)

#Holds a latitude longitude pair to identify a position
class Position:
    def __init__(self, latitude, longitude):
        self.lat = latitude
        self.lon = longitude

#Holds the input from the command line
class Input:
    def __init__(self):
        self.position = Position(Angle(), Angle())
        self.radius = 0.0 # store in Earth Radians

#Represents a bounding box, which encapsulates the circle defined by the input
class BoundingBox:
    def __init__(self, In):
        self.lat_from = Angle()
        self.lat_to = Angle()
        self.long_from = Angle()
        self.long_to = Angle()

        self.lat_from.FromRadians(In.position.lat.ToRadians() - In.radius)
        self.lat_to.FromRadians(In.position.lat.ToRadians() + In.radius)
        if (self.lat_from.ToRadians() < -math.pi / 2.0):
        # we are close to the south pole, it is within radius
            self.lat_from.FromRadians(-math.pi / 2.0)
            self.long_from.FromRadians(0.0) # we go full circle longitudinal
            self.long_to.FromRadians(2 * math.pi)
            return
        if (self.lat_to.ToRadians() > math.pi / 2.0):
        # we are close to the north pole, it is within radius        
            self.lat_to.FromRadians(math.pi / 2.0)
            self.long_from.FromRadians(0.0) # we go full circle longitudinal
            self.long_to.FromRadians(2 * math.pi)
            return
        transformedRadius = In.radius / math.cos(In.position.lat.ToRadians()) # transform the radius to the latitude of the input
        self.long_from.FromRadians(In.position.lon.ToRadians() - transformedRadius)
        self.long_to.FromRadians(In.position.lon.ToRadians() + transformedRadius)

#Responsible for DB communication
class DataBase:
    def __init__(self, urlstr):
        self.client = CouchDB(None, None, url=urlstr,
                     connect=True,auto_renew=True,admin_party=True,timeout=100)
        session = self.client.session()

    def Query(self, bb): 
        my_param = 'lon:[{0} TO {1}] AND lat:[{2} TO {3}]'.format(
            bb.long_from.ToDegrees(), bb.long_to.ToDegrees(),
            bb.lat_from.ToDegrees(), bb.lat_to.ToDegrees())

        print my_param

        end_point = '{0}/{1}'.format(self.client.server_url, 'airportdb/_design/view1/_search/geo')
        params = {'q': my_param, 'limit' : '200'}

        response = self.client.r_session.get(end_point, params=params)

        jsonResponse = response.json()

        numOfRows = jsonResponse['total_rows']

        print (str(numOfRows) + " hit in the DataBase");

        return jsonResponse

    def Close(self):
        self.client.disconnect()

#Responsible for printing help and parse and valide command line arguments
class ScriptHandler:
    @staticmethod
    def parseAndValidateArgs(In):
        #pos.lat
        try:
            In.position.lat.FromDegrees(float(sys.argv[1]))
        except:
            return 'Latitude has to be a number'
            exit()
        if ((In.position.lat.ToDegrees() < -90.0) or (In.position.lat.ToDegrees() > 90.0)):
            return 'Latitude has to be set between -90.0 and +90.0 in degrees.'

        #pos.long
        try:
            In.position.lon.FromDegrees(float(sys.argv[2]))
        except:
            return 'Longitude has to be a number'
            exit()
        if ((In.position.lon.ToDegrees() < 0.0) or (In.position.lon.ToDegrees() > 360.0)):
            return 'Longitude has to be set between 0.0 and 360.0 in degrees.'

        #radius
        try:
            In.radius = float(sys.argv[3]) / EarthRadius
        except:
            return 'Radius has to be a number'
            exit()
        if (In.radius <= 0.0):
            return 'Radius has to be greater than 0.0 in kilometers.'

    @staticmethod
    def printHelp():
        print("\nThis python program lists the airports within a user defined radius of a user defined position.\n")
        print("Usage:")
        print("  CloudantProj.py <pos.lat> <pos.long> <distance>\n")
        print("Where")
        print("  <pos.lat>: The latitude of the center in degrees. (-90.0 - 90.0)")
        print("  <pos.long>: The longitude of the center in degrees. (0.0 - 360.0)")
        print("  <distance>: The radius in kilometers.")

#Calculates distance between two Positions
class Compute:
    @staticmethod
    def CalculateDistance(pos1, pos2):
        fi = pos2.lat.ToRadians() - pos1.lat.ToRadians()
        la = pos2.lon.ToRadians() - pos1.lon.ToRadians()

        a = math.sin(fi / 2.0) * math.sin(fi / 2.0) + \
            math.cos(pos1.lat.ToRadians()) * math.cos(pos2.lat.ToRadians()) * \
            math.sin(la / 2.0) * math.sin(la / 2.0)
        c = 2 * math.atan2(math.sqrt(a),math.sqrt(1-a))

        return c

#main program
def main():
    # Parse and Validate input
    if len(sys.argv) != 4:
        ScriptHandler.printHelp()
        exit()
        
    In = Input()
    error = ScriptHandler.parseAndValidateArgs(In)
    
    if error != None:
        print(error)
        exit()
        
    #Compute bounding box
        
    bb = BoundingBox(In)
    
    #Query Database

    db = DataBase('https://mikerhodes.cloudant.com')
    jsonResponse = db.Query(bb)
    db.Close()

    #Compute Distances / drop which are too far

    List = []

    for i in jsonResponse['rows']:
        pos = Position(Angle(degrees = i['fields']['lat']), Angle(degrees = i['fields']['lon']))
        name = i['fields']['name']
        distance = Compute.CalculateDistance(pos, In.position)
        if distance < In.radius:    # only keep it, if it is really closer than Input radius
            List.append(Index(name, distance * EarthRadius))

    #Sort

    List.sort(key = Index.Distance)

    #Printout

    for i in List:
        print(i.name.encode("utf-8") + " : " + str(i.distance) + " km")

if __name__ == '__main__':
    main()

