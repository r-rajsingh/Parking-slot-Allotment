from flask import Flask
from flask_mysqldb import MySQL
import heapq

app = Flask(__name__)

# setting up the database configurations
app.config['MYSQL_HOST'] = 'localhost'          # database host
app.config['MYSQL_USER'] = 'root'               # database user
app.config['MYSQL_PASSWORD'] = ''               # database password
app.config['MYSQL_DB'] = 'VehicleDB'            # database name
app.config['MYSQL_CURSORCLASS'] = 'DictCursor'  # database cursor type

mysql = MySQL(app)

inputFile = open("input.txt", "r")
outputFile = open("output.txt", "w+")


'''
This method takes in the parking information as a String and
parks the current vehicle saving all the necessary information
in the Vehicle Table and assigns appropriate Slots.

@param      line        a String telling the vehicle number and driver age
            cur         a cursor denoting MySQL connection
            slotList    a heap of list of slots
@return     void
'''


def park(line, cur, slotList):
    vehicle, age = line.split()[1], int(line.split()[-1])
    slot = heapq.heappop(slotList)
    cur.execute("INSERT INTO Slots VALUES(%s, %s)", (slot, vehicle))
    cur.execute("INSERT INTO Vehicles VALUES(%s, %s)", (vehicle, age))
    outputFile.write("Car with vehicle registration number \"{}\" \
has been parked at slot number {}\n".format(vehicle, slot))


'''
This method takes in the slot information as a String and
removes the current vehicle from the Vehicles Table and emptying
the slot, thus updating the slot list.

@param      line        a String telling the slot number
            cur         a cursor denoting MySQL connection
            slotList    a heap of list of slots
@return     void
'''


def leave(line, cur, slotList):
    slot = int(line.split()[-1])
    cur.execute("SELECT Vehicle FROM Slots WHERE Slot = %s", (slot,))
    vehicle = cur.fetchall()[0]['Vehicle']
    cur.execute("SELECT Age FROM Vehicles WHERE Vehicle = %s", (vehicle,))
    age = cur.fetchall()[0]['Age']
    cur.execute("DELETE FROM Slots WHERE Slot = %s", (slot,))
    heapq.heappush(slotList, slot)
    outputFile.write("Slot number {} vacated, the car with vehicle \
registration number \"{}\" left the space, the driver of the \
car was of age {}\n".format(slot, vehicle, age))


'''
This method takes in the vehicle or driver information as a String and
retrieves the vehicle slot as a result.

@param      line    a String telling the vehicle number and \
                    driver age depending upon the query
            cur     a cursor denoting MySQL connection
@return     void
'''


def findSlot(line, cur):
    lineArr = line.split()
    if(lineArr[0] == "Slot_numbers_for_driver_of_age"):
        age = int(lineArr[1])
        cur.execute("SELECT Slot FROM Slots, Vehicles WHERE Vehicles.Age = %s \
            AND Vehicles.Vehicle=Slots.Vehicle", (age,))
    else:
        vehicle = lineArr[1]
        cur.execute("SELECT Slot FROM Slots WHERE Vehicle = %s", (vehicle,))

    tup = cur.fetchall()
    if(len(tup) == 0):
        outputFile.write("null\n")
    else:
        ans, flag = "", 0
        for i in tup:
            if(flag == 0):
                ans += str(i['Slot'])
                flag = 1
            else:
                ans += "," + str(i['Slot'])
        outputFile.write(ans+"\n")


'''
This method takes in the driver age information as a String and
retrieves the vehicle number as a result.

@param      line        a String telling the driver age
            cur         a cursor denoting MySQL connection
@return     void
'''


def findVehicle(line, cur):
    age = int(line.split()[-1])
    cur.execute("SELECT * FROM Vehicles WHERE Age = %s", (age,))

    tup = cur.fetchall()
    if(len(tup) == 0):
        outputFile.write("null\n")
    else:
        ans, flag = "", 0
        for i in tup:
            if(flag == 0):
                ans += str(i['Vehicle'])
                flag = 1
            else:
                ans += "," + str(i['Vehicle'])
        outputFile.write(ans+"\n")


'''
This is the main method that takes in input from "input.txt" and
saves the output in "output.txt"
'''


def main():
    with app.app_context():
        cur = mysql.connection.cursor()
        cur.execute("CREATE TABLE Vehicles (Vehicle varchar(20) PRIMARY KEY, \
            Age int NOT NULL)")
        cur.execute("CREATE TABLE Slots (Slot int PRIMARY KEY, \
            Vehicle varchar(20) NOT NULL, INDEX(Vehicle))")

        line = inputFile.readline()
        n = int(line.split()[-1])
        outputFile.write("Created parking of {} slots\n".format(n))
        slotList = [i for i in range(1, n+1)]
        heapq.heapify(slotList)

        while(line != ''):
            line = inputFile.readline()
            if(line == ''):
                break

            if(line[0] == 'P'):
                park(line, cur, slotList)
            elif(line[0] == 'L'):
                leave(line, cur, slotList)
            elif(line[0] == 'S'):
                findSlot(line, cur)
            elif(line[0] == 'V'):
                findVehicle(line, cur)
            else:
                print("No such command")

        mysql.connection.commit()
        cur.close()

    inputFile.close()
    outputFile.close()


if __name__ == "__main__":
    main()
