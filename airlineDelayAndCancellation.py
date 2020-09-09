#Source of mySQL Connector : https://mariadb.com/resources/blog/how-to-connect-python-programs-to-mariadb/

import mysql.connector
from mysql.connector import Error


try:
    connection = mysql.connector.connect(host='52.14.123.244',
                                         database='Airlinedata',
                                         user='admin',
                                         password='admin')	#Host is the public IP address of a node in the cluster through which we connect using this code on another machine
    
	#This helps the code understand how the user wants to interact with the application
    print("Please indicate what kind of search you want to perform : ")
    print("1. Route specific - Input Origin, Destination and Airline to get probability of delay and cancellation")
    print("2. Find out about the top performing airlines on the top 10 routes in United States")
    choice=input("Please select option (1 or 2)")
    
	#If user wants to perform route specific search for delay and cancellation
    if choice=='1':
    
        #Taking input from user for origin, destination and airline
        origin=input("Enter the airport of origin : ")
        destination=input("Enter the destination airport : ")
        airline=input("Enter the airline : ")
    
        #Calculating the total number of flights on the specified route operated by the airline
        sql_select_fullCount_Query= "select count(Serial_Number) from Airline where Origin = '"+origin+"' AND Destination = '"+destination+"' AND Carrier='"+airline+"'"
        cursor = connection.cursor()
        cursor.execute(sql_select_fullCount_Query)
        records = cursor.fetchall()
        for row in records:
            totalFlights=row[0]
        print("Total Flights on this route : ", totalFlights)
        
        #If airline operates flights on this route then enter the loop, otherwise go to the 'else' part
        if totalFlights>0 :
        
            #Calculating the probability of delayed flights on this route for the airline, delay is when a flight arrives late by 15 minutes or more
            sql_select_delayCount_Query= "select count(Serial_Number) from Airline where Origin = '"+origin+"' AND Destination = '"+destination+"' AND Carrier='"+airline+"' AND Arrival_Delay >= 15"
            cursor = connection.cursor()
            cursor.execute(sql_select_delayCount_Query)
            records = cursor.fetchall()
            for row in records:
                delayedFlights=row[0]
            print("Delayed Flights on this route : ", delayedFlights)
            lateProbability=delayedFlights/totalFlights*100
            lateProbabilityString=str(lateProbability)
            print("Probability of delay on this route : "+lateProbabilityString+"%")
        
            #Calculating the probability of flight getting cancelled on this route for the airline
            sql_select_cancellationCount_Query= "select count(Serial_Number) from Airline where Origin = '"+origin+"' AND Destination = '"+destination+"' AND Carrier='"+airline+"' AND Cancelled = 1"
            cursor = connection.cursor()
            cursor.execute(sql_select_cancellationCount_Query)
            records = cursor.fetchall()
            for row in records:
                cancelledFlights=row[0]
            print("Cancelled Flights on this route : ", cancelledFlights)
            cancellationProbability=cancelledFlights/totalFlights*100
            cancellationProbabilityString=str(cancellationProbability)
            print("Probability of cancellation on this route : "+cancellationProbabilityString+"%")
        
            #Calculating the average delay time of flights that were late
            sql_select_avgDelay_Query= "select avg(Arrival_Delay) from Airline where Origin = '"+origin+"' AND Destination = '"+destination+"' AND Carrier='"+airline+"' AND Arrival_Delay >= 15"
            cursor = connection.cursor()
            cursor.execute(sql_select_avgDelay_Query)
            records = cursor.fetchall()
            for row in records:
                avgDelay=row[0]
            avgDelayString=str(avgDelay)
            print("Average Delay on this route is "+avgDelayString+" minutes")
        
            #Method to calculate the probability of cancellation reason of flights
            def calculateProbabilityCancellationReason(cancelledFlights, cancelledFlightVariable) :
                if cancelledFlights>0 :
                    probabilityCancelledReason=cancelledFlightVariable/cancelledFlights*100
                else :
                    probabilityCancelledReason=0
                return probabilityCancelledReason
        
            #Calculating the probability of cancellation of flight due to Carrier or Airline from it's total Cancelled Flights
            sql_select_cancellationCountA_Query= "select count(Serial_Number) from Airline where Origin = '"+origin+"' AND Destination = '"+destination+"' AND Carrier='"+airline+"' AND Cancelled = 1 AND Cancellation_Code='A'"
            cursor = connection.cursor()
            cursor.execute(sql_select_cancellationCountA_Query)
            records = cursor.fetchall()
            for row in records:
                cancelledFlightsA=row[0]
            cancelledFlightsAProbability=calculateProbabilityCancellationReason(cancelledFlights, cancelledFlightsA)
            cancelledFlightsAProbabilityString=str(cancelledFlightsAProbability)
            print("Cancelled Flights due to Carrier or Airline : "+cancelledFlightsAProbabilityString+"%")
        
            #Calculating the probability of cancellation of flight due to Weather from it's total Cancelled Flights
            sql_select_cancellationCountB_Query= "select count(Serial_Number) from Airline where Origin = '"+origin+"' AND Destination = '"+destination+"' AND Carrier='"+airline+"' AND Cancelled = 1 AND Cancellation_Code='B'"
            cursor = connection.cursor()
            cursor.execute(sql_select_cancellationCountB_Query)
            records = cursor.fetchall()
            for row in records:
                cancelledFlightsB=row[0]
            cancelledFlightsBProbability=calculateProbabilityCancellationReason(cancelledFlights, cancelledFlightsB)
            cancelledFlightsBProbabilityString=str(cancelledFlightsBProbability)
            print("Cancelled Flights due to Weather : "+cancelledFlightsBProbabilityString+"%")
        
            #Calculating the probability of cancellation of flight due to National Air System from it's total Cancelled Flights
            sql_select_cancellationCountC_Query= "select count(Serial_Number) from Airline where Origin = '"+origin+"' AND Destination = '"+destination+"' AND Carrier='"+airline+"' AND Cancelled = 1 AND Cancellation_Code='C'"
            cursor = connection.cursor()
            cursor.execute(sql_select_cancellationCountC_Query)
            records = cursor.fetchall()
            for row in records:
                cancelledFlightsC=row[0]
            cancelledFlightsCProbability=calculateProbabilityCancellationReason(cancelledFlights, cancelledFlightsC)
            cancelledFlightsCProbabilityString=str(cancelledFlightsCProbability)
            print("Cancelled Flights due to National Air System : "+cancelledFlightsCProbabilityString+"%")

            #Calculating the probability of cancellation of flight due to Security reasons from it's total Cancelled Flights
            sql_select_cancellationCountD_Query= "select count(Serial_Number) from Airline where Origin = '"+origin+"' AND Destination = '"+destination+"' AND Carrier='"+airline+"' AND Cancelled = 1 AND Cancellation_Code='D'"
            cursor = connection.cursor()
            cursor.execute(sql_select_cancellationCountD_Query)
            records = cursor.fetchall()
            for row in records:
                cancelledFlightsD=row[0]
            cancelledFlightsDProbability=calculateProbabilityCancellationReason(cancelledFlights, cancelledFlightsD)
            cancelledFlightsDProbabilityString=str(cancelledFlightsDProbability)
            print("Cancelled Flights due to Security reasons : "+cancelledFlightsDProbabilityString+"%")

        #If no flights are operated on the route by the airline then display this message
        else :
            print("The airline doesn't operate on this route!")

        #Calculation of total number of flights operated by all airlines in US
        sql_select_totalFlightsOperated_Query = "select count(Serial_Number) from Airline"
        cursor = connection.cursor()
        cursor.execute(sql_select_totalFlightsOperated_Query)
        records = cursor.fetchall()
        for row in records:
            totalFlightsOperated=row[0]
        print("Total flights scheduled to operate in US in 2017-2018 were : ", totalFlightsOperated)

        #Calculation of percentage of delayed flights all over US, a flight is considered delayed when it reaches its destination 15 minutes or more late
        sql_select_totalFlightsDelayed_Query = "select count(Serial_Number) from Airline where Arrival_Delay >= 15"
        cursor = connection.cursor()
        cursor.execute(sql_select_totalFlightsDelayed_Query)
        records = cursor.fetchall()
        for row in records:
            totalFlightsDelayed=row[0]
        percentDelayed = totalFlightsDelayed/totalFlightsOperated*100
        print("The percentage of flights delayed in US in 2017-2018 was : ", percentDelayed)

        #Calculation of percentage of flights cancelled all over US
        sql_select_totalFlightsCancelled_Query = "select count(Serial_Number) from Airline where Cancelled = 1"
        cursor = connection.cursor()
        cursor.execute(sql_select_totalFlightsCancelled_Query)
        records = cursor.fetchall()
        for row in records:
            totalFlightsCancelled=row[0]
        percentCancelled = totalFlightsCancelled/totalFlightsOperated*100
        print("The percentage of flights cancelled in US in 2017-2018 was : ", percentCancelled)

        #Calculation of average delay time for all flights in US
        sql_select_avgDelayTimeAllFlights_Query= "select avg(Arrival_Delay) from Airline where Arrival_Delay>=15"
        cursor = connection.cursor()
        cursor.execute(sql_select_avgDelayTimeAllFlights_Query)
        records = cursor.fetchall()
        for row in records:
            avgDelayTimeAllFlights=row[0]
        avgDelayTimeAllFlightsString=str(avgDelayTimeAllFlights)
        print("The flights delayed in US in 2017-2018 were late by an average of "+avgDelayTimeAllFlightsString+" minutes")
        
    #When user selects the option to view information about top 10 routes    
    elif choice=='2':
        
        #Code to calculate top airlines in terms of on time performance on top ten routes in US
        #Top ten routes taken from this website - https://www.airfarewatchdog.com/blog/44259160/these-are-the-10-busiest-air-routes-in-the-u-s/
        print("The data for the top on time performing airlines on the top 10 routes in US is given below : ")
        
        print("1. Top on time performing flights in terms of percentage between Los Angeles(LAX) and New York(JFK) : ")
        sql_select_LAXJFK_Query= "select Carrier, OTP_Percentage from TopRoutesOTP where ((Origin='LAX' AND Destination='JFK') OR (Origin='JFK' AND Destination='LAX')) ORDER BY OTP_Percentage DESC"
        cursor = connection.cursor()
        cursor.execute(sql_select_LAXJFK_Query)
        records = cursor.fetchall()
        for row in records:
            print(row[0],"     ",row[1])
            
        print("2. Top on time performing flights in terms of percentage between Los Angeles(LAX) and San Francisco(SFO) : ")
        sql_select_LAXSFO_Query= "select Carrier, OTP_Percentage from TopRoutesOTP where ((Origin='LAX' AND Destination='SFO') OR (Origin='SFO' AND Destination='LAX')) ORDER BY OTP_Percentage DESC"
        cursor = connection.cursor()
        cursor.execute(sql_select_LAXSFO_Query)
        records = cursor.fetchall()
        for row in records:
            print(row[0],"     ",row[1])
            
        print("3. Top on time performing flights in terms of percentage between New York(LGA) and Chicago(ORD) : ")
        sql_select_LGAORD_Query= "select Carrier, OTP_Percentage from TopRoutesOTP where ((Origin='LGA' AND Destination='ORD') OR (Origin='ORD' AND Destination='LGA')) ORDER BY OTP_Percentage DESC"
        cursor = connection.cursor()
        cursor.execute(sql_select_LGAORD_Query)
        records = cursor.fetchall()
        for row in records:
            print(row[0],"     ",row[1])
            
        print("4. Top on time performing flights in terms of percentage between Los Angeles(LAX) and Chicago(ORD) : ")
        sql_select_LAXORD_Query= "select Carrier, OTP_Percentage from TopRoutesOTP where ((Origin='LAX' AND Destination='ORD') OR (Origin='ORD' AND Destination='LAX')) ORDER BY OTP_Percentage DESC"
        cursor = connection.cursor()
        cursor.execute(sql_select_LAXORD_Query)
        records = cursor.fetchall()
        for row in records:
            print(row[0],"     ",row[1])
        
        print("5. Top on time performing flights in terms of percentage between Atlanta(ATL) and Orlando(MCO) : ")
        sql_select_ATLMCO_Query= "select Carrier, OTP_Percentage from TopRoutesOTP where ((Origin='ATL' AND Destination='MCO') OR (Origin='MCO' AND Destination='ATL')) ORDER BY OTP_Percentage DESC"
        cursor = connection.cursor()
        cursor.execute(sql_select_ATLMCO_Query)
        records = cursor.fetchall()
        for row in records:
            print(row[0],"     ",row[1])
            
        print("6. Top on time performing flights in terms of percentage between Las Vegas(LAS) and Los Angeles(LAX) : ")
        sql_select_LASLAX_Query= "select Carrier, OTP_Percentage from TopRoutesOTP where ((Origin='LAS' AND Destination='LAX') OR (Origin='LAX' AND Destination='LAS')) ORDER BY OTP_Percentage DESC"
        cursor = connection.cursor()
        cursor.execute(sql_select_LASLAX_Query)
        records = cursor.fetchall()
        for row in records:
            print(row[0],"     ",row[1])
            
        print("7. Top on time performing flights in terms of percentage between Seattle(SEA) and Los Angeles(LAX) : ")
        sql_select_SEALAX_Query= "select Carrier, OTP_Percentage from TopRoutesOTP where ((Origin='SEA' AND Destination='LAX') OR (Origin='LAX' AND Destination='SEA')) ORDER BY OTP_Percentage DESC"
        cursor = connection.cursor()
        cursor.execute(sql_select_SEALAX_Query)
        records = cursor.fetchall()
        for row in records:
            print(row[0],"     ",row[1])
            
        print("8. Top on time performing flights in terms of percentage between Denver(DEN) and Los Angeles(LAX) : ")
        sql_select_DENLAX_Query= "select Carrier, OTP_Percentage from TopRoutesOTP where ((Origin='DEN' AND Destination='LAX') OR (Origin='LAX' AND Destination='DEN')) ORDER BY OTP_Percentage DESC"
        cursor = connection.cursor()
        cursor.execute(sql_select_DENLAX_Query)
        records = cursor.fetchall()
        for row in records:
            print(row[0],"     ",row[1])
            
        print("9. Top on time performing flights in terms of percentage between Atlanta(ATL) and New York(LGA) : ")
        sql_select_ATLLGA_Query= "select Carrier, OTP_Percentage from TopRoutesOTP where ((Origin='ATL' AND Destination='LGA') OR (Origin='LGA' AND Destination='ATL')) ORDER BY OTP_Percentage DESC"
        cursor = connection.cursor()
        cursor.execute(sql_select_ATLLGA_Query)
        records = cursor.fetchall()
        for row in records:
            print(row[0],"     ",row[1])
            
        print("10. Top on time performing flights in terms of percentage between Atlanta(ATL) and Fort Lauderdale(FLL) : ")
        sql_select_ATLFLL_Query= "select Carrier, OTP_Percentage from TopRoutesOTP where ((Origin='ATL' AND Destination='FLL') OR (Origin='FLL' AND Destination='ATL')) ORDER BY OTP_Percentage DESC"
        cursor = connection.cursor()
        cursor.execute(sql_select_ATLFLL_Query)
        records = cursor.fetchall()
        for row in records:
            print(row[0],"     ",row[1])
    
    #When user selects the wrong choice
    else:
        print("Wrong choice!")
            
            
#When there is an error reading from the database print the following statement
except Error as e:
    print("Error reading data from MySQL table", e)
#Closing the connection
finally:
    if (connection.is_connected()):
        connection.close()
        cursor.close()
        #print("MySQL connection is closed")