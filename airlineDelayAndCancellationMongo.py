#Referred http://zetcode.com/python/pymongo/
from pymongo import MongoClient

#Referred http://zetcode.com/python/pymongo/
client = MongoClient('mongodb://localhost:27017/')

#Referred http://zetcode.com/python/pymongo/
with client:
    
    #Referred http://zetcode.com/python/pymongo/
    db = client.airline
    
    #This helps the code understand how the user wants to interact with the application
    print("Please indicate what kind of search you want to perform : ")
    print("1. Route specific - Input Origin, Destination and Airline to get probability of delay and cancellation")
    print("2. Find out about the top performing airlines on the top 10 routes in United States")
    choice=input("Please select option (1 or 2)")
    
	#If user wants to perform route specific search for delay and cancellation
    if choice=='1':
    
        #Taking input from user for origin, destination and airline
        origin = input('Enter the airport of origin : ')
        destination = input('Enter the destination airport : ')
        carrier = input('Enter the airline : ')

        #Calculating the total number of flights on the specified route operated by the airline
        flightsOnRoute= db.Airline.count_documents({'OP_CARRIER': carrier,'ORIGIN': origin,'DEST': destination})

        #If airline operates flights on this route then enter the loop, otherwise go to the 'else' part
        if flightsOnRoute>0:
            print("Total Flights on this route : ", flightsOnRoute)

            #Calculating the probability of delayed flights on this route for the airline, delay is when a flight arrives late by 15 minutes or more
            delayedFlights= db.Airline.count_documents({'OP_CARRIER': carrier,'ORIGIN': origin,'DEST': destination,'ARR_DELAY':{'$gte':15}})
            print("Delayed Flights on this route : ", delayedFlights)
            delayProbability=delayedFlights/flightsOnRoute*100
            delayProbabilityString=str(delayProbability)
            print("Probability of delay on this route : "+delayProbabilityString+"%")

            #Calculating the probability of flight getting cancelled on this route for the airline
            cancelledFlights=db.Airline.count_documents({'OP_CARRIER': carrier,'ORIGIN': origin,'DEST': destination,'CANCELLED':1})
            print("Cancelled Flights on this route : ", cancelledFlights)
            cancellationProbability=cancelledFlights/flightsOnRoute*100
            cancellationProbabilityString=str(cancellationProbability)
            print("Probability of cancellation on this route : "+cancellationProbabilityString+"%")

            #Calculating the average delay time of flights that were late
            averageDelayAggregate = [{ '$match': {'$and': [ { 'ORIGIN': origin }, { 'DEST': destination }, { 'OP_CARRIER': carrier }, {'ARR_DELAY': {'$gte':15}} ]}}, { '$group': {'_id': 1, 'delayedFlightTotal': { '$sum': "$ARR_DELAY" } }}]
            averageDelayList = list(db.Airline.aggregate(averageDelayAggregate))
            totalDelayAllFlights=(averageDelayList[0]['delayedFlightTotal'])
            averageDelayOnRoute=totalDelayAllFlights/delayedFlights
            averageDelayOnRouteString=str(averageDelayOnRoute)
            print("Average Delay on this route is "+averageDelayOnRouteString+" minutes")

            #Method to calculate the probability of cancellation reason of flights
            def calculateProbabilityCancellationReason(cancelledFlights, cancelledFlightVariable) :
                if cancelledFlights>0 :
                    probabilityCancelledReason=cancelledFlightVariable/cancelledFlights*100
                else :
                    probabilityCancelledReason=0
                return probabilityCancelledReason

            #Calculating the probability of cancellation of flight due to Carrier or Airline from it's total Cancelled Flights
            cancelledFlightsA=db.Airline.count_documents({'OP_CARRIER': carrier,'ORIGIN': origin,'DEST': destination,'CANCELLED':1,'CANCELLATION_CODE':"A"})
            cancelledFlightsAProbability=calculateProbabilityCancellationReason(cancelledFlights, cancelledFlightsA)
            cancelledFlightsAProbabilityString=str(cancelledFlightsAProbability)
            print("Cancelled Flights due to Carrier or Airline : "+cancelledFlightsAProbabilityString+"%")

            #Calculating the probability of cancellation of flight due to Weather from it's total Cancelled Flights
            cancelledFlightsB=db.Airline.count_documents({'OP_CARRIER': carrier,'ORIGIN': origin,'DEST': destination,'CANCELLED':1,'CANCELLATION_CODE':"B"})
            cancelledFlightsBProbability=calculateProbabilityCancellationReason(cancelledFlights, cancelledFlightsB)
            cancelledFlightsBProbabilityString=str(cancelledFlightsBProbability)
            print("Cancelled Flights due to Weather : "+cancelledFlightsBProbabilityString+"%")

            #Calculating the probability of cancellation of flight due to National Air System from it's total Cancelled Flights
            cancelledFlightsC=db.Airline.count_documents({'OP_CARRIER': carrier,'ORIGIN': origin,'DEST': destination,'CANCELLED':1,'CANCELLATION_CODE':"C"})
            cancelledFlightsCProbability=calculateProbabilityCancellationReason(cancelledFlights, cancelledFlightsC)
            cancelledFlightsCProbabilityString=str(cancelledFlightsCProbability)
            print("Cancelled Flights due to National Air System : "+cancelledFlightsCProbabilityString+"%")

            #Calculating the probability of cancellation of flight due to Security reasons from it's total Cancelled Flights
            cancelledFlightsD=db.Airline.count_documents({'OP_CARRIER': carrier,'ORIGIN': origin,'DEST': destination,'CANCELLED':1,'CANCELLATION_CODE':"D"})
            cancelledFlightsDProbability=calculateProbabilityCancellationReason(cancelledFlights, cancelledFlightsD)
            cancelledFlightsDProbabilityString=str(cancelledFlightsDProbability)
            print("Cancelled Flights due to Security reasons : "+cancelledFlightsDProbabilityString+"%")

        #If no flights are operated on the route by the airline then display this message
        else:
            print("The airline doesn't operate on this route!")
    
    elif choice=='2':
        Top10 = db.Top10
        #Code to calculate top airlines in terms of on time performance on top ten routes in US
        #Top ten routes taken from this website - https://www.airfarewatchdog.com/blog/44259160/these-are-the-10-busiest-air-routes-in-the-u-s/
        print("The data for the top on time performing airlines on the top 10 routes in US is given below : ")
        
        print("1. Top on time performing flights in terms of percentage between Los Angeles(LAX) and New York(JFK) : ")
        x = Top10.find({"$and": [{'Origin':'JFK'},{'Destination':'LAX'}]}, {'Airline', 'Percentage'}).sort('Percentage',-1)
        for i in x:
            print(i)
            
        print("2. Top on time performing flights in terms of percentage between Los Angeles(LAX) and San Francisco(SFO) : ")
        x = Top10.find({"$and": [{'Origin':'LAX'},{'Destination':'SFO'}]}, {'Airline', 'Percentage'}).sort('Percentage',-1)
        for i in x:
            print(i)
            
        print("3. Top on time performing flights in terms of percentage between New York(LGA) and Chicago(ORD) : ")
        x = Top10.find({"$and": [{'Origin':'LGA'},{'Destination':'ORD'}]}, {'Airline', 'Percentage'}).sort('Percentage',-1)
        for i in x:
            print(i)
            
        print("4. Top on time performing flights in terms of percentage between Los Angeles(LAX) and Chicago(ORD) : ")
        x = Top10.find({"$and": [{'Origin':'LAX'},{'Destination':'ORD'}]}, {'Airline', 'Percentage'}).sort('Percentage',-1)
        for i in x:
            print(i)
            
        print("5. Top on time performing flights in terms of percentage between Atlanta(ATL) and Orlando(MCO) : ")
        x = Top10.find({"$and": [{'Origin':'ATL'},{'Destination':'MCO'}]}, {'Airline', 'Percentage'}).sort('Percentage',-1)
        for i in x:
            print(i)
            
        print("6. Top on time performing flights in terms of percentage between Las Vegas(LAS) and Los Angeles(LAX) : ")
        x = Top10.find({"$and": [{'Origin':'LAS'},{'Destination':'LAX'}]}, {'Airline', 'Percentage'}).sort('Percentage',-1)
        for i in x:
            print(i)
            
        print("7. Top on time performing flights in terms of percentage between Seattle(SEA) and Los Angeles(LAX) : ")
        x = Top10.find({"$and": [{'Origin':'LAX'},{'Destination':'SEA'}]}, {'Airline', 'Percentage'}).sort('Percentage',-1)
        for i in x:
            print(i)
            
        print("8. Top on time performing flights in terms of percentage between Denver(DEN) and Los Angeles(LAX) : ")
        x = Top10.find({"$and": [{'Origin':'DEN'},{'Destination':'LAX'}]}, {'Airline', 'Percentage'}).sort('Percentage',-1)
        for i in x:
            print(i)
            
        print("9. Top on time performing flights in terms of percentage between Atlanta(ATL) and New York(LGA) : ")
        x = Top10.find({"$and": [{'Origin':'ATL'},{'Destination':'LGA'}]}, {'Airline', 'Percentage'}).sort('Percentage',-1)
        for i in x:
            print(i)
        
        print("10. Top on time performing flights in terms of percentage between Atlanta(ATL) and Fort Lauderdale(FLL) : ")
        x = Top10.find({"$and": [{'Origin':'ATL'},{'Destination':'FLL'}]}, {'Airline', 'Percentage'}).sort('Percentage',-1)
        for i in x:
            print(i)
            
    else:
        print("Wrong choice!")