#####Get Data#####
#import csv files
import pandas as pd
Tickets_import=pd.read_csv('Tickets.csv')
Codes_import=pd.read_csv('Airport_Codes.csv')
Flights_import=pd.read_csv('Flights.csv',low_memory=False)

#####clean and prepare data#####

##AIRPORT CODE DATA##
#find Airports with missing codes and a 3 letter name, assume this code is supposed to be in IATA_CODE field 
###NOTE: for this particular data set this manipulation turned out to be unnecessary but I'm leaving code here for future use
Codes_import.loc[(Codes_import['NAME'].str.len() == 3) & (Codes_import['IATA_CODE'].isnull()),'IATA_CODE'] = Codes_import['NAME'].str.upper()
#Drop duplicated IATA_CODEs
Codes_import = Codes_import.drop_duplicates(subset=['IATA_CODE'])
#drop rows with missing IATA codes
Codes_import = Codes_import[Codes_import['IATA_CODE'].notna()]
#no need to keep these columns
Codes = Codes_import.drop(['MUNICIPALITY', 'ELEVATION_FT','CONTINENT','COORDINATES'], 1)


##TICKETS DATA##
import numpy as np
#replace NaN with 0 (only found in passengers and ITIN_Fare columns) 
Tickets_import = Tickets_import.fillna(0) 
#add a column to hold the 'ORIGIN', 'DESTINATION', REPORTING_CARRIER codes as one field to make aggregation easier
Tickets_import["ITINERARY_KEY"]= Tickets_import["ORIGIN"] + Tickets_import["DESTINATION"] + Tickets_import["REPORTING_CARRIER"]
# remove non numeric characters from ITIN_FARE
Tickets_import['ITIN_FARE'] = Tickets_import['ITIN_FARE'].str.replace(r'[^0-9]+', '', regex=True).replace('', np.nan)
#Re-type ITIN_FARE column
Tickets_import['ITIN_FARE'] = Tickets_import['ITIN_FARE'].astype(float) 
#Remove duplicate data
Tickets=Tickets_import.drop_duplicates()

##FLIGHTS DATA##
#Remove duplicate rows
Flights_import = Flights_import.drop_duplicates()

# remove non-numeric characters from DISTANCE and AIR_TIME
Flights_import['DISTANCE'] = Flights_import['DISTANCE'].str.replace(r'[^0-9]+', '', regex=True).replace('', np.nan)
Flights_import['AIR_TIME'] = Flights_import['AIR_TIME'].str.replace(r'[^0-9]+', '', regex=True).replace('', np.nan)

#convert to DISTANCE and AIR_TIME float
Flights_import['DISTANCE'] = Flights_import['DISTANCE'].astype(float) 
Flights_import['AIR_TIME'] = Flights_import['AIR_TIME'].astype(float) 

#add a column to hold the 'ORIGIN', 'DESTINATION', OP_CARRIER codes as one field to make aggregation easier
Flights_import["ITINERARY_KEY"]= Flights_import["ORIGIN"] + Flights_import["DESTINATION"]+Flights_import["OP_CARRIER"]

#replace NaN with 0 -- ARR_DELAY, DISTANCE, AIRTIME, OCCUPANCY_RATE
#ASSUMPTION that a null ARR_DELAY means no delay and null DISTANCE, AIRTIME, OCCUPANCY_RATE mean they are unknown but 0 is easier to work with. This will affect calcuations later
Flights = Flights_import.fillna(0) 


#load data 
import sqlite3 as db    
conn = db.connect('airline.sqlite')
Flights.to_sql('flights',conn, if_exists='replace', index=False)
Codes.to_sql('codes',conn, if_exists='replace', index=False)
Tickets.to_sql('tickets',conn, if_exists='replace', index=False)

cur = conn.cursor()
#check counts
result = cur.execute("select count(*) from flights").fetchall() 
print("Flights row count: ", result[0][0], "DF row count: ", Flights.shape[0])

result = cur.execute("select count(*) from codes").fetchall() 
print("Codes row count: ", result[0][0] , "DF row count: ", Codes.shape[0])
    
result = cur.execute("select count(*) from tickets").fetchall() 
print("Tickets row count: ", result[0][0] , "DF row count: ", Tickets.shape[0])
conn.close()
    
    
