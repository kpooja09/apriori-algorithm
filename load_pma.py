###
__author__ :"Pooja Kamble"
__version__:"1.1"
###

import traceback
import psycopg2
import csv
import subprocess


#returns a database connection to db server
def init_connection():
	usr = "kpooja"
	pwd= "Test@1234"
	h ="127.0.0.1"
	p = "5432"
	db ="assign4"

	try:
	    connection = psycopg2.connect(user = usr,
	    	password = pwd,
	    	host = h,
	    	port = p,
	    	database = db)
	    connection.autocommit = True;
	    return connection
	    
	except (Exception, psycopg2.Error) as error :
	    print ("Error while connecting to PostgreSQL", error)
	
	return None

#main function creates Popular_Movie_Actors relation
def main():

	lattice_level = 3

	cnt = 1

	connection = init_connection()
	cursor = connection.cursor()

	print("creating Popular_Movie_Actors relation... ")

	query = """CREATE TABLE Popular_Movie_Actors AS
			SELECT movie_actor.movie as movie_id, ARRAY_AGG(movie_actor.actor)  as actors
			FROM movie_actor
			JOIN movie ON movie_actor.movie = movie.id
			WHERE movie.type = 'movie' and movie.avgRating > 5
			GROUP BY movie_actor.movie"""

	print("Popular_Movie_Actors relation created! ")
	cursor.execute(query)

	cursor.close()
	connection.commit()
	connection.close()


if __name__ == '__main__':
	main()

