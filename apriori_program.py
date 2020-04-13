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
	usr = "user"
	pwd= "password"
	h ="127.0.0.1"
	p = "5432"
	db ="db"

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


# This is a generic method for generating queries for all level of lattice
def generate_query(k=3):

	m = str(k-1)
	c_param = ''
	p_param = ''

	for i in range(1,k):
		c_param += "c"+m+".actor"+str(i) +", "
		p_param += "p.actor"+str(i) +", "
	c_param += "c"+m+".actor"+str(k) 
	group_param = c_param
	c_param += ", count(*) as frequency FROM "
	p_param += "q.actor"+m +" as actor"+str(k)+" FROM L"+m+" as p, L"+m+" as q "
	
	comp_param = ""
	for i in range(1,k-1):
		comp_param += "p.actor"+str(i)+" = q.actor"+str(i)+" and "
	comp_param += "p.actor"+m+" < q.actor"+m + ")c"+m+ " JOIN popular_movie_actors as pma ON "
	
	join_cond = "ARRAY["
	for i in range(1,k+1):
		join_cond += "c"+m+".actor"+str(i) + ", "
	join_cond = join_cond[:-2]
	join_cond += "]"

	#This is where final query will be generated
	cmd = "CREATE TABLE L"+ str(k) + " AS SELECT "+c_param + "(SELECT " + p_param + "WHERE " + comp_param + join_cond+" <@ pma.actors GROUP BY "+ group_param +" HAVING count(*) >= 5 "

	return cmd

#main function
def main():

	lattice_level = 2
	cnt = 1

	connection = init_connection()
	cursor = connection.cursor()

	#This creates the level 1 of lattice
	query = """CREATE TABLE L1 AS SELECT actor as actor1, count(movie_id) as frequency
				FROM (SELECT movie_id, unnest(actors) as actor FROM popular_movie_actors)tmp
				GROUP BY actor
				HAVING count(movie_id)  >= 5"""


	cursor.execute(query)

	#this loop creates all the lattice untill its empty
	while cnt > 0:
		#get query for given lattice level
		query = generate_query(lattice_level)
		print(query)

		#create lattice level
		cursor.execute(query)
		print("Lattice for level ", lattice_level, "is created!!")

		query = "SELECT count(*) FROM L"+str(lattice_level)

		#Check if created lattice i emoty or not
		cursor.execute(query)
		cnt = cursor.fetchone()

		cnt = cnt[0]

		lattice_level += 1

	cursor.close()
	connection.commit()
	connection.close()


if __name__ == '__main__':
	main()
















	