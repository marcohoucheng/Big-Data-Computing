from pyspark import SparkContext, SparkConf
from collections import defaultdict
import sys
import os
import random as rand 
import time
import statistics

def CountTriangles(edges):
	# Create a defaultdict to store the neighbors of each vertex
	neighbors = defaultdict(set)

	for edge in edges:
		u, v = edge
		neighbors[u].add(v)
		neighbors[v].add(u) 
	# Initialize the triangle count to zero
	triangle_count = 0
	# Iterate over each vertex in the graph.
	# To avoid duplicates, we count a triangle <u, v, w> only if u<v<w
	for u in neighbors:
		# Iterate over each pair of neighbors of u
		for v in neighbors[u]:
			if v > u:
				for w in neighbors[v]:
					# If w is also a neighbor of u, then we have a triangle
					if w > v and w in neighbors[u]:
						triangle_count += 1
	# Return the total number of triangles in the graph
	return triangle_count

def countTriangles2(colors_tuple, edges, rand_a, rand_b, p, num_colors):
    #We assume colors_tuple to be already sorted by increasing colors. Just transform in a list for simplicity
    colors = list(colors_tuple)
    #Create a dictionary for adjacency list
    neighbors = defaultdict(set)
    #Creare a dictionary for storing node colors
    node_colors = dict()
    for edge in edges:
        u, v = edge
        node_colors[u]= ((rand_a*u+rand_b)%p)%num_colors
        node_colors[v]= ((rand_a*v+rand_b)%p)%num_colors
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph
    for v in neighbors:
        # Iterate over each pair of neighbors of v
        for u in neighbors[v]:
            if u > v:
                for w in neighbors[u]:
                    # If w is also a neighbor of v, then we have a triangle
                    if w > u and w in neighbors[v]:
                        # Sort colors by increasing values
                        triangle_colors = sorted((node_colors[u], node_colors[v], node_colors[w]))
                        # If triangle has the right colors, count it.
                        if colors==triangle_colors:
                            triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count

#ALGORITHM 1
def MR_ApproxTCwithNodeColors(edges, C): # C number of partitions

	# Hash function
	p = 8191
	a = rand.randint(1, p-1)
	b = rand.randint(0, p-1)

	def hashf(u):
		return ((a*u + b) % p) % C
	
	def edge_partition(edge):
		u, v = edge
		if hashf(u) == hashf(v):
			Ci = hashf(u)
			return (Ci, edge)
	
	triangle_count = (edges.map(lambda f: edge_partition(f)) # <-- MAP PHASE (R1)
			  		 .filter(lambda x: x is not None)
					 .groupByKey() # <-- SHUFFLE+GROUPING
					 .mapValues(CountTriangles) # <-- REDUCE PHASE (R1)
					 .values().sum() # <-- REDUCE PHASE (R2)
					 )
	triangle_count = C**2 * triangle_count
	return triangle_count

#ALGORITHM 2
def MR_ExactTC(edges, C):

	# Hash function
	p = 8191
	a = rand.randint(1, p-1)
	b = rand.randint(0, p-1)

	def hashf(u):
		return ((a*u + b) % p) % C
	
	def key_list(edge, i):
		u, v = edge
		ki = tuple(sorted((hashf(u), hashf(v), i)))
		return (ki, edge) # tuple of tuples
	
	triangle_count = (edges.flatMap(lambda x: [key_list(x, i) for i in range(C)]) # <-- MAP PHASE (R1)
					 .groupByKey() # <-- SHUFFLE+GROUPING
					 .map(lambda x: countTriangles2(x[0], x[1], a, b, p, C)) # <-- REDUCE PHASE (R1)
					 .sum() # <-- REDUCE PHASE (R2)
	)
	return triangle_count

def main():
	# CHECKING NUMBER OF CMD LINE PARAMTERS
	assert len(sys.argv) == 5, "Usage: python G082HW2.py <C> <R> <rawData>"

	# SPARK SETUP
	conf = SparkConf().setAppName('G082HW2-Triangle-Counting')
	conf.set("spark.locality.wait", "0s")
	sc = SparkContext(conf=conf)

	# INPUT READING

	# 0. Read name of the file
	filename = sys.argv[0]

	# 1. Read number of partitions
	C = sys.argv[1]
	assert C.isdigit(), "C must be an integer"
	C = int(C)

	# 2. Read number of runs
	R = sys.argv[2]
	assert R.isdigit(), "R must be an integer"
	R = int(R)

	# 3. Read which algorith to run
	F = sys.argv[3]
	assert F.isdigit(), "F must be an integer"
	F = int(F)
	assert F == 0 or F == 1, "F must be equal to 0 or 1"

	# 4. Read input file and subdivide it into C random partitions
	data_path = sys.argv[4]
	# transform it into an RDD of edges (called edges), represented as pairs of integers
	# assert os.path.isfile(data_path), "File or folder not found"
	rawData = sc.textFile(data_path,minPartitions = 32)
	numedges = rawData.count()

	#transform RDD of edges - represented as pairs of integers
	edges = rawData.map(lambda x: (int(x.split(',')[0]), int(x.split(',')[1])))
	edges = edges.repartition(numPartitions = 32).cache()

	# F=0: runs only the approximation algorithm MR_ApproxTCwithNodeColors
	# F=1: runs only the exact algorithm MR_ExactTC

	if F == 0:
		# Run MR_ApproxTCwithNodeColors R times
		trianglecounts1 = []
		ts_alg1_times = []
		for i in range(R):
			## Algorithm 1 Implementation
			ts_alg1_before = time.time()
			trianglecount1 = MR_ApproxTCwithNodeColors(edges, C)
			ts_alg1_after = time.time()
			ts_alg1_time = (ts_alg1_after - ts_alg1_before) * 1000
			trianglecounts1.append(trianglecount1)
			ts_alg1_times.append(ts_alg1_time)

		#Calculate Mean MR_ApproxTCwithNodeColors Time	
		ts_alg1_times_mean = round(statistics.mean(ts_alg1_times))

		#Calculate Median MR_ApproxTCwithNodeColors Triangle Count
		trianglecounts1_median = round(statistics.median(trianglecounts1))

		# Print Output
		print("Dataset =",data_path, "\nNumber of Edges =", numedges, "\nNumber of Colors =", C,  \
			"\nNumber of Repetitions =", R, "\nApproximation algorithm with node coloring", \
			"\n- Number of triangles (median over", R, "runs) =", trianglecounts1_median, \
			"\n- Running time (average over", R, "runs) =", ts_alg1_times_mean, "ms")


	else:
		# Run MR_ExactTC R times
		trianglecounts2 = []
		ts_alg2_times = []
		for i in range(R):
			## Algorithm 2 Implementation
			ts_alg2_before = time.time()
			trianglecount2 = MR_ExactTC(edges, C)
			ts_alg2_after = time.time()
			ts_alg2_time = (ts_alg2_after - ts_alg2_before) * 1000
			trianglecounts2.append(trianglecount2)
			ts_alg2_times.append(ts_alg2_time)

		#Calculate Mean MR_ExactTC Time	
		ts_alg2_times_mean = round(statistics.mean(ts_alg2_times))

		# Print Output
		print("Dataset =",data_path, "\nNumber of Edges =", numedges, "\nNumber of Colors =", C,  \
			"\nNumber of Repetitions =", R, "\nExact algorithm with node coloring", \
			"\n- Number of triangles =", trianglecounts2[-1], \
			"\n- Running time (average over", R, "runs) =", ts_alg2_times_mean, "ms")


if __name__ == "__main__":

	main()