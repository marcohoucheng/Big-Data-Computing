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
def MR_ApproxTCwithSparkPartitions(edges, C):

	triangle_count = (edges.mapPartitions(lambda f: [CountTriangles(f)]) # <-- MAP + REDUCE PHASE (R1)
					 .sum()  # <-- REDUCE PHASE (R2)
					 )
	triangle_count = C**2 * triangle_count
	return triangle_count


def main():
	# CHECKING NUMBER OF CMD LINE PARAMTERS
	assert len(sys.argv) == 4, "Usage: python G082HW1.py <C> <R> <rawData>"

	# SPARK SETUP
	conf = SparkConf().setAppName('HW1-Triangle-Counting')
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

	# 3. Read input file and subdivide it into C random partitions
	data_path = sys.argv[3]
	# transform it into an RDD of edges (called edges), represented as pairs of integers
	assert os.path.isfile(data_path), "File or folder not found"
	rawData = sc.textFile(data_path,minPartitions=C)
	numedges = rawData.count()

	#transform RDD of edges - represented as pairs of integers
	edges = rawData.map(lambda x: (int(x.split(',')[0]), int(x.split(',')[1]))).cache()
	edges = edges.repartition(numPartitions = C)

	# Run Algorithm 1 R times
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

	#Calculate Mean Algorithm 1 Time	
	ts_alg1_times_mean = round(statistics.mean(ts_alg1_times))

	#Calculate Median Algorithm 1 Triangle Count
	trianglecounts1_median = round(statistics.median(trianglecounts1))

	## Algorithm 2 Implementation
	ts_alg2_before = time.time()
	trianglecount2 = MR_ApproxTCwithSparkPartitions(edges, C)
	ts_alg2_after = time.time()
	ts_alg2_time = round((ts_alg2_after - ts_alg2_before) * 1000)

	# Print Output
	print("Dataset =",data_path, "\nNumber of Edges =", numedges, "\nNumber of Colors =", C,  \
		"\nNumber of Repetitions =", R, "\nApproximation through node coloring", \
		"\n- Number of triangles (median over", R, "runs) =", trianglecounts1_median, "\n- Running time (average over", R, "runs) =", ts_alg1_times_mean, "ms",\
		"\nApproximation through Spark partitions", "\n- Number of triangles =", trianglecount2, \
		"\n- Running time =", ts_alg2_time, "ms")


if __name__ == "__main__":
	main()