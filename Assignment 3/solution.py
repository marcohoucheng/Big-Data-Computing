from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import random as rand
import numpy as np
# from collections import Counter
import operator
from collections import OrderedDict

# After how many items should we stop?
THRESHOLD = 10000000

# Operations to perform after receiving an RDD 'batch' at time 'time'
def process_batch(time, batch):
    # We are working on the batch at time `time`.
    global streamLength, histogram, streamRLength, C, C2
    batch_size = batch.count()
    # If we already have enough points (> THRESHOLD), skip this batch.
    if streamLength[0]>=THRESHOLD:
        return
    streamLength[0] += batch_size
    # Extract the distinct items from the batch
    batch_filtered = batch.filter(lambda s: interval(s))
    batch_filtered_size = batch_filtered.count()
    streamRLength[0] += batch_filtered_size
    batch_kv = batch_filtered.map(lambda s: (int(s), 1))
    
    # Exact Methods
    batch_true_count = batch_kv.countByKey() # reduceByKey(lambda x, y: x + y).collectAsMap()

    for key, value in batch_true_count.items():
        if key not in histogram:
            histogram[key] = value
        else:
            histogram[key] += value
    
    # CountSketch
    # batch_items_all = batch_kv.collect()
    # for (x, _) in batch_items_all:
    #     for j in range(D):
    #         C[(j, hash_h(j, x, W))] += hash_g(j, x)

    def c_func(x):
        x = int(x)
        return {((j, hash_h(j, x, W)), hash_g(j, x)) for j in range(D)}
    C_local = batch_filtered.flatMap(c_func).reduceByKey(lambda x, y: x + y).collectAsMap()

    for key, value in C_local.items():
        if key not in C:
            C[(key[0],key[1])] = value
        else:
            C[(key[0],key[1])] += value
    
    if batch_size > 0:
        print("Batch size at time [{0}] is: {1}".format(time, batch_size))
        print("Filtered batch size at time [{0}] is: {1}".format(time, batch_filtered_size))

    if streamLength[0] >= THRESHOLD:
        stopping_condition.set()
        

if __name__ == '__main__':
    assert len(sys.argv) == 7, "USAGE: <D> <W> <left> <right> <K> <portExp>"

    # IMPORTANT: when running locally, it is *fundamental* that the
    # `master` setting is "local[*]" or "local[n]" with n > 1, otherwise
    # there will be no processor running the streaming computation and your
    # code will crash with an out of memory (because the input keeps accumulating).
    conf = SparkConf().setMaster("local[*]").setAppName("DistinctExample")
    # If you get an OutOfMemory error in the heap consider to increase the
    # executor and drivers heap space with the following lines:
    # conf = conf.set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
    
    
    # Here, with the duration you can control how large to make your batches.
    # Beware that the data generator we are using is very fast, so the suggestion
    # is to use batches of less than a second, otherwise you might exhaust the memory.
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)  # Batch duration of 1 second
    ssc.sparkContext.setLogLevel("ERROR")
    
    # TECHNICAL DETAIL:
    # The streaming spark context and our code and the tasks that are spawned all
    # work concurrently. To ensure a clean shut down we use this semaphore.
    # The main thread will first acquire the only permit available and then try
    # to acquire another one right after spinning up the streaming computation.
    # The second tentative at acquiring the semaphore will make the main thread
    # wait on the call. Then, in the `foreachRDD` call, when the stopping condition
    # is met we release the semaphore, basically giving "green light" to the main
    # thread to shut down the computation.
    # We cannot call `ssc.stop()` directly in `foreachRDD` because it might lead
    # to deadlocks.
    stopping_condition = threading.Event()
    
    
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # INPUT READING
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    # 0. Read the number of rows of the count sketch    
    D = int(sys.argv[1])
    print("Receiving number of rows =", D)

    # 1. Read the number of columns of the count sketch    
    W = int(sys.argv[2])
    print("Receiving number of columns =", W)

    # 2. Read the left endpoint of the interval of interest    
    left = int(sys.argv[3])
    print("Receiving left endpoint of interval of interest =", left)

    # 3. Read the right endpoint of the interval of interest    
    right = int(sys.argv[4])
    print("Receiving right endpoint of interval of interest =", right)

    # 4. Read the number of top frequent items of interest    
    K = int(sys.argv[5])
    print("Receiving number of top frequent items =", K)

    # 5. Read the port number   
    portExp = int(sys.argv[6])
    print("Receiving data from port =", portExp)

    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # DEFINING THE REQUIRED FUNCTIONS
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    # Create D versions of the 2 hash functions
    rand.seed(0)
    p = 8191
    a = [rand.randint(1, p-1) for i in range(2*D)]
    b = [rand.randint(0, p-1) for i in range(2*D)]
    def hash_h(j, x, C):
        return (a[j] * x + b[j]) % p % C
    def hash_g(j, x):
        r = (a[j + D] * x + b[j + D]) % p % 2
        if r == 0:
            r = -1
        return r
    
    # Filtering function
    def interval(x):
        return int(x) in range(left, right + 1)
    
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    streamLength = [0] # Stream length (an array to be passed by reference)
    streamRLength = [0]
    histogram = {} # True freuqency table
    C = {} # Count Sketch Table 

    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    # For each batch, to the following.
    # BEWARE: the `foreachRDD` method has "at least once semantics", meaning
    # that the same data might be processed multiple times in case of failure.
    stream.foreachRDD(lambda time, batch: process_batch(time, batch))
    
    # MANAGING STREAMING SPARK CONTEXT
    print("Starting streaming engine")
    ssc.start()
    print("Waiting for shutdown condition")
    stopping_condition.wait()
    print("Stopping the streaming engine")
    # NOTE: You will see some data being processed even after the
    # shutdown command has been issued: This is because we are asking
    # to stop "gracefully", meaning that any outstanding work
    # will be done.
    ssc.stop(False, True)
    print("Streaming engine stopped")

    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # END OF STREAM CALCULATIONS FOR COUNT SKETCH, SECOND MOMENT AND ESTIMATION
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    # Interval Length
    interval_length = streamRLength[0]
    
    # Number of distinct items
    distinct_item_count = len(histogram)

    # Count Sketch
    f_uj = {(key,j): (hash_g(j, key) * C[(j,hash_h(j, key,W))]) for j in range(D) for key in histogram}
    # Approximation of frequency of u
    count_approx = {key: int(np.median([v for k, v in f_uj.items() if k[0] == key])) for key in histogram.keys()}
    # F2 Approximation
    f2_approx = np.median([np.sum([v**2 for k, v in C.items() if k[0] == j]) for j in range(D)])/(interval_length**2)

    # True F2
    F2 = np.sum(np.power(list(histogram.values()),2))/(interval_length**2)

    #Average Error
    sorted_histogram = dict(sorted(histogram.items(), key=operator.itemgetter(1),reverse=True))
    sorted_count_approx = dict(OrderedDict([(key, count_approx[key]) for key in sorted_histogram]))
    sorted_histogram_keys = list(sorted_histogram.keys())
    sorted_histogram_values = list(sorted_histogram.values())
    avg_error = np.mean(abs(np.array(sorted_histogram_values[:K]) - np.array(list(sorted_count_approx.values())[:K]))/np.array(sorted_histogram_values[:K]))

    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # FINAL OUTPUTS FOR THE PROGRAMME
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    # COMPUTE AND PRINT FINAL STATISTICS
    print(f"D = {D} W = {W} [left,right] = [{left},{right}] K = {K} Port = {portExp}")
    print("Total number of items =", streamLength[0])
    print(f"Total number of items in [{left},{right}] =", interval_length)
    print(f"Number of distinct items in [{left},{right}] =", distinct_item_count)
    
    if K < 21:
        for i in range(K-1):
            print(f"Item {sorted_histogram_keys[i]} Freq = {sorted_histogram_values[i]} Est. Freq = {count_approx[sorted_histogram_keys[i]]}")

    print(f"Avg err for top {K} =", avg_error)
    print(f"F2 {F2} F2 Estimate {f2_approx}")