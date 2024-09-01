# Big Data Computing

This repository contains three assessments for the course Big Data Computing offered by the Engineering department at the University of Padua 2022/2023.

All assessments are written in Python 3 and requires PySpark.

## 1: Triangle Counting 1

Implement Spark two MapReduce algorithms to count the number of distinct triangles in an undirected graph $𝐺 = (𝑉 , 𝐸)$, where a triangle is defined by 3 vertices $𝑢, 𝑣, 𝑤$ in $𝑉$ , such that $(𝑢, 𝑣), (𝑣, 𝑤), (𝑤, 𝑢)$ are in $𝐸$. Triangle counting is a popular primitive in social network analysis, where it is used to detect communities and measure the cohesiveness of those communities. It has also been used is different other scenarios, for instance: detecting web spam (the distributions of local triangle frequency of spam hosts significantly differ from those of the non-spam hosts), uncovering the hidden thematic structure in the World Wide Web
(connected regions of the web which are dense in triangles represents a common topic), query plan optimization in databases (triangle counting can be used for estimating the size of some joins).

## 2: Triangle Counting 2

This assessment is an extension to the previous one, where an extra approximation algorithm is implemented and compared to the previous solutions.

## 3: Streaming

Implement a program which processes a stream of items and assesses experimentally the space-accuracy tradeoffs featured by the count sketch to estimate the individual frequencies of the items and the second moment $F_2$.