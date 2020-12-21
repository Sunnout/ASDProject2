import matplotlib.pyplot as plt
import numpy as np
import seaborn as sb
import math
sb.set()
sb.set_style("darkgrid", {"axes.facecolor": ".9"})

through_paxos = [713.31, 771.3, 850.86, 892.43, 884.29, 873.12, 839.38]
lat_paxos = [3.95658, 7.76017, 12.81736, 25.78355, 52.17237, 113.0759, 225.59894]

through_mpaxos = [1374.2, 1781.43, 1797.83, 1825.02, 1843.16, 1819.04, 1012.1]
lat_mpaxos = [2.21374, 3.75727, 8.09333, 15.71946, 33.53865, 67.79841, 249.52864]

# PAXOS --------------------------------------------------------------------
plt.figure()
plt.plot(through_paxos, lat_paxos, color='gray', linestyle='dashed', linewidth = 2, 
         marker='o', markerfacecolor='blue', markersize=8) 

# naming the x axis 
plt.xlabel('Throughput (ops/sec)') 
# naming the y axis 
plt.ylabel('Avg Latency (ms)') 
  
# giving a title to my graph 
plt.title('Paxos Analysis') 
  
# function to show the plot 
plt.savefig("./graphs/paxos.jpg", bbox_inches='tight') 

# MULTIPAXOS ---------------------------------------------------------------
plt.figure()
plt.plot(through_mpaxos, lat_mpaxos, color='gray', linestyle='dashed', linewidth = 2, 
         marker='o', markerfacecolor='blue', markersize=8) 

# naming the x axis 
plt.xlabel('Throughput (ops/sec)') 
# naming the y axis 
plt.ylabel('Avg Latency (ms)') 
  
# giving a title to my graph 
plt.title('Multipaxos Analysis') 
  
# function to show the plot 
plt.savefig("./graphs/multipaxos.jpg", bbox_inches='tight') 

# BOTH ----------------------------------------------------------------------

plt.figure()
plt.plot(through_mpaxos, lat_mpaxos, color='gray', linestyle='dashed', linewidth = 2, 
         marker='o', markerfacecolor='blue', markersize=8, label="Multi-Paxos") 

plt.plot(through_paxos, lat_paxos, color='gray', linestyle='dashed', linewidth = 2, 
         marker='x', markerfacecolor='blue', markersize=8, label="Paxos")

# naming the x axis 
plt.xlabel('Throughput (ops/sec)') 
# naming the y axis 
plt.ylabel('Average Latency (ms)') 
  
# giving a title to my graph 
plt.title('Paxos and Multi-Paxos Comparison') 

plt.legend()
  
# function to show the plot 
plt.savefig("./graphs/both_with_128.jpg", bbox_inches='tight') 

# data without the 128 threads run
through_paxos = [713.31, 771.3, 850.86, 892.43, 884.29, 873.12]
lat_paxos = [3.95658, 7.76017, 12.81736, 25.78355, 52.17237, 113.0759]

through_mpaxos = [1374.2, 1781.43, 1797.83, 1825.02, 1843.16, 1819.04]
lat_mpaxos = [2.21374, 3.75727, 8.09333, 15.71946, 33.53865, 67.79841]

plt.figure()
plt.plot(through_mpaxos, lat_mpaxos, color='gray', linestyle='dashed', linewidth = 2, 
         marker='o', markerfacecolor='blue', markersize=8, label="Multi-Paxos") 

plt.plot(through_paxos, lat_paxos, color='gray', linestyle='dashed', linewidth = 2, 
         marker='x', markerfacecolor='blue', markersize=8, label="Paxos")

# naming the x axis 
plt.xlabel('Throughput (ops/sec)') 
# naming the y axis 
plt.ylabel('Average Latency (ms)') 
  
# giving a title to my graph 
plt.title('Paxos and Multi-Paxos Comparison') 

plt.legend()
  
# function to show the plot 
plt.savefig("./graphs/both_without_128.jpg", bbox_inches='tight') 