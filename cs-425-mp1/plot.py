import matplotlib.pyplot as plt

avg = [0.2429866, 0.1902894, 0.426641, 5.3219166]
std = [0.0041, 0.0898, 0.00413, 0.03514]

labels = ['frequent', 'only-1-log', 'somewhat-frequent', 'with-options']

plt.figure(figsize = (10, 5))
 
# creating the bar plot
plt.bar(labels, avg, width = 0.4, yerr=std)
plt.xlabel('Types of query')
plt.ylabel('Average Latency')
plt.savefig('QueryLatency.png', dpi=400)