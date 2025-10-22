import random
import numpy
alpha = 2
target_mean = 1000
x_min = target_mean * (alpha - 1) / alpha
random.seed(42)
service_times = []
for _ in range(1000000):
    u = random.random()
    service_time = x_min / (u ** (1/alpha))
    service_times.append(service_time)
print(f"Average service time: {numpy.mean(service_times)}")
print(f"50th percentile service time: {numpy.percentile(service_times, 50)}")
print(f"90th percentile service time: {numpy.percentile(service_times, 90)}")
print(f"95th percentile service time: {numpy.percentile(service_times, 95)}")
print(f"99th percentile service time: {numpy.percentile(service_times, 99)}")
print(f"99.9th percentile service time: {numpy.percentile(service_times, 99.9)}")
