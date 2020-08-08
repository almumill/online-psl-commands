import matplotlib.pyplot as plt

def plot_demand(time_demand_dict, time_to_constant_dict):
	x = sorted([time_to_constant_dict[(z[1], z[2], z[3], z[4])] for z in time_demand_dict.keys()])
	print(sorted(list(time_demand_dict.keys()))[0:168*4])
	y = [time_demand_dict[z] for z in sorted(time_demand_dict.keys())]
	plt.plot(x[168*2:168*4], y[168*2:168*4])
	plt.show()