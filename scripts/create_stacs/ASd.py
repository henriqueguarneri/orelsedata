#%%
import numpy as np
import matplotlib.pyplot as plt

# Constants
A = 1
B = 1
B1 = 10

# Range for h
h = np.linspace(0.01, 10, 400)  # start from 0.01 to avoid division by zero


# Scalar field equation
k = A * h**0.7 * (1 - np.exp(-B * h**-0.3))
k1 = A * h**0.7 * (1 - np.exp(-B1 * h**-0.3))

# Plotting
plt.figure(figsize=(10, 6))
plt.plot(h, k, label='k = A*h^0.7*(1-e^{-B*h^-0.3})')
plt.plot(h, k1, label='k = A*h^0.7*(1-e^{-B*h^-0.3})')
plt.xlabel('h')
plt.ylabel('k')
plt.title('Plot of the Scalar Field k = A*h^0.7*(1-e^{-B*h^-0.3})')
plt.legend()
plt.grid(True)
plt.show()
# %%
k