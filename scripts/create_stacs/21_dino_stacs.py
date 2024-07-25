#%%
import xarray as xr
import numpy as np
import pandas as pd
import os
import sys
import glob
import datetime
import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


# %%
asd = xr.open_dataset(r'c:\Users\guarneri\OneDrive - Stichting Deltares\Documents\PostdocVault\Postdoc\ORELSE\Database\DIS_3_0 - Zeeland_WGS84_UTM31_NetCDF.nc')
# %%

plt.scatter(asd.X,asd.Y)
# %%
dq = np.meshgrid(asd.X,asd.Y,asd.Z)
# %%
plt.plot()
##
#:)
asd["SCHELPEN_KLASSE_SOARES"].notnull()[78].plot()
# %%
import panel as pn
pn.extension('vtk')
# %%
asd["KLASSE_DEF_SOARES"][78].plot()
# %%
import numpy as np

data_matrix = np.zeros([75, 75, 75], dtype=np.uint8)
data_matrix[0:35, 0:35, 0:35] = 50
data_matrix[25:55, 25:55, 25:55] = 100
data_matrix[45:74, 45:74, 45:74] = 150

pn.pane.VTKVolume(data_matrix, width=800, height=600, spacing=(3,2,1), interpolation='nearest', edge_gradient=0, sampling=0)
# %%
#%%

# %%
