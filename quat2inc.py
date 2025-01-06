"""
Function for calculating inclinations in fore-aft and side-side direction based on quarternions from output files from HAWC2 time marching simulation available in the dataset https://doi.org/10.11583/DTU.24460090
"""

import pandas as pd
import numpy as np

def get_angles_vec(quaternion):
    a,b,c,d = quaternion.values[:,0],quaternion.values[:,1],quaternion.values[:,2],quaternion.values[:,3]

    R11 = a**2 + b**2 - c**2 - d**2
    R12 = 2*b*c - 2*a*d
    R13 = 2*b*d + 2*a*c
    R21 = 2*b*c + 2*a*d
    R22 = a**2 - b**2 + c**2 - d**2
    R23 = 2*c*d - 2*a*b
    R31 = 2*b*d - 2*a*c
    R32 = 2*c*d + 2*a*b
    R33 = a**2 - b**2 - c**2 + d**2

    # Create a 3D array by stacking the matrices along the first axis
    R = np.stack([R11, R12, R13, R21, R22, R23, R31, R32, R33], axis=1)
    R = R.reshape(len(quaternion), 3, 3)


    correction = np.array([[-1, 0,  0],
                           [ 0, 1,  0],
                           [ 0, 0, -1]])
    R = R @ correction  
    
    inc_SS   = - np.arccos(R[:,0,2]) + np.pi/2
    inc_FA  =   np.arccos(R[:,1,2]) - np.pi/2

    # return inc_FA, inc_SS
    return pd.Series({'inc_FA': inc_FA, 'inc_SS': inc_SS}) 