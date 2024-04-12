"""
Script for reading .hdf5 output files from HAWC2 time marching simulation available in the dataset https://doi.org/10.11583/DTU.24460090

Notes:
    * The script require the pandas package.
    * The script require the lacbox package.
    * It is recommended that the script is stored in the folder structure described in the documentation for the dataset https://doi.org/10.11583/DTU.24460090

"""

#%% import repositories
import os
import sys
import re
import pandas as pd
from pathlib import Path
import multiprocessing

from lacbox.io import ReadHAWC2
from lacbox.test import test_data_path

#%% begin input 
# Number of CPUs to run on
no_cpu = multiprocessing.cpu_count()

# Defining data path in/out
absolute_path = Path(__file__).resolve().parent.parent

relative_path_in  = "res"
data_path_in  = os.path.join(absolute_path, relative_path_in)

relative_path_out = "PyRes"
data_path_out = os.path.join(absolute_path, relative_path_out)

# SHMS - Channel specifications
#Sensors - [acc, pos, rot, moment, force]
sensors         = [[]]
#Directions - [x, y ,z]    
direction       = [[]]
#Coordinate systems -  [Gbal, Lcal:blade1, Lcal:blade2, Lcal:blade3]          
coord_system    = [[]]
# Structural component [twr, mnp, emb-mnp, bld1, bld2, bld3]
struc_component = [['twr', 'mnp', 'emb-mnp']]
# location in specified coordinate system [nXXX, pXXX] --- XXX refers to the z coordinate 
location        = [[]]

# Str patterns for shm channels
chl_request  = [sensors, direction, coord_system, struc_component, location]

if len(set(map(len,chl_request))) != 1: 
    print("WARNING! Lists in chl_request must be the same lengt")
    sys.exit()

#name search better using wildcard/glob/regex
#%% 
def read_shm(filepath, opr_env_chls, opr_env_names, shm_pattern):
    # Read .hdf5 file
    h2res = ReadHAWC2(filepath)
    names, units, desc = h2res.chaninfo

    # Identify opr_env_chls index
    idx = [i for i in range(len(names)) for str in opr_env_chls if str in names[i]]
    idx = list(set(idx))
    # Store selected channels in pandas dataframe
    df1 = pd.DataFrame(h2res.data[:,idx], columns=opr_env_names)

    # Identify str_res_chls index (zip)
    idx = [i for i in range(len(df1.axes[1]),len(desc)) if re.search(shm_pattern,desc[i]) != None]
    str_res_names = [desc[i].split(' ')[-1] for i in idx]
    # Store selected channels in pandas dataframe
    df2 = pd.DataFrame(h2res.data[:,idx], columns=str_res_names)

    # Combine operational and environmental data with structural response
    df = pd.concat([df1, df2], axis=1)

    return df

def write_pqt(filepath):    
    # Running 
    df = read_shm(filepath, opr_env_chls, opr_env_names, pattern)
    
    #subfolder
    subfolder = re.split('\\\|:|\.',filepath)[-3]
    path = os.path.join(data_path_out,subfolder)

    isExist   = os.path.exists(path)
    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(path)
    
    filename = os.path.join(path ,re.split('\W',filepath)[-2] + ".pqt")
    # Saving file to parquet
    df.to_parquet(filename) 

    return filename

#%% Main variables

# Environmental and operational channels
opr_env_chls = ['Time',
                'Omega',
                'Ae rot. power',
                'DLL inp   2:   2',
                'DLL inp   2:   1',
                'Ae rot. torque',
                'Ae rot. thrust',
                'DLL inp   6:   1',
                'bea2 angle',
                'bea2 angle_speed',
                'DLL inp   4:   1',
                'WSP gl. coo.,Vx',
                'WSP gl. coo.,Vy',
                'WSP gl. coo.,Vz',
                'WSP rotor avg gl. coo., Vx',
                'WSP rotor avg gl. coo., Vy',
                'WSP rotor avg gl. coo., Vz',
                'Water surf.']

# Environmental and operational names
opr_env_names = ['opr-env_time',
                 'opr-env_rtr-spd',
                 'opr-env_aero-pwr',
                 'opr-env_elec-pwr',
                 'opr-env_aero-trq',
                 'opr-env_gen-trq',
                 'opr-env_thrst',
                 'opr-env_twr-clr',
                 'opr-env_pitch-bld1',
                 'opr-env_pitch-bld2',
                 'opr-env_pitch-bld3',
                 'opr-env_pitch-speed-bld1',
                 'opr-env_pitch-speed-bld2',
                 'opr-env_pitch-speed-bld3',
                 'opr-env_brk-trq',
                 'opr-env_Vx',
                 'opr-env_Vy',
                 'opr-env_Vz',
                 'opr-env_rot-avg-Vx',
                 'opr-env_rot-avg-Vy',
                 'opr-env_rot-avg-Vz',
                 'opr-env_wave']

# SHM channels pattern
pattern      = ''
for j in range(0,len(chl_request[0])):
    pattern_temp = ''
    group = [0]*len(chl_request)
    for i in range(0,len(chl_request)):
        if not chl_request[i][j]:
            group[i] = ""
        elif chl_request[i][j]:
            group[i] = '('+'|'.join(chl_request[i][j])+')'
        
        pattern_temp  += '\S*'+group[i]
    pattern += pattern_temp+'|'

pattern = '('+pattern[:-1]+')'

# File path
data_filepath = []
for root, directories, files in os.walk(data_path_in):
    for filename in files:
        if filename.endswith('.hdf5'):
            data_filepath.append(os.path.join(root, filename))

#%% Main run
# protect the entry point
if __name__ == '__main__':
    # create and configure the process pool
    with multiprocessing.Pool(no_cpu) as pool:
        # execute tasks in chunks, block until all complete
        pool.map(write_pqt, data_filepath, chunksize=len(data_filepath)//no_cpu )
    # process pool is closed automatically