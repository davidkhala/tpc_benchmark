"""Inventory all data output"""

import config, tools, os

print("TPC-DS Inventory")
print("++++++++++++++++")
print()

_base_local = config.fp_ds_output + config.sep
_base_mnt = config.fp_ds_output_mnt + config.sep

filepath_list = []
for base in [_base_local, _base_mnt]:
    for size in config.tpc_scale:
        filepath_list.append(base + str(size) + "GB")

for fp in filepath_list:
    if os.path.exists(fp):
        tools.print_inventory(fp)
    
print("TPC-H Inventory")
print("+++++++++++++++")
print()

_base_local = config.fp_h_output + config.sep
_base_mnt = config.fp_h_output_mnt + config.sep

filepath_list = []
for base in [_base_local, _base_mnt]:
    for size in config.tpc_scale:
        filepath_list.append(base + str(size) + "GB")

for fp in filepath_list:
    if os.path.exists(fp):
        tools.print_inventory(fp)
    