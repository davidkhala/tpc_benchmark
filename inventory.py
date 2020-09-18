"""Inventory all data output by TPC Benchmark data generators

MIT License, see LICENSE file for complete text.
Copyright (c) 2020 SADA Systems, Inc.
"""

import config, tools, os

print("TPC-DS Inventory")
print("++++++++++++++++")
print()

filepath_list = []
for size in config.scale_factors:
    filepath_list.append(config.fp_ds_output + config.sep + str(size) + "GB")

for fp in filepath_list:
    if os.path.exists(fp):
        tools.print_inventory(fp)
    
print("TPC-H Inventory")
print("+++++++++++++++")
print()

filepath_list = []
for size in config.scale_factors:
    filepath_list.append(config.fp_h_output + config.sep + + str(size) + "GB")

for fp in filepath_list:
    if os.path.exists(fp):
        tools.print_inventory(fp)
