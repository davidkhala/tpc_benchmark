"""Inventory all data output"""

import config, tools

print("TPC-DS")
print("======")
print()
_base = config.fp_ds_output_mnt + config.sep
filepath_list = [_base + "1GB",
                 _base + "1GBA",
                 _base + "1GBB"]
for fp in filepath_list:
    if os.path.exists(fp):
        tools.print_inventory(fp)
    
print("TPC-H")
print("=====")
print()
_base = config.fp_ds_output_mnt + config.sep
filepath_list = [_base + "1GB",
                 _base + "1GBA",
                 _base + "1GBB"]
for fp in filepath_list:
    if os.path.exists(fp):
        tools.print_inventory(fp)