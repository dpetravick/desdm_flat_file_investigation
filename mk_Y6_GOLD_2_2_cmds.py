#!/usr/bin/env python3
#
# print the shell commeands needed to extract 
# Y6gol_2-2 into parquest archive tree
# Produce a dictionary where the keys 
# are a indicied of a lower oder (coarser)
# helpix element. and the values are the
# indicies fo if a finer helpix mesh.
#

import math
low_order = 3
high_order = 5

saved_len = -1 

index_map = {low_index : [low_index] for low_index in range(int(3*math.pow(4,low_order+1)))}

#
# Scuccesively divide index map into smaller Partiions,
# Replace the inxez of preior level wiht nixt level in indexmap[key]
# inerat  for the number of refinements needed to reach the highest order needed.
 
for refinement in  range(high_order - low_order):
    for key in index_map.keys():
        next_index_list = []
        prior_index_list = index_map[key]
        for index in prior_index_list:
            next_index_list.extend([4*index+n for n in range(4)])
            #if len(next_index_list) > 4 : breakpoint()
            index_map[key] = next_index_list
#
# Print the command lines 
print('#!/usr/bin/bash')
print('set -x')
for key in  index_map.keys():
    coarse_partition_index = key 
    fine_partition_indexes = [str(i) for i in index_map[key]]
    fine_partition_indexes = ",".join(fine_partition_indexes)

    print ( 
        f'./parquet_export.py parquet2 ' 
        f' -m 20_000_000_000  Y6_GOLD_2_2 {key} ' 
        f'"where HPIX_32 in ({fine_partition_indexes})"' 
    )
