#this file is mainly scratchwork I don't think it will run on its own
#I ran it line-by-line in R using the Reticulate package
from db2pq import wrds_update_pq

# CRSP
wrds_update_pq('ccmxpf_lnkhist', 'crsp', 
               col_types={'lpermno': 'int32',
                          'lpermco': 'int32'})
wrds_update_pq('dsf', 'crsp', 
               col_types={'permno': 'int32',
                          'permco': 'int32'})
wrds_update_pq('dsi', 'crsp')
wrds_update_pq('erdport1', 'crsp')
wrds_update_pq('comphist', 'crsp')
wrds_update_pq('dsedelist', 'crsp', 
               col_types={'permno': 'int32', 
                          'permco': 'int32'})
wrds_update_pq('dseexchdates', 'crsp', col_types={'permno': 'int32',
                                                   'permco': 'int32'})
wrds_update_pq('msf', 'crsp', col_types={'permno': 'int32',
                                         'permco': 'int32'})
wrds_update_pq('msi', 'crsp')
wrds_update_pq('mse', 'crsp', 
               col_types={'permno': 'int32',
                          'permco': 'int32'})
wrds_update_pq('stocknames', 'crsp',
               col_types={'permno': 'int32',
                          'permco': 'int32'})
wrds_update_pq('dsedist', 'crsp', 
               col_types={'permno': 'int32',
                          'permco': 'int32'})

# Fama-French library
wrds_update_pq('factors_daily', 'ff')

# Compustat
wrds_update_pq('company', 'comp')
wrds_update_pq('funda', 'comp')
wrds_update_pq('funda_fncd', 'comp')
wrds_update_pq('fundq', 'comp')
wrds_update_pq('r_auditors', 'comp')
wrds_update_pq('idx_daily', 'comp')
wrds_update_pq('aco_pnfnda', 'comp')

# The segment data is in comp_segments_hist_daily in PostgreSQL,
# but in compsegd in SAS, so we need to use sas_schema to find the SAS data.
wrds_update_pq('seg_customer', 'compseg')
wrds_update_pq('names_seg', 'compseg')
