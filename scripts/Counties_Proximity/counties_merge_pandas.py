import pandas as pd

#read two csv and type is string
counties=pd.read_csv('/n/holyscratch01/cga/dkakkar/county_proximity/counties_projected.txt',dtype=str)
near=pd.read_csv('/n/holyscratch01/cga/dkakkar/county_proximity/input_1_5M_near_table.txt',dtype=str)

#merge two csv depends on OID_(in countries_protected) and NEAR_FID(in sample_near_table)
merge_result_task1=pd.merge(counties,near,left_on='OID_',right_on='NEAR_FID')

#select the targeted fields: IN_ID,FIPS,NEAR_DIST
merge_result_task1=merge_result_task1[['IN_FID','FIPS','NEAR_DIST']]

merge_result_task1['NEAR_DIST'] = merge_result_task1['NEAR_DIST'].astype(float)

merge_result_task1 = merge_result_task1.sort_values(['NEAR_DIST'])

#combine to one row
fips_agg=merge_result_task1.groupby('IN_FID').agg({'FIPS':list})

near_dist_agg=merge_result_task1.groupby('IN_FID').agg({'NEAR_DIST':list})

inner_merged = pd.merge(fips_agg,near_dist_agg, on=["IN_FID"])

inner_merged.head()

projected=pd.read_csv('/n/holyscratch01/cga/dkakkar/county_proximity/input_1_5M_projected.txt',dtype=str)

merge_result_projected=projected.merge(inner_merged,left_on='OID_',right_on='IN_FID')

merge_result_projected.head()

#select the targeted fields
merge_result_projected=merge_result_projected.drop(labels=['OID_'],axis=1)

merge_result_projected[['DIST_1', 'DIST_2', 'DIST_3','DIST_4']]= pd.DataFrame(merge_result_projected["NEAR_DIST"].to_list())

merge_result_projected[['FIPS_1', 'FIPS_2', 'FIPS_3','FIPS_4']]= pd.DataFrame(merge_result_projected["FIPS"].to_list())

merge_result_projected=merge_result_projected.drop(labels=['FIPS','NEAR_DIST'],axis=1)

merge_result_projected.to_csv('/n/holyscratch01/cga/dkakkar/county_proximity/output/input_1_5M_output.txt')
