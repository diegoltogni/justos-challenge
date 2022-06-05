# %% [markdown]
# # Reading the data
# First things first... Let's read the parquet file and take a look at what's inside

# %%
# reading the CSVs
import pandas as pd
import numpy as np

df = pd.read_parquet('../data/dataset.gz.parquet')
policy_data = df.copy() # to keep raw data untouched

# %%
policy_data.head()

# %%
policy_data.info()

# %% [markdown]
# Not sure why, but the df.info() command didn't show me the count of NULL values. So here it is:

# %%
policy_data.isnull().sum()

# %% [markdown]
# # Preparing the data
# There are a few fields that I want to convert to better data types.

# %%
# weird way to check if I can safely convert to int the fields I want
# extracting the decimal places and validating if they're all 0s
print(policy_data["policy_holder_zipcode"].apply(lambda x: abs(x % 1)).sum())
print(policy_data["policy_claims_num_reported"].apply(lambda x: abs(x % 1)).sum())
print(policy_data["policy_claims_num_paid"].apply(lambda x: abs(x % 1)).sum())
print(policy_data["policy_holder_bonus_clas"].apply(lambda x: abs(x % 1)).sum())
print(policy_data["vehicle_make_year"].apply(lambda x: abs(x % 1)).sum())


# %%
# converting to timestamps
policy_data["policy_start_date"] = pd.to_datetime(policy_data["policy_start_date"])
policy_data["policy_holder_birth_date"] = pd.to_datetime(policy_data["policy_holder_birth_date"].astype(str))
policy_data["vehicle_make_year"] = pd.to_datetime(policy_data["vehicle_make_year"].astype(int), format='%Y')

# converting to string
policy_data["policy_holder_zipcode"] = policy_data["policy_holder_zipcode"].astype(int).astype(str)
policy_data["policy_holder_bonus_clas"] = policy_data["policy_holder_bonus_clas"].astype('Int64').astype(str) # using Int64 to be able to handle NULLs

# converting to int
policy_data["policy_claims_num_reported"] = policy_data["policy_claims_num_reported"].astype(int)
policy_data["policy_claims_num_paid"] = policy_data["policy_claims_num_paid"].astype(int)

# reordering columns
policy_data = policy_data[["policy_id",
                           "policy_start_date",
                           "policy_exposure_days",
                           "policy_premium_received_brl",
                           "policy_claims_num_reported",
                           "policy_claims_num_paid",
                           "policy_claims_total_amount_paid_brl",
                           "policy_holder_birth_date",
                           "policy_holder_gender",
                           "policy_holder_residence_city",
                           "policy_holder_residence_region",
                           "policy_holder_zipcode",
                           "policy_holder_residence_latitude",
                           "policy_holder_residence_longitude",
                           "policy_holder_bonus_clas",
                           "vehicle_brand",
                           "vehicle_model",
                           "vehicle_make_year",
                           "vehicle_tarif_class",
                           "vehicle_value_brl"
                         ]]

policy_data.head()

# %% [markdown]
# # Final adjustments
# Much better... Not even sure if all those transformation were actually necessary, but it is always better to tell python the correct data types. That might be helpful later when creating the graphs. Also, reordering the columns helps me to think when looking at a data sample.
# 
# Now I feel ready for the analysis itself.

# %% [markdown]
# After reading the loss ratio concept and the proposed excercise, it is really important to notice that the average of individual loss ratios is different than the total loss ratio for a specific group.

# %%
# individual loss_ratio calculation
policy_data["policy_loss_ratio"] = policy_data["policy_claims_total_amount_paid_brl"]/policy_data["policy_premium_received_brl"]

# %% [markdown]
# #### Descriptive statistics
# Running some basic descriptive statistics on all columns to better understand the data:

# %%
# Dates
policy_data.describe(include=[np.datetime64], datetime_is_numeric=True)

# %%
# Possible dimensions for the market analysis
policy_data.describe(include=[object])

# %%
# Numerical values
policy_data.describe(include=[np.number])

# %% [markdown]
# #### Data accuracy check
# I noticed that there are some birth dates dated 1900 (a bit odd). Let me double check that:

# %%
policy_data["policy_holder_birth_year"] = pd.DatetimeIndex(policy_data["policy_holder_birth_date"]).year
elderly_sample = policy_data[policy_data["policy_holder_birth_year"] < 1930] # older than ~90
elderly_sample["policy_holder_birth_year"].value_counts()

# %% [markdown]
# Indeed a significant number of policies is being assigned (very likely incorrectly) birth dates on 1900. To be considered in case we do anything using birth dates.

# %% [markdown]
# # Exploratory data analysis
# First I'll create a separated dataframe with only the relevant columns. I could run the analysis with more than just the columns defined here, but for the sake of time I won't. We could for example create age groups and use that for the investigation.

# %%
loss_ratio_analysis = policy_data[["policy_premium_received_brl",
                                   "policy_claims_total_amount_paid_brl",
                                  #  "policy_loss_ratio",
                                  #  "policy_holder_birth_year",
                                   "policy_holder_gender",
                                   "policy_holder_residence_city",
                                   "policy_holder_residence_region",
                                   # "policy_holder_zipcode",
                                   # "policy_holder_residence_latitude",
                                   # "policy_holder_residence_longitude",
                                   "policy_holder_bonus_clas",
                                   "vehicle_brand",
                                   # "vehicle_model",
                                   # "vehicle_make_year",
                                  #  "vehicle_tarif_class",
                                 ]]

# %%
# overall loss_ratio
print(loss_ratio_analysis["policy_claims_total_amount_paid_brl"].sum()/loss_ratio_analysis["policy_premium_received_brl"].sum())

# loss_ratio by gender
grouped = loss_ratio_analysis.groupby(by=["policy_holder_gender"])
claims_paid = grouped["policy_claims_total_amount_paid_brl"].agg(["sum","count"])
premium_received = grouped["policy_premium_received_brl"].agg(["sum","count"])
gender_loss_ratio = pd.DataFrame({"loss_ratio": claims_paid["sum"]/premium_received["sum"], "policy_count": premium_received["count"]})

gender_loss_ratio.sort_values(by="loss_ratio")

# %%
# loss_ratio by region
grouped = loss_ratio_analysis.groupby(by=["policy_holder_residence_region"])
claims_paid = grouped["policy_claims_total_amount_paid_brl"].agg(["sum","count"])
premium_received = grouped["policy_premium_received_brl"].agg(["sum","count"])
region_loss_ratio = pd.DataFrame({"loss_ratio": claims_paid["sum"]/premium_received["sum"], "policy_count": premium_received["count"]})

region_loss_ratio[region_loss_ratio["policy_count"]>200].sort_values(by="loss_ratio")

# %%
# loss_ratio by city
grouped = loss_ratio_analysis.groupby(by=["policy_holder_residence_region", "policy_holder_residence_city"])
claims_paid = grouped["policy_claims_total_amount_paid_brl"].agg(["sum","count"])
premium_received = grouped["policy_premium_received_brl"].agg(["sum","count"])
city_loss_ratio = pd.DataFrame({"loss_ratio": claims_paid["sum"]/premium_received["sum"], "policy_count": premium_received["count"]})

city_loss_ratio[city_loss_ratio["policy_count"]>20000].sort_values(by="loss_ratio")

# %%
# loss_ratio by bonus_clas
grouped = loss_ratio_analysis.groupby(by=["policy_holder_bonus_clas"])
claims_paid = grouped["policy_claims_total_amount_paid_brl"].agg(["sum","count"])
premium_received = grouped["policy_premium_received_brl"].agg(["sum","count"])
bonus_clas_loss_ratio = pd.DataFrame({"loss_ratio": claims_paid["sum"]/premium_received["sum"], "policy_count": premium_received["count"]})

bonus_clas_loss_ratio.sort_values(by="loss_ratio")

# %%
# loss_ratio by vehicle_brand
grouped = loss_ratio_analysis.groupby(by=["vehicle_brand"])
claims_paid = grouped["policy_claims_total_amount_paid_brl"].agg(["sum","count"])
premium_received = grouped["policy_premium_received_brl"].agg(["sum","count"])
vehicle_brand_loss_ratio = pd.DataFrame({"loss_ratio": claims_paid["sum"]/premium_received["sum"], "policy_count": premium_received["count"]})

vehicle_brand_loss_ratio[vehicle_brand_loss_ratio["policy_count"]>500].sort_values(by="loss_ratio")

# %% [markdown]
# # Potential segment opportunities
# These give us some indication of market groups that we could investigate further. I'd need to refresh my memory and python skills, but I'm sure there are robust statistical techniques (ANOVA, K-Means clustering, etc) that could be used to better analyse the data.
# 
# We'd need to check the data's variance and distribution to actually identify what a "small loss_ratio" would be, but for now I could list the groups we identified (using arbritary min sample sizes) with loss_ratio less than 30%:

# %%
#region
small_region_loss_ratio = region_loss_ratio[region_loss_ratio["policy_count"]>200].sort_values(by="loss_ratio")
small_region_loss_ratio = small_region_loss_ratio[region_loss_ratio["loss_ratio"]<0.3]
small_region_loss_ratio

# %%
#city
small_city_loss_ratio = city_loss_ratio[city_loss_ratio["policy_count"]>10000].sort_values(by="loss_ratio")
small_city_loss_ratio = small_city_loss_ratio[city_loss_ratio["loss_ratio"]<0.3]
small_city_loss_ratio

# %%
#vehicle_brand
small_vehicle_brand_loss_ratio = vehicle_brand_loss_ratio[vehicle_brand_loss_ratio["policy_count"]>500].sort_values(by="loss_ratio")
small_vehicle_brand_loss_ratio = small_vehicle_brand_loss_ratio[vehicle_brand_loss_ratio["loss_ratio"]<0.3]
small_vehicle_brand_loss_ratio


