sqlstring1 = '''
select
buy_days,
first_ordr_days,
creat_days,
phone_flag,
last_ordr_days,
shanghai_flag,
pm_wght,
shpmt_amt,
mphone_flag,
import_milk_flag,
ordr_num_y,
categ_lvl3_num_y,
sku_num_y,
pm_num_y,
discount_pct_y,
shpmt_amt_y,
import_food_ordr_y,
kitch_bath_ordr_y,
kitch_bath_pct_y,
clothes_pct_y,
visit_day_num,
elpsed_time,
categ_page_pv,
colct_shop_num,
non_foods_categ_num,
non_foods_kw_num,
foods_categ_num,
sku_vis_num_p3,
ordr_num_p3,
ordr_num_p2,
ordr_num_p1,
loss_3mon_flag
from tmp_dev.cust_loss_unit_1_2014
'''

from pyspark.sql import HiveContext
hivectx = HiveContext(sc)
rows = hivectx.sql(sqlstring1)

from pyspark.mllib.classification import LabeledPoint
def clean_feature(rec):
    label = int(rec[-1])
    features = [float(x)  for x in rec[:-1]]
    return LabeledPoint(label, features)


data = rows.map(clean_feature)
data.cache()

from pyspark.mllib.tree import RandomForest
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# built model
model = RandomForest.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={}, numTrees=1000, featureSubsetStrategy="auto",impurity='gini', maxDepth=4, maxBins=32)