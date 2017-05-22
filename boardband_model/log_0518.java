import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType};
import org.apache.spark.{SparkConf, SparkContext, SparkException};
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;

import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.util.MLUtils;

import org.apache.spark.{SparkConf, SparkContext, SparkException};
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.feature.{StringIndexer, IndexToString, VectorIndexer};
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;


val conf = new SparkConf();

@transient val hiveCtx = new HiveContext(sc);

hiveCtx.sql(s"use jhzx_hive_db");


val pre_Data=hiveCtx.sql("select use_num,KD_RATE,AGE_RATE,CMCC_KD_CNT,HOME_DEPEND,cast(SEARCH_BJ_FLAG as Double) SEARCH_BJ_FLAG,cast(SEARCH_ZJ_FLAG as Double) SEARCH_ZJ_FLAG,AGE_FLAG1,AGE_FLAG2,age_flag3,age_flag4,upay_flag,cluster2,cluster3,ARPU,ownserv_flag,otherserv_flag,stay_flag,LABEL from hdx_jh_dataset_15 ");

val data1=pre_Data.rdd.map(x =>(x(0).toString.toDouble, x(1).toString.toDouble,x(2).toString.toDouble,x(3).toString.toDouble,x(4).toString.toDouble, x(5).toString.toDouble,x(6).toString.toDouble,x(7).toString.toDouble,x(8).toString.toDouble,x(9).toString.toDouble,x(10).toString.toDouble,x(11).toString.toDouble,x(12).toString.toDouble,x(13).toString.toDouble,x(14).toString.toDouble,x(15).toString.toDouble,x(16).toString.toDouble,x(17).toString.toDouble,x(18).toString.toDouble));

val data2=data1.map(x=>LabeledPoint(x._22,Vectors.dense(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._11,x._12,x._13,x._14,x._15,x._16,x._17,x._18,x._19,x._20,x._21)));

val data1=pre_Data.rdd.map(x=> (x(0),x(1)))

val pre_Data=hiveCtx.sql("select * from hdx_jh_dataset_26 ");

//val data1=pre_Data.rdd;

val data1=pre_Data.rdd.map(x =>(x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19),x(20),x(21),x(22),x(24),x(25),x(26),x(27),x(28),x(29),x(30),x(31)))

val data1=pre_Data.rdd.map(x =>(x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19),x(20),x(21),x(30)))

val data1=pre_Data.rdd.map(x =>(x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19),x(20),x(21),x(22),x(24),x(25),x(26),x(27),x(28),x(29)))


val data1=pre_Data.rdd.map(x =>(x(22),x(24),x(25),x(26),x(27),x(28),x(29)))


LabeledPoint



val data1=pre_Data.rdd.map(x =>(x(1),x(2),x(3),x(4)))


val data2 = data1.map(x=>LabeledPoint(x._32,Vectors.dense(x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._11,x._12,x._13,x._14,x._15,x._16,x._17,x._18,x._19,x._20,x._21,x._22,x._23,x._24,x._25,x._26,x._27,x._28,x._29,x._30,x._31)));

val data2=data1.map(x=>(x._1,LabeledPoint(x._19,Vectors.dense(x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._11,x._12,x._13,x._14,x._15,x._16,x._17,x._18))));

pre_Data.take(10)

pre_Data.toDouble

x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19),x(20),x(21),x(22),x(23),x(24),x(25),x(26),x(27),x(28),x(29),x(30),x(31)




select 
cast(user_id as Double),
gprs_fee, 
kd_rate, 
age_rate, 
cmcc_kd_cnt, 
cast(home_depend as Double),
cast(search_bj_flag as Double),
cast(search_zj_flag as Double),
cast(age_flag1 as Double),
cast(age_flag2 as Double),
cast(age_flag3 as Double),
cast(age_flag4 as Double),
cast(upay_flag as Double),
cast(cluster2 as Double),
cast(cluster3 as Double),
arpu,
cast(ownserv_flag as Double),
cast(otherserv_flag as Double),
cast(night_gprs_rate as Double),
cast(new_user_flag as Double),
use_num1,
use_num2,
use_num,
cast(credit_5_tmp as Double),
cast(credit_5 as Double),
credit_4, 
credit_3, 
credit_2, 
credit_1, 
credit_0, 
sex_m_tmp, 
cast(label as Double) from jcc_jh_dataset_26



user_id: bigint,
gprs_fee: double, 
kd_rate: double, 
age_rate: double, 
cmcc_kd_cnt: double, 
home_depend: int, 
search_bj_flag: string, 
search_zj_flag: string, 
age_flag1: int, 
age_flag2: int, 
age_flag3: int, 
age_flag4: int, 
upay_flag: int, 
cluster2: int, 
cluster3: int, 
arpu: double, 
ownserv_flag: int, 
otherserv_flag: int, 
night_gprs_rate: double, 
new_user_flag: int, 
label: int, 
use_num1: double, 
use_num2: double, 
use_num: double, 
credit_5_tmp: int, 
credit_5: int, 
credit_4: double, 
credit_3: double, 
credit_2: double, 
credit_1: double, 
credit_0: double, 
sex_m_tmp: double, 
sex_m: int, 
sex_f: double

val b = data1.map(_.toString.toDouble)






val data = data2.toDF();