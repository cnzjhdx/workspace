
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


val conf = new SparkConf();

@transient val hiveCtx = new HiveContext(sc);

hiveCtx.sql(s"use jhzx_hive_db");




val pre_Data=hiveCtx.sql("select use_num,KD_RATE,AGE_RATE,CMCC_KD_CNT,HOME_DEPEND,SEARCH_BJ_FLAG,SEARCH_ZJ_FLAG,AGE_FLAG1,AGE_FLAG2,age_flag3,age_flag4,upay_flag,cluster2,cluster3,ARPU,ownserv_flag,otherserv_flag,stay_flag,new_user_flag,LABEL from hdx_result_15 ");

val data1=pre_Data.rdd.map(x =>(x(0).toString.toDouble, x(1).toString.toDouble,x(2).toString.toDouble,x(3).toString.toDouble,x(4).toString.toDouble, x(5).toString.toDouble,x(6).toString.toDouble,x(7).toString.toDouble,x(8).toString.toDouble,x(9).toString.toDouble,x(10).toString.toDouble,x(11).toString.toDouble,x(12).toString.toDouble,x(13).toString.toDouble,x(14).toString.toDouble,x(15).toString.toDouble,x(16).toString.toDouble,x(17).toString.toDouble,x(18).toString.toDouble,x(19).toString.toDouble));

val data2=data1.map(x=>LabeledPoint(x._20,Vectors.dense(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._11,x._12,x._13,x._14,x._15,x._16,x._17,x._18,x._19)));


val splits = data2.randomSplit(Array(0.7, 0.3), seed = 11L);

val (trainingData, testData) = (splits(0).cache(), splits(1).cache());


val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "gini"
val maxDepth = 7
val maxBins = 128
val modelDT = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,impurity, maxDepth, maxBins)
  
  val DT_Preds = testData.map { point =>
  val prediction = modelDT.predict(point.features)
  (point.label, prediction)
}


val accuracy_DT = 1.0 * DT_Preds.filter(x => x._1 == x._2).count() / testData.count() //0.88

val DT_r = 1.0 * DT_Preds.filter(x => (x._1 == 1.0) && (x._1 == x._2) ).count() / DT_Preds.filter(x => x._1 == 1.0 ).count()
val DT_p = 1.0 * DT_Preds.filter(x => (x._1 == 1.0) && (x._1 == x._2) ).count() / DT_Preds.filter(x => x._2 == 1.0 ).count()



val metrics_DT = new BinaryClassificationMetrics(DT_Preds)
val auROC_DT = metrics_DT.areaUnderROC()


 //如何增加一列和写入数据库
 
 res2.withColumn("cc",col("id")*0)


data.toDF().registerTempTable("table1")  

res61.registerTempTable("table1")

hiveCtx.sql("create table hdx_test as select * from table1")

val b = hiveCtx.sql("select * from hdx_result_15")

b.registerTempTable("test")

val a=hiveCtx.sql("insert into hdx_test select * from test")