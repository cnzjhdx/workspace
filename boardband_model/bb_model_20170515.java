
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

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}；



val conf = new SparkConf();

@transient val hiveCtx = new HiveContext(sc);

hiveCtx.sql(s"use jhzx_hive_db");

val pre_Data=hiveCtx.sql("select use_num,KD_RATE,AGE_RATE,CMCC_KD_CNT,HOME_DEPEND,SEARCH_BJ_FLAG,SEARCH_ZJ_FLAG,AGE_FLAG1,AGE_FLAG2,age_flag3,age_flag4,upay_flag,cluster2,cluster3,ARPU,ownserv_flag,otherserv_flag,stay_flag,LABEL from jcc_jh_dataset_15 ");

val data1=pre_Data.rdd.map(x =>(x(0).toString.toDouble, x(1).toString.toDouble,x(2).toString.toDouble,x(3).toString.toDouble,x(4).toString.toDouble, x(5).toString.toDouble,x(6).toString.toDouble,x(7).toString.toDouble,x(8).toString.toDouble,x(9).toString.toDouble,x(10).toString.toDouble,x(11).toString.toDouble,x(12).toString.toDouble,x(13).toString.toDouble,x(14).toString.toDouble,x(15).toString.toDouble,x(16).toString.toDouble,x(17).toString.toDouble,x(18).toString.toDouble));

val data2=data1.map(x=>LabeledPoint(x._19,Vectors.dense(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._11,x._12,x._13,x._14,x._15,x._16,x._17,x._18)));

val splits = data2.randomSplit(Array(0.7, 0.3), seed = 11L);

val (trainingData, testData) = (splits(0).cache(), splits(1).cache());



//val numIterations = 100;
 
//val model_log = LogisticRegressionWithSGD.train(trainingData, numIterations);



val lr=new LogisticRegressionWithLBFGS().setIntercept(true);

val model_log=lr.run(trainingData);

println("weight:%s, intercept:%s ".format(model_log.weights,model_log.intercept));

val log_Preds = testData.map { x =>
  val pred = model_log.predict(x.features)
  (pred, x.label)
}

val accuracy_log = 1.0 * log_Preds.filter(x => x._1 == x._2).count() / testData.count()

val log_acc = 1.0 * log_Preds.filter(x => (x._1 == 1.0) && (x._1 == x._2) ).count() / log_Preds.filter(x => x._2 == 1.0 ).count()

val metrics_log = new BinaryClassificationMetrics(log_Preds)
val auROC_log = metrics_log.areaUnderROC()

println("Area under ROC = " + auROC_log)

//0.639


//descion treee
 
val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "gini"
val maxDepth = 7
val maxBins = 128
val modelDT = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
  impurity, maxDepth, maxBins)
  
  val DT_Preds = testData.map { point =>
  val prediction = modelDT.predict(point.features)
  (point.label, prediction)
}


val accuracy_DT = 1.0 * DT_Preds.filter(x => x._1 == x._2).count() / testData.count() //0.88
val DT_acc = 1.0 * DT_Preds.filter(x => (x._1 == 1.0) && (x._1 == x._2) ).count() / DT_Preds.filter(x => x._1 == 1.0 ).count()
val DT_acc = 1.0 * DT_Preds.filter(x => (x._1 == 1.0) && (x._1 == x._2) ).count() / DT_Preds.filter(x => x._2 == 1.0 ).count()
val metrics_DT = new BinaryClassificationMetrics(DT_Preds)
val auROC_DT = metrics_DT.areaUnderROC()

modelDT.clearThreshold();

//0.819 0.821
 
 //naive bayes
 
 val modelNB = NaiveBayes.train(trainingData, lambda = 1.0, modelType = "multinomial")

 val NB_Preds = testData.map(p => (modelNB.predict(p.features), p.label))
val accuracyNB = 1.0 * NB_Preds.filter(x => x._1 == x._2).count() / testData.count() //0.759
val NB_acc = 1.0 * NB_Preds.filter(x => (x._1 == 1.0) && (x._1 == x._2) ).count() / NB_Preds.filter(x => x._1 == 1.0 ).count()

val metrics_NB = new BinaryClassificationMetrics(NB_Preds)
val auROC_NB = metrics_NB.areaUnderROC()
//0.594
//GBT

import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

val boostingStrategy = BoostingStrategy.defaultParams("Classification")
boostingStrategy.numIterations = 5 // Note: Use more iterations in practice.
boostingStrategy.treeStrategy.numClasses = 2
boostingStrategy.treeStrategy.maxDepth = 5
// Empty categoricalFeaturesInfo indicates all features are continuous.
boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

val model_GBT = GradientBoostedTrees.train(trainingData, boostingStrategy)

// Evaluate model on test instances and compute test error
val GBT_Preds = testData.map { point =>
  val prediction = model_GBT.predict(point.features)
  (point.label, prediction)
}
val accuracy_GBT = 1.0 * GBT_Preds.filter(x => x._1 == x._2).count() / testData.count()

val p_acc = 1.0 * GBT_Preds.filter(x => (x._1 == 1.0) && (x._1 == x._2) ).count() / GBT_Preds.filter(x => x._1 == 1.0 ).count()


val metrics_GBT = new BinaryClassificationMetrics(GBT_Preds)
val auROC_GBT = metrics_GBT.areaUnderROC()
//0.808
//
//SVM
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}

val numIterations = 100
val model_SVM = SVMWithSGD.train(trainingData, numIterations)

// Clear the default threshold.
model.clearThreshold()

// Compute raw scores on the test set.
val svmLabels = trainingData.map { point =>
  val score = model_SVM.predict(point.features)
  (score, point.label)
}

// Get evaluation metrics.
val metrics_SVM = new BinaryClassificationMetrics(svmLabels)
val auROC_SVM = metrics_SVM.areaUnderROC()

val SVM_acc = 1.0 * svmLabels.filter(x => (x._1 == 1.0) && (x._1 == x._2) ).count() / svmLabels.filter(x => x._1 == 1.0 ).count()

//0.5
//
//
//数据验证
//
val pre_Data1=hiveCtx.sql("select use_num,KD_RATE,AGE_RATE,CMCC_KD_CNT,HOME_DEPEND,SEARCH_BJ_FLAG,SEARCH_ZJ_FLAG,AGE_FLAG1,AGE_FLAG2,age_flag3,age_flag4,upay_flag,cluster2,cluster3,ARPU,ownserv_flag,otherserv_flag,stay_flag,LABEL from jcc_jh_dataset_5 ");

val data3=pre_Data1.rdd.map(x =>(x(0).toString.toDouble, x(1).toString.toDouble,x(2).toString.toDouble,x(3).toString.toDouble,x(4).toString.toDouble, x(5).toString.toDouble,x(6).toString.toDouble,x(7).toString.toDouble,x(8).toString.toDouble,x(9).toString.toDouble,x(10).toString.toDouble,x(11).toString.toDouble,x(12).toString.toDouble,x(13).toString.toDouble,x(14).toString.toDouble,x(15).toString.toDouble,x(16).toString.toDouble,x(17).toString.toDouble,x(18).toString.toDouble));

val data_test=data3.map(x=>LabeledPoint(x._19,Vectors.dense(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._11,x._12,x._13,x._14,x._15,x._16,x._17,x._18)));

  val DT_Preds = data_test.map { point =>
  val prediction = modelDT.predict(point.features)
  (point.label, prediction)
}


val accuracy_DT = 1.0 * DT_Preds.filter(x => x._1 == x._2).count() / data_test.count() //0.88
val DT_acc = 1.0 * DT_Preds.filter(x => (x._1 == 1.0) && (x._1 == x._2) ).count() / DT_Preds.filter(x => x._1 == 1.0 ).count()
val DT_acc = 1.0 * DT_Preds.filter(x => (x._1 == 1.0) && (x._1 == x._2) ).count() / DT_Preds.filter(x => x._2 == 1.0 ).count()
val metrics_DT = new BinaryClassificationMetrics(DT_Preds)
val auROC_DT = metrics_DT.areaUnderROC()


val pre_Data=hiveCtx.sql("select use_num,KD_RATE,AGE_RATE,CMCC_KD_CNT,HOME_DEPEND,SEARCH_BJ_FLAG,SEARCH_ZJ_FLAG,AGE_FLAG1,AGE_FLAG2,age_flag3,age_flag4,upay_flag,cluster2,cluster3,ARPU,ownserv_flag,otherserv_flag,stay_flag,LABEL from jcc_jh_dataset_15 ");

val data1=pre_Data.rdd.map(x =>(x(0).toString.toDouble, x(1).toString.toDouble,x(2).toString.toDouble,x(3).toString.toDouble,x(4).toString.toDouble, x(5).toString.toDouble,x(6).toString.toDouble,x(7).toString.toDouble,x(8).toString.toDouble,x(9).toString.toDouble,x(10).toString.toDouble,x(11).toString.toDouble,x(12).toString.toDouble,x(13).toString.toDouble,x(14).toString.toDouble,x(15).toString.toDouble,x(16).toString.toDouble,x(17).toString.toDouble,x(18).toString.toDouble));

val data2=data1.map(x=>LabeledPoint(x._19,Vectors.dense(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._11,x._12,x._13,x._14,x._15,x._16,x._17,x._18)));

val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "gini"
val maxDepth = 7
val maxBins = 128
val modelDT = DecisionTree.trainClassifier(data2, numClasses, categoricalFeaturesInfo,
  impurity, maxDepth, maxBins)
  
  val DT_Preds = data_test.map { point =>
  val prediction = modelDT.predict(point.features)
  (point.label, prediction)
}

val modelDT = DecisionTree.trainClassifier(data_test, numClasses, categoricalFeaturesInfo,
  impurity, maxDepth, maxBins)

val DT_Preds = data2.map { point =>
  val prediction = modelDT.predict(point.features)
  (point.label, prediction)
}

//随机森林
//
//

import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;

val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val numTrees = 3 // Use more in practice.
val featureSubsetStrategy = "auto" // Let the algorithm choose.
val impurity = "gini"
val maxDepth = 7
val maxBins = 128

val modelRF = RandomForest.trainClassifier(data2, numClasses, categoricalFeaturesInfo,
  numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

  val RF_Preds = data_test.map { point =>
  val prediction = modelRF.predict(point.features)
  (point.label, prediction)
}

val accuracy_RF = 1.0 * RF_Preds.filter(x => x._1 == x._2).count() / data_test.count() //0.88
val RF_acc = 1.0 * RF_Preds.filter(x => (x._1 == 1.0) && (x._1 == x._2) ).count() / RF_Preds.filter(x => x._1 == 1.0 ).count()
val RF_acc = 1.0 * RF_Preds.filter(x => (x._1 == 1.0) && (x._1 == x._2) ).count() / RF_Preds.filter(x => x._2 == 1.0 ).count()
val metrics_RF = new BinaryClassificationMetrics(RF_Preds)
val auROC_RF = metrics_RF.areaUnderROC()

val modelRF = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
  numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

  val RF_Preds = testData.map { point =>
  val prediction = modelRF.predict(point.features)
  (point.label, prediction)
}

val accuracy_RF = 1.0 * RF_Preds.filter(x => x._1 == x._2).count() / testData.count() //0.88
val RF_acc = 1.0 * RF_Preds.filter(x => (x._1 == 1.0) && (x._1 == x._2) ).count() / RF_Preds.filter(x => x._1 == 1.0 ).count()
val RF_acc = 1.0 * RF_Preds.filter(x => (x._1 == 1.0) && (x._1 == x._2) ).count() / RF_Preds.filter(x => x._2 == 1.0 ).count()
val metrics_RF = new BinaryClassificationMetrics(RF_Preds)
val auROC_RF = metrics_RF.areaUnderROC()