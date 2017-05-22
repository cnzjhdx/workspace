//ML库

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

pre_Data.show();
pre_Data.printSchema();

hiveCtx.sql(s"use jhzx_hive_db");

val pre_Data=hiveCtx.sql("select use_num,KD_RATE,AGE_RATE,CMCC_KD_CNT,HOME_DEPEND,cast(SEARCH_BJ_FLAG as Double) SEARCH_BJ_FLAG,cast(SEARCH_ZJ_FLAG as Double) SEARCH_ZJ_FLAG,AGE_FLAG1,AGE_FLAG2,age_flag3,age_flag4,upay_flag,cluster2,cluster3,ARPU,ownserv_flag,otherserv_flag,stay_flag,LABEL from hdx_jh_dataset_15 ");

val data1=pre_Data.rdd.map(x =>(x(0).toString.toDouble, x(1).toString.toDouble,x(2).toString.toDouble,x(3).toString.toDouble,x(4).toString.toDouble, x(5).toString.toDouble,x(6).toString.toDouble,x(7).toString.toDouble,x(8).toString.toDouble,x(9).toString.toDouble,x(10).toString.toDouble,x(11).toString.toDouble,x(12).toString.toDouble,x(13).toString.toDouble,x(14).toString.toDouble,x(15).toString.toDouble,x(16).toString.toDouble,x(17).toString.toDouble,x(18).toString.toDouble));

val data2=data1.map(x=>LabeledPoint(x._19,Vectors.dense(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._11,x._12,x._13,x._14,x._15,x._16,x._17,x._18)));

val data = data2.toDF();


val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data);
// Automatically identify categorical features, and index them.
//val featureIndexer = new VectorIndexer().setInputCol("use_num,KD_RATE,AGE_RATE,CMCC_KD_CNT,HOME_DEPEND,SEARCH_BJ_FLAG,SEARCH_ZJ_FLAG,AGE_FLAG1,AGE_FLAG2,age_flag3,age_flag4,upay_flag,cluster2,cluster3,ARPU,ownserv_flag,otherserv_flag,stay_flag").setOutputCol("indexedFeatures").setMaxCategories(4).fit(pre_Data); 
val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data); 

// features with > 4 distinct values are treated as continuous.fit(data2)

val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3));

val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures");

// Convert indexed labels back to original labels.
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels);

// Chain indexers and tree in a Pipeline
val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt, labelConverter));

// Train model.  This also runs the indexers.
val model = pipeline.fit(trainingData);

// Make predictions.
val predictions = model.transform(testData);

// Select example rows to display.
predictions.select("predictedLabel", "label", "features").show(5)

// Select (prediction, true label) and compute test error
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("precision");
val accuracy = evaluator.evaluate(predictions);
println("Test Error = " + (1.0 - accuracy));

val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel];
println("Learned classification tree model:\n" + treeModel.toDebugString);
predictions.select("predictedLabel", "label", "rawPrediction").show(5);
predictions.count();
predictions.
val DT_coverage=predictions.filter(predictions("predictedLabel") == 1 && predictions("predictedLabel")==predictions("label")).count()/predictions.filter(predictions("label") == 1).count();
val DT_shoot=predictions.filter(predictions("predictedLabel") == 1 && predictions("predictedLabel")==predictions("label")).count()/predictions.filter(predictions("predictedLabel") == 1).count();

