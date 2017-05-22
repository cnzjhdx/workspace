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
predictions.show(20);
predictions.groupBy("predictedLabel").count().show()

// Select (prediction, true label) and compute test error
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("precision");
val accuracy = evaluator.evaluate(predictions);
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("recall");

println("Test Error = " + (1.0 - accuracy));

val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel];
println("Learned classification tree model:\n" + treeModel.toDebugString);
predictions.select("predictedLabel", "label", "rawPrediction").show(5);

val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("f1");
val accuracy = evaluator.evaluate(predictions);