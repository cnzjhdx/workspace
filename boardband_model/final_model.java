import org.apache.spark.{SparkConf, SparkContext, SparkException};
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.feature.{StringIndexer, IndexToString, VectorIndexer};
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;

val conf = new SparkConf();

@transient val hiveCtx = new HiveContext(sc);

hiveCtx.sql(s"use jhzx_hive_db");