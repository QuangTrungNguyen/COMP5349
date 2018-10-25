#spark-submit --master yarn --deploy-mode client --num-executors 3 --executor-cores 3 DecisionTree.py 28

from pyspark.sql import SparkSession
from pyspark.ml.feature import PCA
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.ml.feature import Normalizer
import sys
import time


""" Function to calculate accuracy """
def calculate_accuracy(predictions, train=False):

    # Define evaluator
    evaluator = MulticlassClassificationEvaluator(
        labelCol="labels_indexed", predictionCol="prediction", metricName="accuracy")

    # Calculate and print accuracy
    accuracy = evaluator.evaluate(predictions)
    if train: # Training accuracy
        print("Training accuracy is {:0.2f}%".format(accuracy*100))
    else:
        print("Test accuracy is {:0.2f}%".format(accuracy*100))

""" Decision Tree Classifier """
def decision_tree_classifier(train_set, test_set):
    # Reference: https://spark.apache.org/docs/2.1.0/ml-classification-regression.html

    # Index the labels of the whole train set
    label_indexer = StringIndexer(inputCol="_c0", outputCol="labels_indexed").fit(train_set)
    # Index the features of the whole train set
    feature_indexer = VectorIndexer(inputCol="pca", outputCol="features_indexed").fit(train_set)

    # Decision Tree model
    DT_model = DecisionTreeClassifier(labelCol="labels_indexed", featuresCol="features_indexed", maxDepth=15,
                                      minInstancesPerNode=30, cacheNodeIds=True)

    # Combine indexers and decision tree model in a Pipeline
    pipeline = Pipeline(stages=[label_indexer, feature_indexer, DT_model])

    # Fit the train set using indexers in the pipeline
    model = pipeline.fit(train_set)

    # Test the model on train set
    print('Predicting results...')
    predictions = model.transform(train_set)
    # Calculate training accuracy
    calculate_accuracy(predictions,True)

    # Predict labels for test set
    predictions = model.transform(test_set)
    # Calculate test accuracy
    calculate_accuracy(predictions)



if __name__ == "__main__":

    # Check runtime
    start = time.time()

    # Initiate the spark #
    spark = SparkSession \
        .builder \
        .appName("Python Spark Decision Tree classifier") \
        .getOrCreate()

    # Store the dimension from the console
    dimension = int(sys.argv[1])

    ###Load the data###
    test_datafile = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/MNIST/Test-28x28.csv"
    test_labelfile= "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/MNIST/Test-label.csv"
    train_datafile = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/MNIST/Train-28x28.csv"
    train_labelfile = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/MNIST/Train-label.csv"

    # Read all csv files from the server
    train_df = spark.read.csv(train_datafile,header=False,inferSchema="true")
    train_labeldf = spark.read.csv(train_labelfile,header=False,inferSchema="true")
    test_df = spark.read.csv(test_datafile,header=False,inferSchema="true")
    test_labeldf = spark.read.csv(test_labelfile,header=False,inferSchema="true")

    # Define the normalizer to normalize the data
    normalizer = Normalizer(inputCol="pca", outputCol="norm_pca", p=1.0)

    # Perform dimentionality reduction on TRAIN data using PCA
    print('Performing PCA on train set...')
    # Convert into vectors
    assembler_train = VectorAssembler(inputCols=train_df.columns,outputCol="features")
    train_vectors = assembler_train.transform(train_df).select("features")
    # PCA & Normalizing
    pca = PCA(k=dimension, inputCol="features", outputCol="pca")
    pca_model = pca.fit(train_vectors)
    pca_result_train = normalizer.transform(pca_model.transform(train_vectors).select('pca'))

    # Perform dimentionality reduction on TEST data using PCA
    print('Performing PCA on test set...')
    # Convert into vectors
    assembler_test = VectorAssembler(inputCols=test_df.columns,outputCol="features")
    test_vectors = assembler_test.transform(test_df).select("features")
    # PCA & Normalizing
    pca_result_test = normalizer.transform(pca_model.transform(test_vectors).select('pca'))
    print('Reduced dimension is {}'.format(dimension))

    # Add corresponding index column to data and labels
    x_train = pca_result_train.withColumn("id", monotonically_increasing_id())
    y_train = train_labeldf.withColumn("id", monotonically_increasing_id())
    x_test = pca_result_test.withColumn("id", monotonically_increasing_id())
    y_test = test_labeldf.withColumn("id", monotonically_increasing_id())

    # Join data with labels using index column
    train_set = y_train.join(x_train, y_train.id == x_train.id)
    test_set = y_test.join(x_test, y_test.id == x_test.id)

    # Fit model and predict results
    print('Training Decision Tree model...')
    decision_tree_classifier(train_set,test_set)

    # Print runtime
    end = time.time()
    print ("Total time to run is {:0.2f} seconds".format(end-start))

    spark.stop()
