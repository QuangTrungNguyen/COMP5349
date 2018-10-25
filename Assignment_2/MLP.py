#spark-submit --master yarn --deploy-mode client --num-executors 3 --executor-cores 3 MLP.py 50 75 100

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import monotonically_increasing_id
import sys
import time

""" Function to calculate accuracy """
def calculate_accuracy(predictions, train=False):

    # Define an evaluator
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy")

    # Calculate and print test accuracy
    if not train:
        print("Test accuracy is {:0.2f}%".format(evaluator.evaluate(predictions.select("prediction", "label"))*100))
    # Calculate and print train accuracy
    if train:
        print("Train accuracy is {:0.2f}%".format(evaluator.evaluate(predictions.select("prediction", "label"))*100))

""" Multilayer Perceptron Classifier """
def multilayer_perceptron_classifier(train_set, test_set, hidden_size=100):
    # Reference: https://spark.apache.org/docs/2.1.0/ml-classification-regression.html

    '''
    Define layers for the neural net
        Input layer = n_features of 784
        Hidden layer = any value between 50-100 / default = 100
        Output layer = n_classes of 10
    '''
    layers = [784, hidden_size, 10]

    # Define a multilayer percentron classiifer
    MLP = MultilayerPerceptronClassifier(maxIter=100, layers=layers, blockSize=128, seed=1000)

    # Train the model
    model = MLP.fit(train_set)

    # Prediction on train data
    print('Predicting results...')
    predictions = model.transform(train_set)
    # Calculate train accuracy
    calculate_accuracy(predictions,True)

    # Prediction on test data
    print('Predicting results...')
    predictions = model.transform(test_set)
    # Calculate test accuracy
    calculate_accuracy(predictions)


if __name__ == "__main__":

    # Initiate the spark #
    spark = SparkSession \
        .builder \
        .appName("Python Spark Multilayer Perceptron classifier") \
        .getOrCreate()

    # Store the 3 different sizes of hidden layer from console
    hidden_sizes = []
    for i in range(1,len(sys.argv)):
        hidden_sizes.append(int(sys.argv[i]))

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

    # Convert train + set data into vectors
    assembler_train = VectorAssembler(inputCols=train_df.columns,outputCol="features")
    train_vectors = assembler_train.transform(train_df).select("features")
    assembler_test = VectorAssembler(inputCols=test_df.columns,outputCol="features")
    test_vectors = assembler_test.transform(test_df).select("features")

    # Rename column '_c0' of labels with 'label'
    train_label = train_labeldf.withColumnRenamed("_c0", "label")
    test_label = test_labeldf.withColumnRenamed("_c0", "label")

    # Add corresponding index column to data and labels
    x_train = train_vectors.withColumn("id", monotonically_increasing_id())
    y_train = train_label.withColumn("id", monotonically_increasing_id())
    x_test = test_vectors.withColumn("id", monotonically_increasing_id())
    y_test = test_label.withColumn("id", monotonically_increasing_id())

    # Join data with labels using index column
    train_set = y_train.join(x_train, y_train.id == x_train.id)
    test_set = y_test.join(x_test, y_test.id == x_test.id)

    # Train classifier and predict results
    print('--- Multilayer Perceptron Classifier ---\n')
    for size in hidden_sizes:
        print('Training classifier with Hidden Layer size of {}'.format(size))
        # Check runtime
        start = time.time()
        multilayer_perceptron_classifier(train_set, test_set, size)
        # Print runtime
        end = time.time()
        print ("Total time to run is {:0.2f} seconds\n".format(end-start))

    spark.stop()
