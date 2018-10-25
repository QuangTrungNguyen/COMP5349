#spark-submit --master yarn --deploy-mode client --num-executors 3 --executor-cores 3 KnnClassifier.py 50 5 10 4

from pyspark.sql import SparkSession
from pyspark.ml.feature import PCA
from pyspark.ml.feature import VectorAssembler
from collections import Counter
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import numpy as np
import sys
import time

#############################################
# Predict the labe usint euclidean distance #
#############################################
def predict(data_test):
    # create list for distances and targets
    targets = []
    temp_np_array = np.arange(len(data_trains))
    temp_np_array_one = np.array(temp_np_array).reshape(len(data_trains),1)

    dis = np.sqrt(np.sum(np.square(data_trains - data_test), axis=2))
    combine_dis = np.hstack((dis, temp_np_array_one))
    sorted_dis = combine_dis[combine_dis[:,0].argsort()].tolist()

    # make a list of the k neighbors' targets
    for i in range(key):
        index = int(sorted_dis[i][1])
        targets.append(int(label_trains[index]))

    return int(Counter(targets).most_common(1)[0][0])


######################
## Find k neighbors ##
######################

def k_nearest_neighbor():
    data_test_repart = pca_result_test.repartition(worker_num)

    udf_func = udf(predict,returnType=IntegerType())
    pca_new = pca_result_test.withColumn("prediction", lit(udf_func('pca')))

    #Loop to find predictions
    predictions = np.array(pca_new.select('prediction').collect())
    print("Building a confusion Matrix...")
    build_matrix(predictions)
def build_matrix(predictions):

    correct_num = 0;

    for i in range(len(label_tests)):
        confusion_matrix[label_tests[i][0]][int(predictions[i][0])] += 1
        if (int(predictions[i][0])) == label_tests[i][0]:
            correct_num += 1
            confusion_matrix[label_tests[i][0]][10] += 1
            confusion_matrix[10][int(predictions[i][0])] += 1
        else:
            confusion_matrix[label_tests[i][0]][11] += 1
            confusion_matrix[11][int(predictions[i][0])]+= 1

    print("Print out the result...")
    print_result(correct_num)

def print_result(correct_num):
    acc = (float(correct_num)/float(len(label_tests)))*100
    print ("")
    print ("The number of training data is {} and the number of testing data is {}".format(len(data_trains), len(label_tests)))
    print ("The dimension is {} and the k value is {}".format(dimension, key))
    print ("The accuracy is {:0.2f}% ".format(acc))

    for i in range(10):
        if confusion_matrix[i][10] + confusion_matrix[i][11] == 0:
            precision = 0
        else:
            precision = float(confusion_matrix[i][10]/(confusion_matrix[i][10]+confusion_matrix[i][11]))
        if confusion_matrix[10][i] + confusion_matrix[11][i] == 0:
            recall = 0
        else:
            recall = float(confusion_matrix[10][i]/(confusion_matrix[10][i]+confusion_matrix[11][i]))
        if precision + recall == 0:
            f1_score = 0
        else:
            f1_score = float(2*((precision*recall)/(precision + recall)))
        print ("the label is {}. Precision is {:0.2f}. Recall is {:0.2f}, f1 score is {:0.2f}".format(i, precision, recall, f1_score))
    print ("")

if __name__ == "__main__":

    #check for the timing
    start = time.time()
    processes_num = int(sys.argv[3])
    executors_num = int(sys.argv[4])

    ######################
    # Initiate the spark #
    ######################

    spark = SparkSession \
        .builder \
        .appName("Python Spark knn classifier") \
        .getOrCreate()


    ####################################
    # Initiate the valiables and lists #
    ####################################

    #Predictions table
    predictions = []

    #Confusion table to calculate the precision, recall, f1-score
    confusion_matrix = [[0.0]*12 for i in range(12)]

    #Store the dimension and k from the consloe
    dimension = int(sys.argv[1])
    key = int(sys.argv[2])

    worker_num = executors_num * processes_num


    ###################
    #  Load the data  #
    ###################

    test_datafile = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/MNIST/Test-28x28.csv"
    test_labelfile= "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/MNIST/Test-label.csv"
    train_datafile = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/MNIST/Train-28x28.csv"
    train_labelfile = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/MNIST/Train-label.csv"

    #Read all csv files from the server
    train_df = spark.read.csv(train_datafile,header=False,inferSchema="true")
    train_labeldf = spark.read.csv(train_labelfile,header=False,inferSchema="true")
    test_df = spark.read.csv(test_datafile,header=False,inferSchema="true")
    test_labeldf = spark.read.csv(test_labelfile,header=False,inferSchema="true")

    #Transform the train data in to 2D space using dimension from the console
    assembler = VectorAssembler(inputCols=train_df.columns,outputCol="features")
    train_vectors = assembler.transform(train_df).select("features")
    pca = PCA(k=dimension, inputCol="features", outputCol="pca")
    model = pca.fit(train_vectors)
    pca_result = model.transform(train_vectors).select('pca')

    #Save the PCA result as an array

    local_pca_train=np.array(pca_result.collect())
    #Change the type of value in array to float
    data_trains = local_pca_train.astype(np.float64)


    #Transform the test data in to 2D space using dimension from the console
    assembler_test = VectorAssembler(inputCols=test_df.columns,outputCol="features")
    train_vectors_test = assembler_test.transform(test_df).select("features")
    pca_result_test = model.transform(train_vectors_test).select('pca')

    #Store the dataframe as array
    label_trains=np.array(train_labeldf.collect())
    label_tests=np.array(test_labeldf.collect())


    ########################################
    # Find out the accuracy rate uinsg KNN #
    ########################################

    if key > 1:

        print("Start to find K neighbor...")
        k_nearest_neighbor()
        end = time.time()
        print ("Total time to run is {} minutes".format((end-start)/60))

    else:

        print ("Key is too small to classify")

    print("Successfully finished the process")

    spark.stop()
