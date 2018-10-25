# Author: Quang Trung Nguyen (470518197)
# Calculate the view increase of video between 1st and 2nd trending by country
# In order to run this, we use spark-submit, below is the 
# spark-submit  \
#   --master local[2] \
    #   VideosViewIncrease.py
#   --input input-path
#   --output outputfile

from pyspark import SparkContext
from ml_utils import *
import argparse

if __name__ == "__main__":
    sc = SparkContext(appName="Impact of Trending on View Number")
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="the input path",
                        default='hdfs://192.168.1.4:9000/comp5349/') # Please update the output accordingly
    parser.add_argument("--output", help="the output path", 
                        default='Workload_2_Output') 
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output
    data = sc.textFile(input_path + "ALLvideos.csv")
    videoViews = data.map(extractVideoViews)
    # Group all view counts of each video to a single entry, sorted by country
    # then filter video with more than 1 trending
    groupedVideosByCountry = videoViews.reduceByKey(lambda a, b: a + ', ' + b).filter(lambda x: len(x[1].split(',')) > 1)
    # Calculate view increase, sorted by descending view
    sortedViewIncrement = groupedVideosByCountry.map(calculateViewIncrease).filter(increaseThresholdFilter).sortBy(lambda x: x.split(';')[1].split(',')[1].strip('%').strip(), ascending=False).sortBy(lambda x: x[0:2])
    # Save files to output path
    sortedViewIncrement.saveAsTextFile(output_path)
