# Author: Quang Trung Nguyen (470518197)
#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Invalid number of parameters!"
    echo "Usage: ./youtube_driver.sh [input_location] [output_location]"
    exit 1
fi

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.9.0.jar \
-D mapreduce.job.reduces=3 \
-D mapreduce.job.name='Youtube Trending Correlation' \
-file youtube_mapper.py \
-mapper "python youtube_mapper.py US GB" \
-file youtube_reducer.py \
-reducer "python youtube_reducer.py US GB" \
-input $1 \
-output $2
