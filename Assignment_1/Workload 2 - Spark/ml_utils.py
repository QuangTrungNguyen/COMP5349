# Author: Quang Trung Nguyen (470518197)
"""
This module includes a few functions used in calculate view increase of videos between 1st and 2nd trending
"""
import csv


def extractVideoViews(record):
    """ This function converts entries of ALLvideos.csv into key,value pair of the following format
    ((country, video_id), views) where (country, video_id) is the composite key
    Args:
        record (str): A row of CSV file, with 18 columns separated by comma
    Returns:
        The return value is a tuple ((country, video_id), views)
    """
    parts = list(csv.reader([record.encode('utf-8')]))
    country = parts[0][17]
    video = parts[0][0]
    views = parts[0][8]
    return (country + ', ' + video, views)

def calculateViewIncrease(line):
    """This function compute the videos view increase between 1st and 2nd trending
    Args:
        line (tuple): a tuple of ((country, video_id), views)
    Returns:
        The return value is a string containing values of country, video_id, percent 
    """
    first_view = int(line[1].split(',')[0].strip())
    second_view = int(line[1].split(',')[1].strip())
    # calculate increase percentage
    percent = second_view / (first_view * 1.0) * 100
    country = line[0].strip().split(',')[0]
    video_id = line[0].strip().split(',')[1]

    return "{}; {}, {:0.2f}%".format(country, video_id, percent)

def increaseThresholdFilter(record):
    """This function filter the record that has view increase of at least 1000%
    Args:
        record: a string containing values of country, video_id, percent 
    Returns:
        The record (string) that has percent >= 1000
    """
    increase = record.split(';')[1].split(',')[1].strip('%').strip()
    if float(increase) > 1000.0:
        return record


