# Author: Quang Trung Nguyen (470518197)
#!/usr/bin/python3

import sys
import csv


def tag_mapper():
    """ This mapper select category and return the category-country-video information.
    Input format: 18-column dataset, separated by comma
    Output format: category \t country \t video_id
    """

    video_list = [] # video_ids of videos trending in 1st country
    temp = [] # array to store outputs of 2nd country before printing 

    for line in (sys.stdin):
        parts = list(csv.reader([line]))
        category = parts[0][5].strip()
        video = parts[0][0].strip()
        country = parts[0][17].strip()
        
        # print outputs of 1st country
        if country == sys.argv[1]:
            print("{}\t{}\t{}".format(category, country, video))
            if video not in video_list:
                video_list.append(video)
        # store outputs of 2nd country
        elif country == sys.argv[2]:
            temp.append([category,country,video])

    # print outputs of 2nd country
    for item in temp:
        if item[2] in video_list:
            print("{}\t{}\t{}".format(item[0], item[1], item[2]))


if __name__ == "__main__":
    tag_mapper()
