# Author: Quang Trung Nguyen (470518197)
#!/usr/bin/python3

import sys


def read_map_output(file):
    """ Return an iterator for key, value pair extracted from file (sys.stdin).
    Input format:  key \t value_1 \t value_2
    Output format: (key, value_1, value_2)
    """
    for line in file:
        yield line.strip().split("\t")


def tag_reducer():
    """ This reducer, for each category, counts # of trending videos in country 1, country 2
    and calculate % in country 2
    Input format: category \t country \t video_id
    Output format: category \t count_1 (country 1) \t percent (in country 2)
    """
    current_cat = ""
    count_1 = 0 # total videos in country 1
    count_2 = 0 # number of videos also trending in country 2
    country = ""

    for cat, country_id, video in read_map_output(sys.stdin):
        # Check if the category read is the same as the category currently being processed
        if current_cat != cat:

            # If this is the first line (indicated by the fact that current_cat will have the default value of "",
            # we do not need to output the count yet
            if current_cat != "":
                percent = -1
                # if country 1 has videos in this category, calculate % in country 2
                if count_1 != 0:
                    percent = count_2 / (count_1 * 1.0) * 100
                if percent != -1:
                    output = current_cat + "; "
                    output += "total: {}; {:0.2f}% in {}\n".format(count_1, percent, sys.argv[2])
                    print(output.strip())

            # Reset the category being processed and clear the count variables for the new tag
            current_cat = cat
            count_1 = 0
            count_2 = 0

        country = country_id
        if country == sys.argv[1]:
            count_1 += 1
        elif country == sys.argv[2]:
            count_2 += 1

    # We need to output the count for the last category. However, we only want to do this if the for loop is called.
    if current_cat != "":
        percent = -1
        if count_1 != 0:
            percent = count_2 / (count_1 * 1.0) * 100
        if percent != -1:
            output = current_cat + "; "
            output += "total: {}; {:0.2f}% in {}\n".format(count_1, percent, sys.argv[2])
            print(output.strip())

if __name__ == "__main__":
    tag_reducer()
