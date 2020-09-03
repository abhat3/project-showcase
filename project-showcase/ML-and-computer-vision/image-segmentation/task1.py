"""
K-Means Segmentation Problem
(Due date: Nov. 25, 11:59 P.M., 2019)
The goal of this task is to segment image using k-means clustering.

Do NOT modify the code provided to you.
Do NOT import ANY library or API besides what has been listed.
Hint: 
Please complete all the functions that are labeled with '#to do'. 
You are allowed to add your own functions if needed.
You should design you algorithm as fast as possible. To avoid repetitve calculation, you are suggested to depict clustering based on statistic histogram [0,255]. 
You will be graded based on the total distortion, e.g., sum of distances, the less the better your clustering is.
"""


import utils
import numpy as np
import json
import time

def getAllCombinations(input_points):
    # Find all combinations of length 2 (since k = 2) in input_points
    # i.e., nC2 combinations, where n is length  of input_points
    # input points is a set of points in the image
    input_points_copy = list(input_points)
    all_points = []
    while len(input_points_copy) > 1:
        current = input_points_copy[0]
        input_points_copy.pop(0)
        for each in input_points_copy:
            all_points.append([current, each])        
    return all_points

def calcDistanceAbs(i, j):
    # calc absolute distance
    return abs(i - j)

def getAllEuclideanDistances():
    # all differences between pixels pre-computed for faster execution
    dist_matrix = [[-1 for i in range(256)] for i in range(256)] 
    for i in range(len(dist_matrix)):
        for j in range(len(dist_matrix[0])):
            if dist_matrix[i][j] == -1 or dist_matrix[j][i] == -1:
                if i == j:
                    dist_matrix[i][i] = 0
                else:
                    dist = calcDistanceAbs(i, j)
                    dist_matrix[i][j] = dist
                    dist_matrix[j][i] = dist
            else:
                continue
    return dist_matrix

def calcFrequency(reshaped_img):
    # frequency of each pixel in the image
    frequency_dict = dict()
    for pixel in reshaped_img:
        if pixel in frequency_dict:
            frequency_dict[pixel] = frequency_dict[pixel] + 1
        else:
            frequency_dict[pixel] = 1
    return frequency_dict

def closestCluster(center0, center1, pixel, dist_matrix):
    # get distance between center and pixel
    dist0 = dist_matrix[center0][pixel]
    dist1 = dist_matrix[center1][pixel]
    if dist0 == dist1:
        return 0 if center0 < center1 else 1
    if dist0 < dist1:
        return 0
    else:
        return 1

def calcCenters(clustering_labels, center0_old, center1_old, frequency_dict):
    # calc new cluster by taking mean of the pixel in the current cluster    
    cluster0_sum = 0
    cluster1_sum = 0
    len0 = 0
    len1 = 0
    for each in clustering_labels:
        num_each = frequency_dict[each[0]]
        if each[1] == 0:
            cluster0_sum = cluster0_sum + each[0] * num_each
            len0 += num_each
        else:
            cluster1_sum = cluster1_sum + each[0] * num_each
            len1 += num_each
    if len0 == 0:
        center0 = center0_old
    else:
        center0 = cluster0_sum // len0
    if len1 == 0:
        center1 = center1_old
    else:
        center1 = cluster1_sum // len1
    return center0, center1

def calcSum(clustering_labels, center0, center1, abs_matrix, frequency_dict):
    # calc sum of all distances between cluster centers and their pixels 
    center0_sum = 0
    center1_sum = 0
    for each in clustering_labels:
        num_each = frequency_dict[each[0]]
        if each[1] == 0:
            center0_sum = center0_sum + abs_matrix[each[0]][center0] * num_each
        else:
            center1_sum = center1_sum + abs_matrix[each[0]][center1] * num_each
    return center0_sum + center1_sum

def calcAllLabels(label_dict, reshaped_img):
    # map all pixels to their corresponding labels
    all_labels = []
    for each in reshaped_img:
        all_labels.append([each, label_dict[each]])
    return all_labels

def kmeans(img,k):
    """
    Implement kmeans clustering on the given image.
    Steps:
    (1) Random initialize the centers.
    (2) Calculate distances and update centers, stop when centers do not change.
    (3) Iterate all initializations and return the best result.
    Arg: Input image;
         Number of K. 
    Return: Clustering center values;
            Clustering labels of all pixels;
            Minimum summation of distance between each pixel and its center.  
    """
    # TODO: implement this function.

    # get number of unique pixels
    unique_points = np.unique(img).astype("int")
    # get all possible combinations of centers of unique pixels 
    all_possible_centers = getAllCombinations(unique_points)
    # calc euclidean distances in the grayscale world
    dist_matrix = getAllEuclideanDistances() 

    reshaped_img = img.reshape(len(img) * len(img[0]))
    #flatten image completely
    reshaped_img = [int(i) for i in reshaped_img]
    # get frequencies of unique pixels
    frequency_dict = calcFrequency(reshaped_img)

    # initializations
    clusters = []
    total_sum = 0
    clustering_labels = []
    label_dict = dict()
    labels = []
    min_sum = float("inf")

    # exhaust all possible centers
    for each_center in all_possible_centers:
        center0 = each_center[0]
        center1 = each_center[1]
        prev_center0 = -1
        prev_center1 = -1
        # convergence when the cluster center values do not change
        while center0 != prev_center0 or center1 != prev_center1:
            prev_center0 = center0
            prev_center1 = center1
            clustering_labels = []
            label_dict = dict()
            
            # loop through only unique points
            for each_pixel in unique_points:
                if each_pixel in label_dict:
                    label = label_dict[each_pixel]
                else:
                    label = closestCluster(center0, center1, each_pixel, dist_matrix)
                    label_dict[each_pixel] = label
                clustering_labels.append([each_pixel, label])
            # but consider all points while calculating the centers 
            # so that the mean does not shift due to absence of duplicates which make up the original image
            # this can be optimized by just storing the frequency of each pixel as done before
            center0, center1 = calcCenters(clustering_labels, center0, center1, frequency_dict)

        # calc sum of distances between current cluster centers and the pixels
        total_sum = calcSum(clustering_labels, center0, center1, dist_matrix, frequency_dict)
        # choose best model
        if total_sum < min_sum:
            min_sum = int(total_sum)
            clusters = [int(center0), int(center1)]
            # map labels to all points after cluster centers for the current initialization is found iff it is the best model
            labels = calcAllLabels(label_dict, reshaped_img)
    return clusters, labels, min_sum


def visualize(centers,labels):
    """
    Convert the image to segmentation map replacing each pixel value with its center.
    Arg: Clustering center values;
         Clustering labels of all pixels. 
    Return: Segmentation map.
    """
    # TODO: implement this function.
    newImg = []
    # replace pixel values with their corresponding cluster centers
    for i, each in enumerate(labels):
        newImg.append(centers[each[1]])
    # return as a numpy array of type uint8
    return np.array(newImg).astype(np.uint8)
    

     
if __name__ == "__main__":
    img = utils.read_image('lenna.png')
    k = 2

    start_time = time.time()
    
    centers, labels, sumdistance = kmeans(img,k)

    result = visualize(centers, labels)
    result = result.reshape((len(img), len(img[0])))
    
    end_time = time.time()

    running_time = end_time - start_time
    print(running_time)

    centers = list(centers)
    with open('results/task1.json', "w") as jsonFile:
        jsonFile.write(json.dumps({"centers":centers, "distance":sumdistance, "time":running_time}))
    utils.write_image(result, 'results/task1_result.jpg')
