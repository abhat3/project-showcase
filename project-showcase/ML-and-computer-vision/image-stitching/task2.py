"""
Image Stitching Problem
(Due date: Oct. 23, 3 P.M., 2019)
The goal of this task is to stitch two images of overlap into one image.
To this end, you need to find feature points of interest in one image, and then find
the corresponding ones in another image. After this, you can simply stitch the two images
by aligning the matched feature points.
For simplicity, the input two images are only clipped along the horizontal direction, which
means you only need to find the corresponding features in the same rows to achieve image stiching.

Do NOT modify the code provided to you.
You are allowed use APIs provided by numpy and opencv, except “cv2.findHomography()” and
APIs that have “stitch”, “Stitch”, “match” or “Match” in their names, e.g., “cv2.BFMatcher()” and
“cv2.Stitcher.create()”.
"""
import cv2
import numpy as np
import random

# using cv2 version '3.4.2'

def solution(left_img, right_img):
    """
    :param left_img:
    :param right_img:
    :return: you need to return the result image which is stitched by left_img and right_img
    """

    img1 = right_img
    img2 = left_img

    # Can use ORB instead of SIFT or SURF because they are patented
    # Also ORB is very fast, but orb doesnt give good match for all images 
    sift = cv2.xfeatures2d.SIFT_create()
    # orb = cv2.ORB_create()
    
    # find the keypoints and descriptors with SIFT
    kp1, des1 = sift.detectAndCompute(img1,None)
    kp2, des2 = sift.detectAndCompute(img2,None)
    
    # USE flannBasedMatcher or BFMatcher, Flann is faster, 
    # but uses approximate nearest neigbhor solution, may not be the best
    # BF is a brute force technique, will give the best match

    # FLANN parameters
    # FLANN_INDEX_KDTREE = 0
    # index_params = dict(algorithm = FLANN_INDEX_KDTREE, trees = 5)
    # search_params = dict(checks=50) 
    # flann = cv2.FlannBasedMatcher(index_params,search_params)
    # matches = flann.knnMatch(des1,des2,k=2)
    bf = cv2.BFMatcher()
    matches = bf.knnMatch(des1,des2, k=2)

    # Filter matches by distance score
    matches = list(filter(lambda x: x[0].distance < 0.5*x[1].distance, matches))

    # initialize points to zeros
    points1 = np.zeros((len(matches), 2), dtype=np.float32)
    points2 = np.zeros((len(matches), 2), dtype=np.float32)

    # extract location of good matches
    for i, match in enumerate(matches):
        points1[i, :] = kp1[match[0].queryIdx].pt
        points2[i, :] = kp2[match[0].trainIdx].pt
    H, masked = cv2.findHomography(points1, points2, cv2.RANSAC)

    final_image = cv2.warpPerspective(right_img,H,(left_img.shape[1] + right_img.shape[1], right_img.shape[0]))
    final_image[0:left_img.shape[0], 0:left_img.shape[1]] = left_img
    return final_image
    # raise NotImplementedError

if __name__ == "__main__":
    left_img = cv2.imread('left.jpg')
    right_img = cv2.imread('right.jpg')
    result_image = solution(left_img, right_img)
    cv2.imwrite('results/task2_result.jpg',result_image)


