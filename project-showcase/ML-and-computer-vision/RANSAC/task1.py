"""
RANSAC Algorithm Problem
(Due date: Oct. 23, 3 P.M., 2019)
The goal of this task is to fit a line to the given points using RANSAC algorithm, and output
the names of inlier points and outlier points for the line.

Do NOT modify the code provided to you.
Do NOT use ANY API provided by opencv (cv2) and numpy (np) in your code.
Do NOT import ANY library (function, module, etc.).
You can use the library random
Hint: It is recommended to record the two initial points each time, such that you will Not 
start from this two points in next iteration.
"""
import random

def getAllCombinations(input_points):
    input_points_copy = input_points[:]
    all_points = []
    while len(input_points_copy) > 1:
        each_point = input_points_copy[0]
        list_without_prev = [i for i in input_points_copy if each_point != i]
        for other_point in list_without_prev:
            all_points.append([each_point["value"], other_point["value"]])
        input_points_copy.pop(0)

    return all_points

def calcDistance(x1, y1, x2, y2, x0, y0):
    num = abs(((y2-y1)*x0) - ((x2-x1)*y0) + (x2*y1) - (y2*x1))
    denom = (((y2-y1)**2) + ((x2-x1)**2))**0.5
    if denom == 0:
        print("Points are same")
        return 0
    dist = num / denom
    return dist

def solution(input_points, t, d, k):
    """
    :param input_points:
           t: t is the perpendicular distance threshold from a point to a line
           d: d is the number of nearby points required to assert a model fits well, you may not need this parameter
           k: k is the number of iteration times
           Note that, n for line should be 2
           (more information can be found on the page 90 of slides "Image Features and Matching")
    :return: inlier_points_name, outlier_points_name
    inlier_points_name and outlier_points_name is two list, each element of them is str type.
    For example: If 'a','b' is inlier_points and 'c' is outlier_point.
    the output should be two lists of ['a', 'b'], ['c'].
    Note that, these two lists should be non-empty.
    """
    # TODO: implement this function.
    list_input_points = [i["value"] for i in input_points]
    list_point_names = [i["name"] for i in input_points]

    best_model = {"final_inliers":[], "final_outliers":[], "err":999}

    combList = getAllCombinations(input_points)

    newErr = 999

    for each_point in combList:
        first_point = each_point[0]
        sec_point = each_point[1]
        x1 = first_point[0]
        y1 = first_point[1]
        x2 = sec_point[0]
        y2 = sec_point[1]
        inliers = []
        total_dist = 0
        dist = 0
        for point_name, other_point in zip(list_point_names, list_input_points):
            x0 = other_point[0]
            y0 = other_point[1]
            dist = calcDistance(x1, y1, x2, y2, x0, y0)
            # if inlier consider for total error, else skip
            # when considering point on the line the ditance is 0, so it does not contribute to error
            if dist <= t:
                total_dist = total_dist + dist
                inliers.append(point_name)
        # do not consider initial two points for comparing with d
        thisD = len(inliers) - 2
        if thisD >= d:
            newErr = total_dist / thisD
            if newErr < best_model["err"]:
                best_model["final_inliers"] = inliers
                best_model["final_outliers"] = [i for i in list_point_names if i not in inliers]
                best_model["err"] = newErr

    return best_model["final_inliers"], best_model["final_outliers"]
        
    # raise NotImplementedError



if __name__ == "__main__":
    input_points = [{'name': 'a', 'value': (0.0, 1.0)}, {'name': 'b', 'value': (2.0, 1.0)},
                    {'name': 'c', 'value': (3.0, 1.0)}, {'name': 'd', 'value': (0.0, 3.0)},
                    {'name': 'e', 'value': (1.0, 2.0)}, {'name': 'f', 'value': (1.5, 1.5)},
                    {'name': 'g', 'value': (1.0, 1.0)}, {'name': 'h', 'value': (1.5, 2.0)}]
    t = 0.5
    d = 3
    k = 100
    inlier_points_name, outlier_points_name = solution(input_points, t, d, k)  # TODO
    assert len(inlier_points_name) + len(outlier_points_name) == 8  
    f = open('./results/task1_result.txt', 'w')
    f.write('inlier points: ')
    for inliers in inlier_points_name:
        f.write(inliers + ',')
    f.write('\n')
    f.write('outlier points: ')
    for outliers in outlier_points_name:
        f.write(outliers + ',')
    f.close()


