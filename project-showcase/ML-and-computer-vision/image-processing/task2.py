"""
Template Matching
(Due date: Sep. 25, 3 P.M., 2019)

The goal of this task is to experiment with template matching techniques, i.e., normalized cross correlation (NCC).

Please complete all the functions that are labelled with '# TODO'. When implementing those functions, comment the lines 'raise NotImplementedError' instead of deleting them. The functions defined in 'utils.py'
and the functions you implement in 'task1.py' are of great help.

Do NOT modify the code provided to you.
Do NOT use ANY API provided by opencv (cv2) and numpy (np) in your code.
Do NOT import ANY library (function, module, etc.).
"""


import argparse
import json
import os

import utils
from task1 import *


def parse_args():
    parser = argparse.ArgumentParser(description="cse 473/573 project 1.")
    parser.add_argument(
        "--img-path",
        type=str,
        default="./data/proj1-task2.jpg",
        help="path to the image")
    parser.add_argument(
        "--template-path",
        type=str,
        default="./data/proj1-task2-template.jpg",
        help="path to the template"
    )
    parser.add_argument(
        "--result-saving-path",
        dest="rs_path",
        type=str,
        default="./results/task2.json",
        help="path to file which results are saved (do not change this arg)"
    )
    args = parser.parse_args()
    return args

def norm_xcorr2d(patch, template):
    """Computes the NCC value between a image patch and a template.

    The image patch and the template are of the same size. The formula used to compute the NCC value is:
    sum_{i,j}(x_{i,j} - x^{m}_{i,j})(y_{i,j} - y^{m}_{i,j}) / (sum_{i,j}(x_{i,j} - x^{m}_{i,j}) ** 2 * sum_{i,j}(y_{i,j} - y^{m}_{i,j}) ** 2) ** 0.5
    This equation is the one shown in Prof. Yuan's ppt.

    Args:
        patch: nested list (int), image patch.
        template: nested list (int), template.

    Returns:
        value (float): the NCC value between a image patch and a template.
    """
    
    rows_patch = len(patch)
    cols_patch = len(patch[0])
    rows_template = len(template)
    cols_template = len(template[0])

    if not (rows_patch == rows_template and cols_patch == cols_template):
        print("Image patch and template are not of same size!")
        return -1

    mean_patch = sum([sum(patch[row]) for row in range(rows_patch)]) / (rows_patch * cols_patch)
    mean_template = sum([sum(template[row]) for row in range(rows_template)]) / (rows_template * cols_template)

    ncc = 0

    num = sum([(patch[i][j] - mean_patch) * (template[i][j] - mean_template) for i in range(rows_patch) for j in range(cols_patch)])
    denom = ((sum([(patch[i][j] - mean_patch) ** 2 for i in range(rows_patch) for j in range(cols_patch)])) * (sum([(template[i][j] - mean_template) ** 2 for i in range(rows_patch) for j in range(cols_patch)]))) ** 0.5

    ncc = num / denom

    return ncc
    # raise NotImplementedError

def match(img, template):
    """Locates the template, i.e., a image patch, in a large image using template matching techniques, i.e., NCC.

    Args:
        img: nested list (int), image that contains character to be detected.
        template: nested list (int), template image.

    Returns:
        x (int): row that the character appears (starts from 0).
        y (int): column that the character appears (starts from 0).
        max_value (float): maximum NCC value.
    """
    # TODO: implement this function.

    ncc_dict = {}

    height_template = len(template)
    width_template = len(template[0])
    height_img = len(img)
    width_img = len(img[0])
    n_rows = height_img - height_template + 1
    n_cols = width_img - width_template + 1
    for row in range(n_rows):
        for col in range(n_cols):
            img_patch = [img[row+i][col:col+width_template] for i in range(height_template)]
            ncc_dict[norm_xcorr2d(img_patch, template)] = (row, col)
    max_ncc = max(ncc_dict)
    max_ncc_rounded = round(max(ncc_dict), 3)
    # raise NotImplementedError
    return ncc_dict[max_ncc][0], ncc_dict[max_ncc][1], max_ncc_rounded

def save_results(coordinates, template, template_name, rs_directory):
    results = {}
    results["coordinates"] = sorted(coordinates, key=lambda x: x[0])
    results["templat_size"] = (len(template), len(template[0]))
    with open(os.path.join(rs_directory, template_name), "w") as file:
        json.dump(results, file)


def main():
    args = parse_args()

    img = read_image(args.img_path)
    # template = utils.crop(img, xmin=10, xmax=30, ymin=10, ymax=30)
    # template = np.asarray(template, dtype=np.uint8)
    # cv2.imwrite("./data/proj1-task2-template.jpg", template)
    template = read_image(args.template_path)

    x, y, max_value = match(img, template)
    # The correct results are: x: 17, y: 129, max_value: 0.994
    with open(args.rs_path, "w") as file:
        json.dump({"x": x, "y": y, "value": max_value}, file)


if __name__ == "__main__":
    main()
