import sys
from typing import Union, Sequence
import numpy as np
from sklearn.metrics import accuracy_score

def evaluate_accuracies(
    y_true: Union[np.ndarray, Sequence[int]], 
    y_pred: Union[np.ndarray, Sequence[int]]
) -> None:
    """
    Evaluates and prints the exact match accuracy and off-by-one accuracy.
    
    This function accepts flexible input types (lists, tuples, or numpy arrays)
    and converts them to numpy arrays. It computes:
      - Exact match accuracy using sklearn's accuracy_score.
      - Off-by-one accuracy where predictions that differ by at most 1 are considered correct.
      
    It also prints validation messages before each accuracy is output.
    
    Args:
        y_true (Union[np.ndarray, Sequence[int]]): True labels.
        y_pred (Union[np.ndarray, Sequence[int]]): Predicted labels.
    """
    # Convert inputs to numpy arrays in case they are not already
    y_true_arr = np.array(y_true)
    y_pred_arr = np.array(y_pred)
    
    print("Validating Exact Match Accuracy ...")
    # Calculate exact match accuracy
    exact_accuracy = accuracy_score(y_true_arr, y_pred_arr)
    print(f"Exact Match Accuracy: {exact_accuracy:.2f}")
    
    print("Validating Off-by-One Accuracy ...")
    # Calculate off-by-one accuracy: count predictions that are within 1 of the true value
    correct_or_off_by_one = np.abs(y_true_arr - y_pred_arr) <= 1
    off_by_one_accuracy = np.sum(correct_or_off_by_one) / len(y_true_arr)
    print(f"Off-by-One Accuracy: {off_by_one_accuracy:.2f}")

# Example usage:
if __name__ == "__main__":
    y_true = [0, 1, 2, 3, 4]
    y_pred = [0, 2, 2, 2, 5]
    evaluate_accuracies(y_true, y_pred)
