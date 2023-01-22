## A) Please write a function that sorts 11 small numbers (<100) as fast as possible. Once written,
# provide an estimate for how long it would take to execute that function 10 Billion (10^10) times on a normal machine.

# If I would have to implement a function that sorts 11 small numbers effectivelly I would use the built-in functions to do it.

# This is two possoble implementations using the built-in functions:

def sort_numbers_with_builtin_sorted(nums):
    return sorted(nums)

# It would take a long time to execute this function 10 billion times on a normal machine 
# because the sorting algorithm used by the built-in sorted() function is not designed to be
# particularly efficient when sorting small numbers. A more efficient sorting algorithm, 
# like quicksort, will be more suitable for this case.

def sort_numbers_with_builtin_sort(nums):
    nums.sort()
    return nums

# this function executed 10^8 on my local machine (Apple M1 Pro with 10 CPU and 16 GPU 16) took 32.80675 seconds. 
# Presumably it will take 100 times longer to execute it 10^10 time, it could probably be 3280.675 seconds is equivalent to 54.678125 minute.


# The above function will be faster than the previous one as it uses the in-place sorting algorithm of python 
# called Timsort which is a combination of merge sort and insertion sort.

# As for the time taken to execute the function 10 billion times, it would be highly dependent on the specific machine and 
# implementation details. Without knowing more information about the specific machine, it's difficult to provide an accurate estimate. 
# However, it would likely take many hours or even days to complete on a normal machine.


# Here is an implementation of the sort_numbers() function that uses a simple bubble sort algorithm 
# to sort the numbers without using any built-in sorting functions:

def sort_numbers_using_bubble_sort(nums):
    for i in range(len(nums)):
        for j in range(len(nums)-1):
            if nums[j] > nums[j+1]:
                nums[j], nums[j+1] = nums[j+1], nums[j]
    return nums

# This function sorts the numbers by repeatedly swapping adjacent elements that are out of order.
# The outer loop runs for n-1 times where n is the number of elements and the inner loop runs for n-1-i times.

# As for the time taken to execute the function 10 billion times, 
# it would be highly dependent on the specific machine and implementation details, 
# but it would likely take a longer time to complete than the previous implementation as the bubble sort is not as efficient as Timsort.


# Bubble sort is a simple sorting algorithm that is easy to understand and implement, but it is not the most optimized algorithm for sorting small numbers.
# It has O(n^2) time complexity which makes it less efficient for large datasets and for small numbers as well.
# There are other sorting algorithms like quicksort, merge sort, and heapsort, that have better time complexities than bubble sort.

# For small numbers (less than 100) insertion sort is a good choice as it has a time complexity of O(n^2) as well but it performs better on small datasets.

# Here is an implementation of the sort_numbers() function that uses the insertion sort algorithm:

def sort_numbers_using_insertion(nums):
    for i in range(1, len(nums)):
        key = nums[i]
        j = i-1
        while j >= 0 and key < nums[j] :
                nums[j + 1] = nums[j]
                j -= 1
        nums[j + 1] = key
    return nums

# this function executed 10^8 on my local machine (Apple M1 Pro with 10 CPU and 16 GPU 16) took 413.68078 seconds. 
# Presumably it will take 100 times longer to execute it 10^10 time, it could probably be 41368.078 seconds is equivalent to 11.4911471667 hours.

# This function sorts the numbers by repeatedly moving elements that are greater than the key element one position to their right. 
# Insertion sort has a time complexity of O(n^2) but performs well on small datasets.

# It is worth noting that, there are other sorting algorithm that are more efficient than these,
# but they are a bit more complex to understand and implement like radix sort and bucket sort, 
# However, for small dataset, these algorithms will be a good choice for optimization.

import time

n = time.time()
print("1 - Sorted list: ", sort_numbers_with_builtin_sorted([99, 1, 0, 22, 42, 80, 66, 2, 5, 55]), "-  " + str(round(time.time() - n, 5)) + " seconds to be executed")

n = time.time()
print("2 - Sorted list: ", sort_numbers_with_builtin_sort([99, 1, 0, 22, 42, 80, 66, 2, 5, 55]), "-  " + str(round(time.time() - n, 5)) + " seconds to be executed")

n = time.time()
print("3 - Sorted list: ", sort_numbers_using_bubble_sort([99, 1, 0, 22, 42, 80, 66, 2, 5, 55]), "-  " + str(round(time.time() - n, 5)) + " seconds to be executed")

n = time.time()
print("4 - Sorted list: ", sort_numbers_using_insertion([99, 1, 0, 22, 42, 80, 66, 2, 5, 55]), "-  " + str(round(time.time() - n, 5)) + " seconds to be executed")
