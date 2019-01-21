package simStarter;

import java.util.Arrays;

public class Main {
	
	public static void main(String[] args) {
		
		int[] array = new int[] {2,4,1,3,7,9,6,4};
		//mergeSort(array, 0, array.length - 1);
		insertionSort(array);
		for(int x: array) {
			System.out.println(x);
		}

	}

	private static void insertionSort(int[] array) {
		insertionHelper(array, 1, 1);
		
	}

	private static void insertionHelper(int[] array, int current, int index) {
		if(index == array.length) {
			return;
		}
		else if (current == 0) {
			index++;
			insertionHelper(array, index, index);
		}
		else {
			if(array[current] < array[current - 1]) {
				int temp = array[current - 1];
				array[current - 1] = array[current];
				array[current] = temp;
				insertionHelper(array, current - 1, index);
			}
			else {
				insertionHelper(array, index + 1, index + 1);
			}
		}
		
		
	}

	private static void mergeSort(int[] array, int start, int end) {
		if(start < end) {
			int mid = (start + end) / 2;
			mergeSort(array, start, mid);
			mergeSort(array, mid + 1, end);
			merge(array, start, mid, end);
		}
		
	}

	private static void merge(int[] array, int start, int mid, int end) {
		int[] leftArray = Arrays.copyOfRange(array, start, mid + 1);
		int[] rightArray = Arrays.copyOfRange(array, mid + 1, end + 1);
		int i = 0;
		int j = 0;
		int count = start;
		for(; i < leftArray.length || j < rightArray.length;) {
			if (i == leftArray.length) {
				array[count] = rightArray[j];
				count++;
				j++;
			}
			else if (j == rightArray.length) {
				array[count] = leftArray[i];
				count++;
				i++;
			}
			else {
				if(leftArray[i] < rightArray[j]) {
					array[count] = leftArray[i];
					i++;
					count++;
				}
				else {
					array[count] = rightArray[j];
					j++;
					count++;
				}
				
			}
		}
		
	}

}
