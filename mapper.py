#  MapReduce com Python
import sys

if __name__ == "__main__":
	for line in sys.stdin:
		words = line.split()
		for word in words:
			print(word + '\t' + str(1))
