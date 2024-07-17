import sys
from collections import defaultdict

def count_occurrences(file_path):
    number_counts = defaultdict(int)
    
    with open(file_path, 'r') as file:
        for line in file:
            if ':' in line:
                numbers = line.split(':')[1].strip().split(',')
                for number in numbers:
                    if number.strip():
                        number_counts[int(number.strip())] += 1
                        
    return number_counts

def main():
    if len(sys.argv) != 2:
        print("Usage: python count_occurrences.py <file_path>")
        sys.exit(1)
        
    file_path = sys.argv[1]
    number_counts = count_occurrences(file_path)
    
    sorted_counts = sorted(number_counts.items(), key=lambda item: item[1], reverse=True)
    
    for number, count in sorted_counts:
        print(f"{number} -> {count}")

if __name__ == "__main__":
    main()
