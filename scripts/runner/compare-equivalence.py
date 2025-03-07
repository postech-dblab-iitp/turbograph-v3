import csv
import sys

def read_csv(file_path):
    """Reads a CSV file and returns a list of rows (excluding the n._id column)."""
    with open(file_path, newline='', encoding='ISO-8859-1') as f:
        reader = csv.DictReader(f, delimiter='|')
        if 'n._id' not in reader.fieldnames:
            print(f"Warning: 'n._id' column not found in {file_path}, proceeding without it.")

        rows = []
        for row in reader:
            row.pop('n._id', None)  # Remove 'n._id' column if it exists
            rows.append(row)
        return rows

def compare_csv(file1, file2):
    """Compares two CSV files row-by-row after removing the 'n._id' column."""
    data1 = read_csv(file1)
    data2 = read_csv(file2)

    if len(data1) != len(data2):
        print("Files differ in the number of rows.")
        return False

    for i, (row1, row2) in enumerate(zip(data1, data2)):
        if row1 != row2:
            print(f"Difference found in row {i+1}:")
            print(f"File1: {row1}")
            print(f"File2: {row2}")
            return False

    print("Files are equivalent (ignoring 'n._id' column).")
    return True

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <file1.csv> <file2.csv>")
        sys.exit(1)

    file1, file2 = sys.argv[1], sys.argv[2]
    compare_csv(file1, file2)
