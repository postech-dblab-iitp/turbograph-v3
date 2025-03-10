import sys
import pandas as pd
import numpy as np

def geometric_mean(arr):
    return np.exp(np.mean(np.log(arr))) if len(arr) > 0 else float('nan')

def compare_csv(file1, file2):
    try:
        df1 = pd.read_csv(file1, skip_blank_lines=True).dropna()
        df2 = pd.read_csv(file2, skip_blank_lines=True).dropna()
    except pd.errors.EmptyDataError:
        print("Error: One of the input files is empty or invalid.")
        sys.exit(1)
    except pd.errors.ParserError:
        print("Error: CSV parsing issue detected. Ensure proper formatting.")
        sys.exit(1)
    
    if not df1.columns.equals(df2.columns):
        print("Error: CSV headers do not match.")
        sys.exit(1)
    
    speedup_data = []
    compile_speedups = []
    execution_speedups = []
    end_to_end_speedups = []
    
    for _, (row1, row2) in enumerate(zip(df1.itertuples(index=False), df2.itertuples(index=False))):
        if row1[0] != row2[0]:
            print("Error: Query numbers do not match.")
            sys.exit(1)
        
        try:
            c1, e1, t1 = float(row1[1]), float(row1[2]), float(row1[3])
            c2, e2, t2 = float(row2[1]), float(row2[2]), float(row2[3])
        except ValueError:
            print(f"Warning: Skipping query {row1[0]} due to missing or invalid values.")
            continue
        
        if c2 == 0 or e2 == 0 or t2 == 0:
            print(f"Warning: Skipping query {row1[0]} due to zero division issue.")
            continue
        
        compile_speedup = c1 / c2
        execution_speedup = e1 / e2
        end_to_end_speedup = t1 / t2
        
        compile_speedups.append(compile_speedup)
        execution_speedups.append(execution_speedup)
        end_to_end_speedups.append(end_to_end_speedup)
        
        speedup_data.append([row1[0], compile_speedup, execution_speedup, end_to_end_speedup])
    
    speedup_df = pd.DataFrame(speedup_data, columns=["Query number", "Compile time speedup", "Execution time speedup", "End-to-end time speedup"])
    print(speedup_df.to_csv(index=False))
    
    if compile_speedups and execution_speedups and end_to_end_speedups:
        print("Geometric Mean:")
        print(f"Compile time speedup: {geometric_mean(compile_speedups):.6f}")
        print(f"Execution time speedup: {geometric_mean(execution_speedups):.6f}")
        print(f"End-to-end time speedup: {geometric_mean(end_to_end_speedups):.6f}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python compare_csv.py <csv_file1> <csv_file2>")
        sys.exit(1)
    
    compare_csv(sys.argv[1], sys.argv[2])
