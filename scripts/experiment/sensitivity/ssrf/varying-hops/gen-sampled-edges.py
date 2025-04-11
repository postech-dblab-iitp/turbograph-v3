import sys
import pandas as pd
import os

def format_percentage_str(percentage: float) -> str:
    if percentage >= 1:
        return str(int(percentage))
    else:
        # e.g., 0.1 → "01", 0.05 → "005"
        decimal_digits = str(percentage).split('.')[-1]
        return decimal_digits.rjust(2, '0')

def main():
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: python sample_edges.py <input_csv> <percentage> [--bidirectional]")
        sys.exit(1)

    input_path = sys.argv[1]
    percentage = float(sys.argv[2])
    bidirectional = len(sys.argv) == 4 and sys.argv[3] == "--bidirectional"

    if not os.path.exists(input_path):
        print(f"Error: File {input_path} does not exist.")
        sys.exit(1)

    if not (0 < percentage <= 100):
        print("Error: Percentage must be in the range (0, 100].")
        sys.exit(1)

    # Convert percentage to string without decimal point (e.g., 0.01 -> 001, 1.5 -> 15)
    percentage_str = format_percentage_str(percentage)

    # Read the CSV
    df = pd.read_csv(input_path, sep='|')
    sample_size = int(len(df) * (percentage / 100.0))
    sampled_df = df.sample(n=sample_size, random_state=42).sort_values(by=[df.columns[0], df.columns[1]])

    # Save sampled edges
    filename_wo_ext = os.path.splitext(input_path)[0]
    sampled_filename = f"{filename_wo_ext}_sampled_{percentage_str}_prcnt_di.csv"
    sampled_df.to_csv(sampled_filename, sep='|', index=False)

    # Create backward edges by swapping columns
    reversed_df = sampled_df[[df.columns[1], df.columns[0]]]
    reversed_df.columns = df.columns  # Rename back to original header style
    reversed_df = reversed_df.sort_values(by=[reversed_df.columns[0], reversed_df.columns[1]])
    reversed_filename = f"{sampled_filename}.backward"
    reversed_df.to_csv(reversed_filename, sep='|', index=False)

    print(f"Sampled file saved to: {sampled_filename}")
    print(f"Backward file saved to: {reversed_filename}")

    # If bidirectional mode, merge and save
    if bidirectional:
        merged_df = pd.concat([sampled_df, reversed_df]).drop_duplicates()
        merged_df = merged_df.sort_values(by=[merged_df.columns[0], merged_df.columns[1]])
        bidir_filename = f"{filename_wo_ext}_sampled_{percentage_str}_prcnt_bi.csv"
        merged_df.to_csv(bidir_filename, sep='|', index=False)
        print(f"Bidirectional merged file saved to: {bidir_filename}")

if __name__ == "__main__":
    main()
