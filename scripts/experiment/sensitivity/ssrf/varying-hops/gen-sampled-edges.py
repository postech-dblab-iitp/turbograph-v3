import sys
import pandas as pd
import os

def main():
    if len(sys.argv) != 3:
        print("Usage: python sample_edges.py <input_csv> <percentage>")
        sys.exit(1)

    input_path = sys.argv[1]
    percentage = float(sys.argv[2])

    if not os.path.exists(input_path):
        print(f"Error: File {input_path} does not exist.")
        sys.exit(1)

    if not (0 < percentage <= 100):
        print("Error: Percentage must be in the range (0, 100].")
        sys.exit(1)

    # Read the CSV
    df = pd.read_csv(input_path, sep='|')
    sample_size = int(len(df) * (percentage / 100.0))
    sampled_df = df.sample(n=sample_size, random_state=42)

    # Create backward edges by swapping source and destination
    reversed_df = sampled_df[[df.columns[1], df.columns[0]]]
    reversed_df.columns = df.columns  # Keep the same column names

    # Combine original and reversed edges
    bidirectional_df = pd.concat([sampled_df, reversed_df]).sort_values(by=[df.columns[0], df.columns[1]])

    # Save bidirectional edges
    filename_wo_ext = os.path.splitext(input_path)[0]
    bidir_filename = f"{filename_wo_ext}_sampled_{int(percentage)}_prcnt.csv"
    bidirectional_df.to_csv(bidir_filename, sep='|', index=False)

    print(f"Bidirectional sampled file saved to: {bidir_filename}")

if __name__ == "__main__":
    main()
