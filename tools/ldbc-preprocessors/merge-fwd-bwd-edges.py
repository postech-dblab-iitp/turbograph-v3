import pandas as pd
import sys

DYNAMIC_PATH = sys.argv[1]

def generate_undirected_edges_with_types(input_file, output_file_all, output_file_basic):
    # Read the input file with '|' as the delimiter
    df = pd.read_csv(input_file, delimiter='|')
    
    # Function to extract base column names and types
    def extract_base_and_type(col):
        if '(' in col:
            base, col_type = col.rsplit('(', 1)
            col_type = '(' + col_type
        else:
            base, col_type = col, ''
        return base, col_type
    
    # Extracting and handling :START_ID and :END_ID columns
    start_col = [col for col in df.columns if ':START_ID' in col][0]
    end_col = [col for col in df.columns if ':END_ID' in col][0]
    
    # Rename columns to swap START_ID and END_ID while preserving types
    df_copy = df.rename(columns={
        start_col: end_col,
        end_col: start_col
    })
    
    # Concatenate the original and copied dataframes
    df_concat = pd.concat([df, df_copy], ignore_index=True)
    
    # Sort by START_ID and END_ID (without types for sorting) for the first output file
    df_concat.sort_values(by=[start_col, end_col], inplace=True)
    
    # Write the first output file with all columns
    df_concat.to_csv(output_file_all, index=False, sep='|')
    
    # For the second output file, sort by END_ID and then by START_ID
    # Explicitly creating a copy to avoid SettingWithCopyWarning
    df_basic = df_concat[[end_col, start_col]].copy()
    df_basic.sort_values(by=[end_col, start_col], inplace=True)
    
    # Write the second output file with only END_ID and START_ID (preserving types)
    df_basic.to_csv(output_file_basic, index=False, sep='|')

# Example usage (commented out as it's just for demonstration)
generate_undirected_edges_with_types(f'{DYNAMIC_PATH}/Person_knows_Person.csv', f'{DYNAMIC_PATH}/Person_knows_Person.csv', f'{DYNAMIC_PATH}/Person_knows_Person.csv.backward')
