import os
import sys
import pandas as pd
import numpy as np
import time

def corrupt_data(df, percentage):
    """Corrupts the given percentage of data by setting random entries to NaN, excluding integer columns."""
    
    # Keep track of the original integer columns to avoid corrupting them
    integer_columns = df.select_dtypes(include=['int']).columns.tolist()
    
    # Select non-integer columns for corruption
    non_int_columns = [col for col in df.columns if col not in integer_columns]

    # Convert all non-integer columns to allow NaN values
    df_non_int = df[non_int_columns].apply(lambda col: pd.to_numeric(col, errors='ignore', downcast='float') if col.dtype == 'int' else col)

    # Convert the non-integer portion of the DataFrame to a NumPy array for faster manipulation
    arr = df_non_int.to_numpy()
    num_rows, num_cols = arr.shape
    num_to_corrupt = int(num_cols * percentage / 100)

    # Corrupt data by selecting random indices in each row
    for i in range(num_rows):
        # Ensure we are using integer indices
        indices_to_corrupt = np.random.choice(range(num_cols), num_to_corrupt, replace=False).astype(int)
        arr[i, indices_to_corrupt] = np.nan

    # Convert the array back to a DataFrame
    df_corrupted_non_int = pd.DataFrame(arr, columns=non_int_columns)

    # Combine the original integer columns (untouched) with the corrupted non-integer columns
    df_corrupted = pd.concat([df[integer_columns], df_corrupted_non_int], axis=1)

    return df_corrupted

def process_csv_files(folder_path, corruption_percentage, delimiter, output_folder):
    total_start_time = time.time()  # Start timing for the whole program

    # Create the output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            print(f"Processing file: {filename}")
            start_time = time.time()  # Start timing for this file

            file_path = os.path.join(folder_path, filename)

            # Read the CSV file with 'low_memory=False' to dynamically handle mixed data types
            df = pd.read_csv(file_path, delimiter=delimiter, low_memory=False)

            # Corrupt the data using the corrupt_data function
            df_corrupted = corrupt_data(df, corruption_percentage)

            # Save the modified DataFrame back to csv with a suffix '_corrupted'
            new_file_path = os.path.join(output_folder, f"{filename[:-4]}_corrupted.csv")
            df_corrupted.to_csv(new_file_path, index=False, sep=delimiter)  # Use the same delimiter for saving

            elapsed_time = time.time() - start_time  # Calculate elapsed time for this file
            print(f"Processed and saved: {new_file_path} (took {elapsed_time:.2f} seconds)")

    total_elapsed_time = time.time() - total_start_time  # Total time for all files
    print(f"All files processed in {total_elapsed_time:.2f} seconds.")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python script_name.py <folder_path> <corruption_percentage> <delimiter> <output_folder>")
        sys.exit(1)

    folder_path = sys.argv[1]
    corruption_percentage = float(sys.argv[2])
    delimiter = sys.argv[3]
    output_folder = sys.argv[4]

    process_csv_files(folder_path, corruption_percentage, delimiter, output_folder)