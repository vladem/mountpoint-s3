import argparse


def compute_average(file_path):
    with open(file_path, 'r') as file:
        # Read all lines, convert them to floats, and filter out invalid lines
        numbers = [float(line.strip()) for line in file if line.strip().replace('.', '', 1).isdigit()]

    if numbers:
        return sum(numbers) / len(numbers)  # Calculate the average
    else:
        return None  # Return None if no valid numbers are found


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Compute the average of numbers in a file.")
    parser.add_argument('file', help="The path to the file containing the numbers")

    # Parse arguments
    args = parser.parse_args()

    # Compute and print the average
    average = compute_average(args.file)

    if average is not None:
        print(f"The average of the numbers is: {average}")
    else:
        print("No valid numbers found in the file.")


if __name__ == '__main__':
    main()