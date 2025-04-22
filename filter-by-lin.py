import argparse
import polars as pl

def main(args):
    # Load the data. Treat assemblyID column as a string (initially looks like integers, but there's at least one string)
    df = pl.read_csv(args.csv_file, schema_overrides={"assemblyID": pl.Utf8})

    # find any duplicated rows
    # print the number of duplicated rows
    duplicated = df.filter(pl.col("assemblyID").is_duplicated())
    print(f"Duplicated rows: {duplicated.height} ({df.height} total). Dropping duplicates.")
    # drop duplicates
    df = df.unique(subset=["assemblyID"])


    # print the number with entries in 'accession' column
    print(f"Rows with 'accession' column: {df.filter(pl.col('accession').is_not_null()).height} ({df.height} total)")


    # Filter rows based on the LIN prefix
    filtered = df.filter(pl.col("LIN").str.starts_with(args.lin_prefix))

    # describe number left
    rows_after_filter = filtered.height

    # filter to keep only rows with entries in accession column
    filtered = filtered.filter(pl.col("accession").is_not_null())

    # print the number with entries in 'accession' column
    print(f"LIN prefix {args.lin_prefix} rows with accessions: {filtered.height} ({rows_after_filter} total)")

    # Show result
    print(filtered)

    # save result
    filtered.write_csv(args.output)
    print(f"Filtered results written to {args.output}")

    # save gbsketch
    # accession, name
    # create the name with f"{accession} {ncbi_genus-species} {strain}"
    gbsketch = filtered.select(
    [
        pl.col("accession"),
        pl.col("ncbi_genus-species"),
        pl.col("strain")
    ]
    ).with_columns(
        (pl.col("ncbi_genus-species") + " strain: " + pl.col("strain")).alias("name")
    ).select(
        [pl.col("accession"), pl.col("name")]
    )
    gbsketch.write_csv(args.output_gbsketch)
    print(f"gbsketch written to {args.output_gbsketch}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter rows where LIN starts with a specific prefix.")
    parser.add_argument("csv_file", help="Path to the input CSV file")
    parser.add_argument("--output", help="Path to the output CSV file", default = "ralstonia-864.0.0.1.csv")
    parser.add_argument("--output-gbsketch", help="Path to the output gbSketch file", default = "ralstonia-864.0.0.1.gbsketch.csv")
    parser.add_argument(
        "--lin-prefix",
        default="864,0,0,1",
        help="LIN prefix to match (default: '864,0,0,1')"
    )

    args = parser.parse_args()
    main(args)

