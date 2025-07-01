import argparse
import pandas as pd


# Report additional stats on ANI thresholds
def report_ani_stats(df, label):
    # print label first
    print(f"\n{label}:")
    # print minimum query_containment_ani
    min_ani = df['query_containment_ani'].min() * 100
    max_ani = df['query_containment_ani'].max() * 100
    mean_ani = df['query_containment_ani'].mean() * 100
    median_ani = df['query_containment_ani'].median() * 100
    print(f"  Min: {min_ani:.1f}%, Max: {max_ani:.1f}%, Mean: {mean_ani:.1f}%, Median: {median_ani:.1f}%")
    # Count unique match_name entries with query_containment_ani above the thresholds
    for thresh in [0.85, 0.9, 0.95]:
        count = df[df['query_containment_ani'] >= thresh]['match_name'].nunique()
        thresh_as_percent = thresh * 100
        print(f"  {count} unique metagenomes with ANI ≥ {thresh_as_percent:.1f}%")

def report_query_containment(df, label):
    # print label first
    print(f"\n{label}:")
    # print minimum query_containment_ani
    min_ani = df['containment'].min() * 100
    max_ani = df['containment'].max() * 100
    mean_ani = df['containment'].mean() * 100
    median_ani = df['containment'].median() * 100
    print(f"  Min: {min_ani:.1f}%, Max: {max_ani:.1f}%, Mean: {mean_ani:.1f}%, Median: {median_ani:.1f}%")
    # number above 10%, 20%
    for thresh in [0.1, 0.2]:
        count = df[df['containment'] >= thresh]['match_name'].nunique()
        thresh_as_percent = thresh * 100
        print(f"  {count} unique metagenomes with query containment ≥ {thresh_as_percent:.1f}%")

def report_f_weighted_stats(df, label):
    # print label first
    print(f"\n{label}:")
    for thresh in [0.1, 0.01, 0.001]:
        # Count unique match_name entries with f_weighted_target_in_query above the threshold
        count = df[df['f_weighted_target_in_query'] >= thresh]['match_name'].nunique()
        thresh_as_percent = thresh * 100
        print(f"  {count} unique metagenomes with f_weighted_target_in_query ≥ {thresh_as_percent:.1f}%")

def main():

    # Load input
    df = pd.read_csv(args.input)

    # Sort first by ANI, then by f_weighted_target_in_query
    df_sorted = df.sort_values(
        # ["query_containment_ani", "f_weighted_target_in_query", "match_name"],
        ["f_weighted_target_in_query", "query_containment_ani", "match_name"],
        ascending=[False, False, False]
    )

     # Report total unique match_name
    total_unique_matches = df_sorted['match_name'].nunique()
    print(f"Total unique metagenomes: {total_unique_matches}")

    # report average number of queries per match_name
    queries_per_match = (
        df_sorted.groupby('match_name')['query_name']
        .nunique()
        .mean()
    )
    print(f"Average number of queries per metagenome: {queries_per_match:.2f}")
     
    # report some stats
    report_query_containment(df_sorted, "Query containment")
    report_ani_stats(df_sorted, "Query containment ANI")
    report_f_weighted_stats(df_sorted, "Total % metagenome (f_weighted_target_in_query)")

    # Apply ANI filter if provided
    if args.ani_threshold is not None:
        df_sorted = df_sorted[df_sorted['query_containment_ani'] >= args.ani_threshold]
        print(f"Total rows after ANI threshold ({args.ani_threshold}): {len(df_sorted)}")
        unique_matches_after_filter = df_sorted['match_name'].nunique()
        print(f"Number of unique match_name after ANI threshold: {unique_matches_after_filter}")

        queries_per_match = (
            df_sorted.groupby('match_name')['query_name']
            .nunique()
            .mean()
        )
        print(f"Average number of queries per match_name (ANI >= {args.ani_threshold}): {queries_per_match:.2f}")

    # Output all sorted rows (post-threshold if applied)
    if args.output_sorted:
        df_sorted.to_csv(args.output_sorted, index=False)
        print(f"Saved sorted table to: {args.output_sorted}")

    # Select top N unique match_name entries from the sorted (and filtered) data
    df_top_unique = df_sorted.drop_duplicates(subset="match_name", keep="first")

    top_matches = (
        df_top_unique
        .head(args.n)
        .reset_index(drop=True)
    )

    if args.output_top_n:
        top_matches.to_csv(args.output_top_n, index=False)
        print(f"Saved top {args.n} match_name entries to: {args.output_top_n}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find top match_name entries weighted by f_weighted_target_in_query.")
    parser.add_argument('--input', required=True, help="Path to input CSV file.")
    parser.add_argument('--output-top-n', help="Path to output top-N match_name summary CSV.")
    parser.add_argument('-n', type=int, default=1000, help="Number of top match_name entries to return.")
    parser.add_argument('--ani-threshold', type=float, help="Minimum query_containment_ani to include.")
    parser.add_argument('--output-sorted', help="Output CSV path for rows sorted by containment and passing any ANI filter.")

    args = parser.parse_args()
    main()
