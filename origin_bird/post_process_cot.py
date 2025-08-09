import argparse
import json


def fetch_sql(predicted_results, output_path=None):
    final_sql = {}
    invalid_result = []
    for k, v in predicted_results.items():
        idx = int(k)
        print(
            f"------------------- processing {idx}th example -------------------"
        )
        print(v)
        try:
            cot, sql = v.split(": SELECT")
            clean_sql = "SELECT" + sql
        except Exception:
            invalid_result.append(idx)
            clean_sql = 0  # filter resutls without valid SQL, i.e., too long, etc.
        final_sql[k] = clean_sql

    if output_path:
        json.dump(final_sql, open(output_path, "w"), indent=4)
    return final_sql, invalid_result


if __name__ == "__main__":
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument(
        "--predicted_sql_path", type=str, required=True, default=""
    )
    args_parser.add_argument("--output_clean_path", type=str, required=True, default="")
    args = args_parser.parse_args()
    exec_result = []

    # generate sql file:
    pred_file = json.load(open(args.predicted_sql_path))
    post_sql, invalid_results = fetch_sql(pred_file, args.output_clean_path)

    print(
        f"filtered results, among {len(post_sql)} examples, {len(invalid_results)} results are invaid"
    )
