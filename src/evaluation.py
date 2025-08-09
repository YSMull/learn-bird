import json
import multiprocessing as mp
import os
import re
import sqlite3


def load_json(dir):
    with open(dir) as j:
        contents = json.loads(j.read())
    return contents


def result_callback(result):
    exec_result.append(result)


def extract_sql_blocks(llm_output: str) -> list[str]:
    """用正则提取```sql```包裹的部分"""
    pattern = r"```sql\s+(.*?)```"
    matches = re.findall(pattern, llm_output, re.DOTALL | re.IGNORECASE)
    return [m.strip() for m in matches]


def execute_sql(predicted_sql, ground_truth, db_path):
    conn = sqlite3.connect(db_path)
    # Connect to the database
    cursor = conn.cursor()
    predicted_sql = extract_sql_blocks(predicted_sql)[-1]
    cursor.execute(predicted_sql)
    predicted_res = cursor.fetchall()
    cursor.execute(ground_truth)
    ground_truth_res = cursor.fetchall()
    res = 0
    if set(predicted_res) == set(ground_truth_res):
        res = 1
    return res


def execute_model(predicted_sql, ground_truth, db_place, idx):
    try:
        res = execute_sql(predicted_sql, ground_truth, db_place)
    except Exception:
        res = 0
    # print(result)
    # result = str(set([ret[0] for ret in result]))
    result = {"sql_idx": idx, "res": res}
    return result


def package_sqls(sql_path, db_root_path, mode="gpt", data_mode="dev"):
    clean_sqls = []
    db_path_list = []
    if mode == "gpt":
        sql_data = json.load(open(sql_path + "predict_" + data_mode + ".json"))
        for idx, sql_str in sql_data.items():
            if type(sql_str) == str:
                sql, db_name = sql_str.split("\t----- bird -----\t")
            else:
                sql, db_name = " ", "financial"
            clean_sqls.append(sql)
            db_path_list.append(db_root_path + db_name + "/" + db_name + ".sqlite")

    elif mode == "gt":
        sqls = open(sql_path + data_mode + ".sql")
        sql_txt = sqls.readlines()
        # sql_txt = [sql.split('\t')[0] for sql in sql_txt]
        for idx, sql_str in enumerate(sql_txt):
            sql, db_name = sql_str.strip().split("\t")
            clean_sqls.append(sql)
            db_path_list.append(db_root_path + db_name + "/" + db_name + ".sqlite")

    return clean_sqls, db_path_list


def run_sqls_parallel(sqls, db_places, num_cpus=1):
    pool = mp.Pool(processes=num_cpus)
    for i, sql_pair in enumerate(sqls):
        predicted_sql, ground_truth = sql_pair
        pool.apply_async(
            execute_model,
            args=(predicted_sql, ground_truth, db_places[i], i),
            callback=result_callback,
        )
    pool.close()
    pool.join()


def sort_results(list_of_dicts):
    return sorted(list_of_dicts, key=lambda x: x["sql_idx"])


def compute_acc_by_diff(exec_results, diff_json_path):
    num_queries = len(exec_results)
    results = [res["res"] for res in exec_results]
    contents = load_json(diff_json_path)
    simple_results, moderate_results, challenging_results = [], [], []

    for i, content in enumerate(contents):
        if i >= len(results):
            break
        if content["difficulty"] == "simple":
            simple_results.append(exec_results[i])

        if content["difficulty"] == "moderate":
            moderate_results.append(exec_results[i])

        if content["difficulty"] == "challenging":
            challenging_results.append(exec_results[i])

    simple_acc = (
        sum([res["res"] for res in simple_results]) / len(simple_results)
        if len(simple_results) > 0
        else 0
    )
    moderate_acc = (
        sum([res["res"] for res in moderate_results]) / len(moderate_results)
        if len(moderate_results) > 0
        else 0
    )
    challenging_acc = (
        sum([res["res"] for res in challenging_results]) / len(challenging_results)
        if len(challenging_results) > 0
        else 0
    )
    all_acc = sum(results) / num_queries if num_queries > 0 else 0
    count_lists = [
        len(simple_results),
        len(moderate_results),
        len(challenging_results),
        num_queries,
    ]
    return (
        simple_acc * 100,
        moderate_acc * 100,
        challenging_acc * 100,
        all_acc * 100,
        count_lists,
    )


def print_data(score_lists, count_lists):
    levels = ["simple", "moderate", "challenging", "total"]
    print("{:20} {:20} {:20} {:20} {:20}".format("", *levels))
    print("{:20} {:<20} {:<20} {:<20} {:<20}".format("count", *count_lists))

    print(
        "======================================    ACCURACY    ====================================="
    )
    print(
        "{:20} {:<20.2f} {:<20.2f} {:<20.2f} {:<20.2f}".format("accuracy", *score_lists)
    )


if __name__ == "__main__":
    cur_dir_path = os.path.dirname(os.path.abspath(__file__)) + "/"
    exec_result = []
    predicted_sql_path = cur_dir_path + "../exp_result/turbo_output/"
    db_root_path = cur_dir_path + "../data/dev/dev_databases/"
    data_mode = "dev"
    ground_truth_path = cur_dir_path + "../data/dev/"
    num_cpus = 16
    diff_json_path = cur_dir_path + "../data/dev/dev.json"

    pred_queries, db_paths = package_sqls(
        predicted_sql_path, db_root_path, mode="gpt", data_mode=data_mode
    )
    gt_queries, db_paths_gt = package_sqls(
        ground_truth_path, db_root_path, mode="gt", data_mode=data_mode
    )

    query_pairs = list(zip(pred_queries, gt_queries, strict=False))
    run_sqls_parallel(query_pairs, db_places=db_paths, num_cpus=num_cpus)
    exec_result = sort_results(exec_result)

    print("start calculate")
    simple_acc, moderate_acc, challenging_acc, acc, count_lists = compute_acc_by_diff(
        exec_result, diff_json_path
    )
    score_lists = [simple_acc, moderate_acc, challenging_acc, acc]
    print_data(score_lists, count_lists)
    print(
        "==========================================================================================="
    )
    print("Finished evaluation")
