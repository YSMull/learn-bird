import json
import os

from tqdm import tqdm

from prompt.bird import generate_combined_prompts_one
from util.llm import llm

def new_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)

def generate_sql_file(sql_lst, output_path=None):
    result = {}
    for i, sql in enumerate(sql_lst):
        result[i] = sql

    if output_path:
        directory_path = os.path.dirname(output_path)
        new_directory(directory_path)
        json.dump(result, open(output_path, "w"), indent=4)

    return result

def collect_response_from_llm(
    db_path_list, question_list, model, output_path, knowledge_list=None
):
    """
    :param db_path: str
    :param question_list: []
    :return: dict of responses collected from openai
    """
    responses_dict = {}
    response_list = []
    for i, question in tqdm(enumerate(question_list)):
        print(
            "--------------------- processing {}th question ---------------------".format(
                i
            )
        )
        print("the question is: {}".format(question))

        if knowledge_list:
            cur_prompt = generate_combined_prompts_one(
                db_path=db_path_list[i], question=question, knowledge=knowledge_list[i]
            )
        else:
            cur_prompt = generate_combined_prompts_one(
                db_path=db_path_list[i], question=question
            )

        sql_response = llm(
            model=model,
            prompt=cur_prompt,
            max_tokens=2000,
            temperature=0
        )
        # pdb.set_trace()
        # plain_result = request_llm(engine=engine, prompt=cur_prompt, max_tokens=256, temperature=0, stop=['</s>'])
        # determine wheter the sql is wrong
        # responses_dict[i] = sql
        db_id = db_path_list[i].split("/")[-1].split(".sqlite")[0]
        sql_response = (
            sql_response + "\t----- bird -----\t" + db_id
        )  # to avoid unpredicted \t appearing in codex results
        response_list.append(sql_response)
        generate_sql_file(sql_lst=response_list, output_path=output_path)

    return response_list


def decouple_question_schema(datasets, db_root_path):
    question_list = []
    db_path_list = []
    knowledge_list = []
    for i, data in enumerate(datasets):
        question_list.append(data["question"])
        cur_db_path = db_root_path + data["db_id"] + "/" + data["db_id"] + ".sqlite"
        db_path_list.append(cur_db_path)
        knowledge_list.append(data["evidence"])

    return question_list, db_path_list, knowledge_list

if __name__ == "__main__":
    eval_path='../data/dev/dev.json'
    dev_path='../output/'
    db_root_path='../data/dev/dev_databases/'
    use_knowledge=True
    not_use_knowledge=True
    mode='dev'
    model='openai/gpt-oss-20b'
    # model='omnisql-7b-mlx'
    data_output_path='../exp_result/turbo_output/'

    eval_data = json.load(open(eval_path, "r"))
    # '''for debug'''
    eval_data = eval_data[:13]
    # '''for debug'''

    question_list, db_path_list, knowledge_list = decouple_question_schema(
        datasets=eval_data, db_root_path=db_root_path
    )
    assert len(question_list) == len(db_path_list) == len(knowledge_list)

    output_path = data_output_path + "predict_dev.json"

    if use_knowledge:
        responses = collect_response_from_llm(
            db_path_list=db_path_list,
            question_list=question_list,
            model=model,
            knowledge_list=knowledge_list,
            output_path=output_path
        )
    else:
        responses = collect_response_from_llm(
            db_path_list=db_path_list,
            question_list=question_list,
            model=model,
            knowledge_list=None,
            output_path=output_path
        )

    
    # pdb.set_trace()

    print(
        "successfully collect results from {} for {} evaluation; Use knowledge: {}; Use COT: {}".format(
            model, mode, use_knowledge, True
        )
    )
