


import ast
import json
import os
import re


def load_sub_agent_result():
    QUERY_CACHE_PATH = os.environ["QUERY_CACHE_PATH"]
    result_path = os.path.join(QUERY_CACHE_PATH, "step_result.json")
    if not os.path.exists(result_path):
        return []
    with open(result_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        return data



def save_item_in_json(item):
    try:
        QUERY_CACHE_PATH = os.environ["QUERY_CACHE_PATH"]
        result_path = os.path.join(QUERY_CACHE_PATH, "step_result.json")
        if not os.path.exists(result_path):
            with open(result_path, "w", encoding="utf-8") as f:
                json.dump([], f, ensure_ascii=False, indent=4)
        # 读取现有 JSON
        with open(result_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        data.append(item)
        # 写回文件
        with open(result_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        print("中间结果存储错误")
        print(e)

def response2json(response):
    if isinstance(response, (dict, list)):
        return response

    if not isinstance(response, str):
        response = str(response)

    # 统一处理常见的非法拼写或大小写
    normalized = (
        response.replace("None", "null")
        .replace("True", "true")
        .replace("False", "false")
        .replace("{{", "{")
        .replace("}}", "}")
    )

    match = re.search(r"```json(.*?)```", normalized, re.S)
    if match:
        json_str = match.group(1).strip()
        response_json = json.loads(json_str)
    else:
        print("未返回markdown")
        try:
            response_json = json.loads(normalized)
        except json.JSONDecodeError:
            print("不是直接输出json")
            try:
                # ast.literal_eval 无法识别双重括号等复杂结构，尝试宽松解析
                response_json = ast.literal_eval(normalized)
            except (ValueError, SyntaxError, MemoryError):
                # 一些模型会返回以双括号包裹的 JSON 或带多余引号的内容
                trimmed = normalized.strip()
                if trimmed.startswith("{{") and trimmed.endswith("}}"):
                    trimmed = trimmed[1:-1]
                if trimmed.startswith('"') and trimmed.endswith('"'):
                    trimmed = trimmed[1:-1]

                trimmed = trimmed.replace('\'"', '"')
                try:
                    response_json = json.loads(trimmed)
                except Exception as final_err:
                    raise ValueError("无法解析响应为 JSON") from final_err
    return response_json


