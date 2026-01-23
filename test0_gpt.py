import os
import time
from dotenv import load_dotenv
from openai import OpenAI
load_dotenv()
# ① 把你的 Key 写这里（或用环境变量 OPENAI_API_KEY）
api_key=os.environ['OPENAI_API_KEY1']
base_url=os.environ['OPENAI_BASE_URL']
model_name=os.environ['OPENAI_MODEL_NAME']
print("Using OpenAI Model:", model_name)
print("Using OpenAI Base URL:", base_url)
print("Using OpenAI API Key:", api_key)
client = OpenAI(
    api_key=api_key,
    base_url=base_url
)
model_name=model_name


def check_gpt_api():
    start = time.time()
    try:
        resp = client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": "hello"}],
            max_tokens=10
        )
        cost = time.time() - start
        print("✅ API 可用")
        print("⏱ 耗时:", round(cost, 2), "秒")
        print("🤖 返回:", resp.choices[0].message.content)
    except Exception as e:
        print("❌ API 不可用 / 请求失败")
        print("错误信息:", e)


if __name__ == "__main__":
    check_gpt_api()
