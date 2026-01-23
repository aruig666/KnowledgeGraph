import os
from qwen_agent.tools import BaseTool

class SearchTool(BaseTool):
    name = "search_part"
    description = "Search part information from database"

    def call(self, query: str):
        if "S3M060" in query:
            return {
                "material": "Al6061",
                "volume_mm3": 12000,
                "process": ["turning", "milling_keyway", "anodizing"]
            }
        return {}

class CostTool(BaseTool):
    name = "estimate_cost"
    description = "Estimate machining cost"

    def call(self, volume_mm3: float, process: list):
        material_cost = volume_mm3 * 2.7e-6 * 20   # 简化模型
        process_cost = len(process) * 15
        return {
            "material_cost": round(material_cost, 2),
            "process_cost": process_cost,
            "total_cost": round(material_cost + process_cost, 2)
        }

class DeliveryTool(BaseTool):
    name = "predict_delivery"
    description = "Predict delivery time"

    def call(self, process: list):
        return {
            "days": 1 + 0.5 * len(process)
        }


from dotenv import load_dotenv
load_dotenv()

from qwen_agent.agents import Assistant
llm_cfg = {
            'model': os.environ['OPENAI_MODEL_NAME'],
            'model_server': os.environ['OPENAI_BASE_URL'],
            'api_key': os.environ['OPENAI_API_KEY1'],
            # 'generate_cfg': {
            #     'top_p': 0.8
            # }
        }
agent = Assistant(
    llm=llm_cfg,
    function_list=[SearchTool(), CostTool(), DeliveryTool()],
    system_message="You are a manufacturing process planning agent."
)


query = "请给我S3M060同步带轮的加工工艺、成本和交期"
messages = [
        {"role": "user", "content": query}
    ]

result = agent.run_nonstream(messages)
content = result[0]['content']

print(content)