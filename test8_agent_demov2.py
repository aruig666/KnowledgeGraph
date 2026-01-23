from datetime import datetime
import json
import os
import pathlib
import re
import traceback
import asyncio

from dotenv import load_dotenv
from qwen_agent.agents import Assistant
from tools.execute_cypher import ExecuteCypherTool, QueryCypherEmbeddingTool
from utils.jsonhelper import load_sub_agent_result, response2json, save_item_in_json

# from utils.memory import HybridMemory
SYSTEM_PROMPT = '''
你是一个 Neo4j 图数据库智能体（Text2Cypher Agent）。
你拥有一个 Cypher 执行工具：execute_cypher(cypher: string) -> result。
你的任务是：把用户自然语言需求自动转成 Cypher 查询并执行。
但你不知道知识图谱的结构，所以需要先检索模式层，再根据输入的标签和嵌入向量检索，最相似的三个。
最后沿着关系的方向，召回节点的邻接节点。
Surface可以召回MachiningFeature，MachiningFeature到ProcessUnit，ProcessUnit到Operation，最后是Tool

严禁使用改变知识图谱属性和结构的指令
你逐步执行流程，一次只能执行一步。
每一步都要思考并调用工具，自动查询可能感兴趣的内容
输出Operation和Tool后停止。

'''


class KnowledgeGraphAgent:
    def __init__(self, task: str, propertys: dict, max_loop: int, model_name: str = "gpt-4.1-mini") -> None:

        self.llm_cfg = {
            'model': os.environ['OPENAI_MODEL_NAME'],
            'model_server': os.environ['OPENAI_BASE_URL'],
            'api_key': os.environ['OPENAI_API_KEY1'],
            # 'generate_cfg': {
            #     'top_p': 0.8
            # }
        }
        print(self.llm_cfg)
        self.task = task
        self.propertys = propertys
        self.max_rounds = max_loop
        self.round = 0

        self.tools = [
            ExecuteCypherTool(),
            QueryCypherEmbeddingTool()
        ]
        self.name = "KnowledgeGraphAgent"
        self.description = "根据用户输入的节点信息，自动生成 Cypher 查询语句并执行，返回查询结果。"
        self.parameters = [{
            "name": "query",
            "type": "string",
            "description": "The query of user",
            "required": True
        }]
        self.bot = Assistant(
            llm=self.llm_cfg,
            function_list=self.tools,
            # files=stp_files,
            # description=self.description
        )

    async def run(self, loop=False):
        result_all = []
        while True:

            messages = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(self.propertys, ensure_ascii=False)},
                {"role": "user", "content": self.task}
            ]

            responses = self.bot.run_nonstream(messages)

            content = responses[0]['content']
            response_json = response2json(content)
            tool_output = await self.run_tool(response_json['tool_name'], json.dumps(response_json['call_paras']))

            result_item = {"agent_name": self.name,
                           "reason": response_json['reason'],
                           "result": tool_output
                           }
            result_all.append(result_item)
            # 4. 判断是否结束
            self.round += 1
            if self.max_rounds and self.round >= self.max_rounds:
                break

            if not loop:
                break

        return result_all

    async def run_tool(
            self, tool_id: str, tool_input: dict, context: str | None = None
    ) -> str:
        try:
            tool = self.bot.function_map[tool_id]
            tool_record = await tool.call(tool_input)
            return tool_record
        except Exception as e:
            print(f"Failed to run tool {tool_id}")
            print(traceback.format_exc())
            return f"Tool execution failed: {e}"


def main(task, propertys, max_loop):
    agent = KnowledgeGraphAgent(task, propertys, max_loop=max_loop)

    try:
        result = asyncio.run(agent.run(loop=True))
    except Exception as e:
        import sys, traceback
        print("错误信息:", e)
        print("详细信息:", sys.exc_info())
        traceback.print_exc()
    return result


if __name__ == "__main__":
    load_dotenv()
    propertys = {
        "label": "Surface",
        "embedding": [-0.01838618516921997, -0.6906036734580994, -0.8806816339492798, -0.24042096734046936,
                      -0.3494674563407898, -0.7708428502082825, -0.9503121972084045, -0.10143092274665833,
                      0.670846700668335, 0.458579957485199, 0.11778119206428528, -0.353695273399353,
                      -0.4043779969215393, 0.012008432298898697, 0.24277380108833313, 0.3698858618736267,
                      -0.4975522458553314, 0.5655354857444763, -0.5522143840789795, 0.5355432033538818,
                      0.44269293546676636, -0.061983950436115265, 0.4237976670265198, -0.9028345346450806,
                      -0.0566115528345108, 0.04464548081159592, 0.18457025289535522, -0.6989099979400635,
                      -0.8627482652664185, -0.6678647994995117, 0.3505161702632904, -0.8253023028373718,
                      -0.14858278632164001, -0.868085503578186, 0.004028609022498131, 0.5805688500404358,
                      -0.24223338067531586, -0.49287405610084534, 0.29024630784988403, -0.7591813802719116,
                      -0.8590357899665833, -0.11347335577011108, -0.1424797922372818, 0.43985715508461,
                      -0.8233121037483215, 0.8255826830863953, -0.5990272760391235, -0.10639093816280365,
                      0.45035403966903687, -1.0479824542999268, -0.7658216953277588, 0.6887220740318298,
                      -0.21506373584270477, -0.9769014716148376, -0.7512994408607483, -0.020453397184610367,
                      -0.38907214999198914, 0.08007098734378815, -0.22990649938583374, 0.29622289538383484,
                      -0.8738183975219727, 0.8234713673591614, 0.44007444381713867, -0.5227694511413574]
    }
    task = "请帮我找到图数据库中与以下节点最相似的三个节点，并列出它们的关键属性和值："
    max_loop = 10
    main(task=task, propertys=propertys, max_loop=max_loop)

    # from qwen_agent.gui import WebUI
    # agent = Agent(task=task, model_name = model_name)
    # bot = agent.bot
    # WebUI(bot).run()
