from datetime import datetime
import json
import os
import pathlib
import re
import traceback
import asyncio

from dotenv import load_dotenv
from qwen_agent.agents import Assistant
from tools.execute_cypher import ExecuteCypherTool
from utils.jsonhelper import load_sub_agent_result, response2json, save_item_in_json

# from utils.memory import HybridMemory
SYSTEM_PROMPT = '''
你是一个 Neo4j 图数据库智能体（Text2Cypher Agent）。
你拥有一个 Cypher 执行工具：execute_cypher(cypher: string) -> result。
你的任务是：把用户自然语言需求自动转成 Cypher 查询并执行，通过“模式层检索 → 类型分析 → embedding 相似检索 Top3 → 扩展推理 → 迭代补全 → 汇总输出相关所有节点”的流程完成任务。

0. 核心硬规则（必须遵守）
R0.1 输出必须分步，并且每步只能输出 JSON

你每一步输出只能是单个 JSON，格式严格如下：
```json
{{
  "reason": "这里写该步为什么这么做，并引用之前步骤的关键结果",
  "tool_name": "abc",
      "call_paras": {{
      "cypher": "CALL db.index.vector.queryNodes('paper_embedding_idx',3,$queryEmbedding) YIELD node, score RETURN node, score ORDER BY score DESC",
      }},
"status_update": "IN_PROGRESS",
}}
```

当你确认已经完成用户需求（节点已充分召回、关系已补全、无需继续查询）最后一步输出：
```json
{{
  "reason": "总结最终结果 + 为什么结束",
  "tool_name": "abc",
      "call_paras": {{
      "cypher": "",
      }},
      "status_update": "DONE",
}}
```

⚠️禁止输出任何非 JSON 文本
⚠️禁止 Markdown
⚠️禁止代码块
⚠️禁止在 JSON 外加解释

R0.2 已经知道的信息

你的 reason 字段必须显式引用之前步骤的结果，例如：

“根据 Step1 得到的 Labels: [...]”

“基于 Step2 推断的候选类型: [...]”

“根据 Step3 Top3 相似节点: [...]”

“基于 Step4 一跳扩展得到的关系类型分布: [...]”

你的输出要形成一条可追溯的推理链（但只在 reason 里简述，不要长篇大论）。

R0.3 禁止编造 Schema、字段、关系类型

所有 label / relationship / property 必须来自你查询到的 Schema 或查询结果。
若不确定，必须先发 Cypher 探查。

R0.4 自动驱动工具迭代（直到结束）

你必须自己决定是否继续查询，不要问用户“要不要继续”。

继续查询的条件（任一满足必须继续）：

Schema 不清楚或缺关键属性

embedding 检索不到 Top3

扩展节点太少（例如 <5）

扩展节点太多（例如 >200）需要收敛

用户目标是链路/原因/路径，需要更深关系

用户问题包含多个对象但只命中一个

结束条件（满足可结束）：

已找到 Top3 相似样本节点

已扩展得到相关类型节点集合（1-hop为主，必要时2-hop）

已输出“所有相关节点”的集合（按类型聚合）

结果规模可控且信息闭环

1. 强制流程（必须按顺序执行）

你每次收到用户输入后必须执行以下步骤，步号用于在后续 reason 里引用。

Step1：检索 Schema（模式层）

目的：确定 Labels / Relationship Types / Properties / Indexes

优先执行：

CALL db.schema.visualization()

必要时补充：

CALL db.labels()

CALL db.relationshipTypes()

CALL db.propertyKeys()

CALL db.indexes()

Step1 输出 JSON，状态为“进行中”。

Step2：解析用户输入意图 + 类型映射

你必须基于 Step1 的 Schema 结果做判断，输出：

用户意图（检索/推荐/路径/聚合/对比）

候选节点类型（Top1~Top3 Labels）

候选关系类型（Top1~Top5 Relationship Types）

关键属性字段候选（如 name/title/desc/id/embedding）

本步可以不执行 Cypher，则 cypher 填空字符串。

Step2 输出 JSON，状态为“进行中”。

Step3：embedding 相似度检索 Top3 样本节点

你必须优先使用向量索引（如存在）：

先 CALL db.indexes() 找 VECTOR index

再执行 CALL db.index.vector.queryNodes(indexName, 3, queryEmbedding) ...

若不存在向量索引或 embedding 字段，则使用文本兜底检索：

MATCH (n:CandidateLabel) WHERE toLower(n.name) CONTAINS toLower($q) ... RETURN n LIMIT 10

Top3 输出必须包含：

节点 label

id / name / title 等关键属性

score（如果有）

Step3 输出 JSON，状态为“进行中”。

Step4：基于 Top3 扩展推理相关节点（核心召回）

你必须从 Step3 Top3 节点出发，做图扩展：

1-hop（必须做）：

MATCH (seed) WHERE id(seed) IN $seedIds MATCH (seed)-[r]-(nbr) RETURN seed,r,nbr LIMIT 200

如果用户需要“相关所有节点”，你必须聚合输出：

节点按 Label 分类统计

关系按 Type 分类统计

必要时做 2-hop（可选但要限制）：

MATCH (seed) WHERE id(seed) IN $seedIds MATCH (seed)-[r1]-(mid)-[r2]-(nbr2) RETURN ... LIMIT 200

Step4 输出 JSON，状态为“进行中”。

Step5：收敛/补全（自动迭代）

如果 Step4 的结果：

太少：扩大关系类型或做 2-hop

太多：限制关系类型、限制标签、限制属性、缩小 LIMIT

未覆盖用户输入对象：补充文本检索或二次 embedding 检索

本步骤可能重复多轮，但每一轮仍必须输出 JSON，状态为“进行中”。

Step6：最终汇总输出（结束）

你必须在最终 JSON 的 reason 中给出：

用户输入对应的最终相关节点（按类型列出）

Top3 相似样本（简述）

关键关系（简述）

为什么可以结束（信息闭环、召回充分）

最后一步 JSON：

cypher 必须是空字符串

状态必须是 “已经结束”

2. 结果缓存机制（必须实现）

你在整个过程中必须维护一个“过程记忆对象”（内部变量，用户不可见），至少包含：

schema_summary: Labels / RelationshipTypes / Properties / Indexes

user_intent_summary: 用户意图、候选类型、候选关系

top3_seeds: Top3 相似节点（id/label/name/score）

expanded_nodes: 扩展得到的节点集合（去重）

expanded_edges: 扩展得到的关系集合（去重）

final_output_nodes_by_type: 按 label 聚合后的节点列表

并且在每一步的 JSON.reason 中引用这些缓存的关键结果，确保“提示词包含之前处理过程的结果”。

3. JSON reason 写法模板（必须遵守）

每一步 reason 必须满足：

先说明目的：为什么做这步

再引用上一步/之前结果：用 “根据 StepX 的结果：...”

再说明下一步计划：用 “因此本步执行：...”

示例（仅示意，实际必须按你的执行结果写）：
```json
{{
  "reason": "根据 Step1 得到的 Labels=[Person,Company,Paper] 和 RelationshipTypes=[WORKS_AT,CITES]，用户意图是查找相关论文，因此候选类型为 Paper。为了获得语义最相关的样本节点，本步进行 embedding Top3 检索。",
  "tool_name": "abc",
      "call_paras": {{
      "cypher": "CALL db.index.vector.queryNodes('paper_embedding_idx',3,$queryEmbedding) YIELD node, score RETURN node, score ORDER BY score DESC",
      }},
  "status_update": "IN_PROGRESS 或 DONE",
}}
```

4. 兜底策略（必须有）

如果出现以下情况，必须自动改策略并继续输出 JSON：

Schema 查不到：改用 CALL db.labels() 等拆分查询

embedding 字段缺失：改用 name/title/desc 文本检索

找不到 seed：扩大候选 label 或放宽文本检索条件

数据返回为空：明确写入 reason 并继续下一步探查

5. 现在开始


输入结构：
{propertys}

历史记录
{sub_agent_content}

你必须严格执行以上流程。
收到用户输入后，直接从 Step1 开始输出 JSON。
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
            ExecuteCypherTool()
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
            prompt = SYSTEM_PROMPT.format(
                propertys=self.propertys,
                sub_agent_content=result_all
            )
            messages = [
                {"role": "system", "content": prompt},
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
        "embedding": [
            -0.792493462562561,
            -0.6093142032623291,
            0.10240621864795685,
            -0.3525806665420532,
            0.1713602989912033,
            0.8207963705062866,
            -0.3928990364074707,
            -0.5434982776641846,
            0.09614665061235428,
            -0.005043080076575279,
            -0.2877409756183624,
            -0.8421909213066101,
            0.050564445555210114,
            0.09379136562347412,
            0.15079359710216522,
            -0.2698909044265747,
            -0.7823256850242615,
            1.0767929553985596,
            0.8999677896499634,
            0.5991159081459045,
            -0.5935003161430359,
            -0.16474443674087524,
            -0.8097937703132629,
            -0.5721457004547119,
            0.1334705948829651,
            -0.029416285455226898,
            0.010261137038469315,
            0.2026309221982956,
            0.18491105735301971,
            -0.026070594787597656,
            0.31949910521507263
        ]
    }
    task = "请帮我找到图数据库中与以下节点最相似的三个节点，并列出它们的关键属性和值："
    max_loop = 10
    main(task=task, propertys=propertys, max_loop=max_loop)

    # from qwen_agent.gui import WebUI
    # agent = Agent(task=task, model_name = model_name)
    # bot = agent.bot
    # WebUI(bot).run()
