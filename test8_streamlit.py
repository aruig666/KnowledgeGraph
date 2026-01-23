import streamlit as st
import json


with open("data.json",encoding='utf-8') as f:
    messages = json.load(f)
import streamlit as st

st.set_page_config(layout="wide")
left, right = st.columns(2)

buffer = {"assistant": None, "function": None}

def flush_row(buf):
    if buf["assistant"] is None and buf["function"] is None:
        return
    with left:
        if buf["assistant"] is not None:
            st.write(buf["assistant"])
        else:
            st.write("")   # 或 st.empty()
    with right:
        if buf["function"] is not None:
            st.json(buf["function"])
        else:
            st.write("")
    st.divider()

for msg in messages:
    role = msg["role"]
    buffer[role] = msg["content"]

    # 如果一对齐全了，或者下一条还是同角色，就输出一行
    if role == "function" or buffer["assistant"] is not None and role == "assistant":
        flush_row(buffer)
        buffer = {"assistant": None, "function": None}

# 处理最后残留的一行
flush_row(buffer)