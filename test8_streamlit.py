import streamlit as st
import json
st.markdown("""
<style>
html, body, [class*="css"]  {
    font-size: 20px;
}
</style>
""", unsafe_allow_html=True)

with open("data.json",encoding='utf-8') as f:
    messages = json.load(f)
import streamlit as st

st.set_page_config(layout="wide")
left, right = st.columns(2)



def flush_row(buf):
    print("buf:", buf)

    # if buf["assistant"] is None and buf["function"] is None:
    #     return
    with left:
        if buf["role"] == "assistant":
            st.write(buf["content"])
        else:
            st.write("")   # 或 st.empty()
    with right:
        if buf.get("function_call",None) is not None:
            print("function_call:", buf)
            st.json(buf["function_call"])

        if buf['role'] == "function":
            st.json(buf.get("name"))
            st.json(buf.get("content"))
        else:
            st.write("")
    st.divider()

for msg in messages:
    flush_row(msg)

    # role = msg["role"]
    # buffer[role] = msg["content"]

    # # 如果一对齐全了，或者下一条还是同角色，就输出一行
    # if role == "function" or buffer["assistant"] is not None and role == "assistant":
    #     buffer = {"assistant": None, "function": None}

# 处理最后残留的一行
flush_row(buffer)