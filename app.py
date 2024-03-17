import streamlit as st
from pipeline import pipeline

st.title("Processador de arquivos")

if st.button('Processar'):
    with st.spinner('Processando...'):
        logs = pipeline()
        for log in logs:
            st.write(log)