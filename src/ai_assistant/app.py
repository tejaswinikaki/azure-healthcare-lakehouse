import os

import streamlit as st
from google import genai

from healthcare_insights import get_summary, load_patients


st.set_page_config(page_title="HealthLake AI Insights", page_icon="🏥")

st.title("🏥 HealthLake AI Patient Insights")
st.caption("AI-assisted analysis of synthetic healthcare patient data.")

patients = load_patients()
summary = get_summary()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Patient Records", summary["total_patients"])
col2.metric("Unique Patients", summary["unique_patients"])
col3.metric("States", summary["states"])
col4.metric("Missing Values", summary["missing_values"])

st.subheader("Patients by State")
state_counts = patients["state"].value_counts().reset_index()
state_counts.columns = ["State", "Patient Count"]
st.bar_chart(state_counts.set_index("State"))

st.subheader("Ask HealthLake AI")
question = st.text_input(
    "Ask a question about the patient data",
    placeholder="Example: What are the main data-quality findings?",
)

if st.button("Get AI insight"):
    if not question:
        st.warning("Please type a question first.")
    elif not os.getenv("GEMINI_API_KEY"):
        st.error("Gemini API key is not available in this terminal session.")
    else:
        data_context = f"""
        This project uses synthetic patient data only.

        Dataset summary:
        - Total patient records: {summary["total_patients"]}
        - Unique patient IDs: {summary["unique_patients"]}
        - States represented: {summary["states"]}
        - Missing values: {summary["missing_values"]}

        Patients by state:
        {state_counts.to_string(index=False)}
        """

        client = genai.Client()
        response = client.interactions.create(
            model="gemini-3.1-flash-lite",
            input=f"""
            You are the HealthLake AI assistant.
            Answer only from the dataset details below.
            Be concise and do not invent facts.

            {data_context}

            User question: {question}
            """,
        )

        st.success(response.output_text)

st.subheader("Data Preview")
st.dataframe(patients, width="stretch")