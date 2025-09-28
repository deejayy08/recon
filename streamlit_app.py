# streamlit_app.py
import os
import streamlit as st
import tempfile
from app.orchestrator import (Orchestrator)

orc = Orchestrator()

st.title("Recon POC — Bedrock KB with chunk metadata (page/table/row/col)")

use_case = st.text_input("Use case id", value="payments_recon")
kb_id = st.text_input("KB id (existing Bedrock KB)", value="")
uploader = st.text_input("Uploader name", value="streamlit_user")

st.subheader("Upload up to 4 documents")
uploaded = st.file_uploader("Files (pdf/pptx/csv/xlsx/png/jpg)", accept_multiple_files=True, type=["pdf","pptx","csv","xlsx","png","jpg","jpeg"], help="Up to 4 files")
if uploaded:
    if not kb_id:
        st.error("Set the KB id for the use case (existing Bedrock KB).")
    else:
        if len(uploaded) > 4:
            st.error("Max 4 files")
        else:
            out = []
            for f in uploaded:
                tmp = tempfile.NamedTemporaryFile(delete=False)
                tmp.write(f.getbuffer())
                tmp.flush()
                res = orc.ingest_file_and_sync(use_case, kb_id, tmp.name, f.name, uploader)
                out.append(res)
                tmp.close()
            st.write(out)

st.subheader("Run recon (retrieve & generate via Claude)")
batch_id = st.text_input("Batch id (leave empty to search whole KB)")
user_query = st.text_area("User query / reconciliation instruction", value="Find mismatched amounts and give references to source rows/tables.")
global_template = st.text_area("Global template", value="Always normalize currency using the use-case currency table.")
usecase_template = st.text_area("Use-case template", value="Apply payment reconciliation rules: match on transaction id, then compare amounts.")

if st.button("Run Recon"):
    if not kb_id:
        st.error("Provide KB id")
    else:
        res = orc.query_kb_and_reconcile(use_case, kb_id, user_query, batch_id=batch_id or None, global_template=global_template, usecase_template=usecase_template)
        st.json(res)
