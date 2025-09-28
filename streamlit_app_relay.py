# streamlit_app.py
import streamlit as st
from app.orchestrator import Orchestrator

orc = Orchestrator()

st.title("Recon POC — Bedrock KB")

tab1, tab2 = st.tabs(["Ingest & Recon", "Replay"])

with tab1:
    st.subheader("Upload & Ingest")
    use_case = st.text_input("Use case id", value="payments_recon")
    kb_id = st.text_input("KB id (existing Bedrock KB)", value="")
    uploader = st.text_input("Uploader name", value="streamlit_user")

    uploaded = st.file_uploader("Files", accept_multiple_files=True, type=["pdf","pptx","csv","xlsx","png","jpg","jpeg"])
    if uploaded and kb_id:
        for f in uploaded:
            import tempfile
            tmp = tempfile.NamedTemporaryFile(delete=False)
            tmp.write(f.getbuffer())
            tmp.flush()
            res = orc.ingest_file_and_sync(use_case, kb_id, tmp.name, f.name, uploader)
            st.json(res)
            tmp.close()

    st.subheader("Run Recon")
    batch_id = st.text_input("Batch id for recon (optional)")
    user_query = st.text_area("Recon query", value="Find mismatched amounts.")
    global_template = st.text_area("Global template", value="Always normalize currency.")
    usecase_template = st.text_area("Use-case template", value="Apply payment rules.")
    if st.button("Run Recon"):
        res = orc.query_kb_and_reconcile(use_case, kb_id, user_query, batch_id or None, global_template, usecase_template)
        st.json(res)

with tab2:
    st.subheader("Replay Recon Results")
    use_case_replay = st.text_input("Use case id for replay", value="payments_recon")
    if st.button("List Recons"):
        recons = orc.list_recons(use_case_replay)
        if recons:
            for rec in recons:
                st.markdown(f"**Recon {rec['recon_id']}** (Batch: {rec.get('batch_id')})")
                st.json(rec.get("payload", {}))
                if "references" in rec.get("payload", {}):
                    for ref in rec["payload"]["references"]:
                        if st.button(f"View snippet {ref['kb_chunk_id']}", key=f"{rec['recon_id']}-{ref['kb_chunk_id']}"):
                            snippet = orc.fetch_reference_snippet(ref)
                            st.write(snippet)
        else:
            st.info("No recon results found.")
