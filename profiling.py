import pandas as pd
from ydata_profiling import ProfileReport
import streamlit as st

from streamlit_pandas_profiling import st_profile_report

df = pd.read_csv("customers-1000.csv")
pr = ProfileReport(df, title="Report")

st_profile_report(pr)