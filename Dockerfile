FROM python:3.9
WORKDIR /app
RUN python -m pip install --upgrade pip
RUN pip install "snowflake-connector-python[pandas]"
RUN pip install streamlit
EXPOSE 8501
COPY snowflurry.py /app
ENTRYPOINT [ "streamlit", "run" ]
CMD [ "snowflurry.py" ]
