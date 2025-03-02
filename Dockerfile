FROM apache/airflow:slim-2.10.5-python3.12

ARG DAGS_FOLDER=uk

ENV DAG_IGNORE_FILE_SYNTAX=glob

COPY <<EOF /opt/airflow/dags/.airflowignore
*/
!airflow_dags/dags/$DAGS_FOLDER/*
EOF

COPY --chown=airflow:root src /opt/airflow/dags

COPY --chown=airflow:root pyproject.toml /opt/airflow/dags
RUN cd /opt/airflow/dags && mkdir src && pip install -e .

# Use the same entrypoint as the default airflow image
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD []

