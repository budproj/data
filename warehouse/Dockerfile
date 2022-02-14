FROM fishtownanalytics/dbt:1.0.0

WORKDIR /usr/app

ARG DW_HOST
ARG DW_USER
ARG DW_PASSWORD
ARG DW_DB
ARG DW_SCHEMA

RUN apt-get update && apt-get install -y gettext-base

ENV DW_HOST=$DW_HOST
ENV DW_USER=$DW_USER
ENV DW_PASSWORD=$DW_PASSWORD
ENV DW_DB=$DW_DB
ENV DW_SCHEMA=$DW_SCHEMA

RUN mkdir /root/.dbt
COPY ./profiles/template.yml ./profile_template.yml
RUN envsubst < ./profile_template.yml > /root/.dbt/profiles.yml

COPY ./seeds ./seeds
COPY ./macros ./macros
COPY ./models ./models
COPY ./dbt_project.yml ./
COPY ./packages.yml ./

ENTRYPOINT cat /root/.dbt/profiles.yml && dbt deps && dbt seed && dbt run
