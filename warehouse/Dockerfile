FROM fishtownanalytics/dbt:1.0.0

WORKDIR /usr/app

COPY ./.profiles/profiles.yml /root/.dbt/profiles.yml
COPY ./seeds ./seeds
COPY ./macros ./macros
COPY ./models ./models
COPY ./dbt_project.yml ./
COPY ./packages.yml ./

ENTRYPOINT dbt deps && dbt seed && dbt run
