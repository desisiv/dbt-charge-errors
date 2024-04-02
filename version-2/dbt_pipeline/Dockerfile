FROM mcuadros/ofelia:latest
USER root

RUN apk update

#install python and pip
RUN apk add python3
RUN apk add py3-pip

#copy over dbt-trino profiles.yml and dbt project directory
ADD ./profiles.yml /root/.dbt/
ADD ./ /dbt_pipeline

#copy over ofelia scheduler config
ADD ./scheduler.ini /scheduler.ini

#install dbt-trino with dbt-core
RUN pip3 install --upgrade pip
RUN pip3 install dbt-trino

WORKDIR /dbt_pipeline

RUN echo "healthy" > ./healthy.txt

ENTRYPOINT ["ofelia", "daemon", "--config=../scheduler.ini"]