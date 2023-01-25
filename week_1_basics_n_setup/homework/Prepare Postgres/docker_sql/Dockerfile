FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy  psycopg2

# This is the location in the image in the container where the file will be copy
WORKDIR /app

# This command copies the file from the current directory to the docker image
#COPY pipeline.py pipeline.py 
COPY ingest_data.py ingest_data.py 

#The argument "bash" specifies that when we build the image, the bash(command line interface) will be displayed.
# If we want to run python code then we need to write 'python' in the command line.
# ENTRYPOINT [ "bash" ]

#This specifies that when  docker run is executed it will run  directly the "pipeline.py" file.
#ENTRYPOINT [ "python", "pipeline.py" ]
ENTRYPOINT [ "python", "ingest_data.py" ]