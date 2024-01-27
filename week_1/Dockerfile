# Select base image
FROM python:3.9

# Install dependencies
RUN pip install pandas

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY pypeline.py pipeline.py

# Specify the command to run on container start
ENTRYPOINT [ "bash" ]
