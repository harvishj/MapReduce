# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Workers do not need to expose ports since they initiate connections to the master
# However, if you plan to implement a listening service on workers, uncomment the next line
EXPOSE 5000

# The default command will be overridden by docker-compose.yml
# CMD ["python", "./worker.py"]