# Use an official Python 3.10 image with Debian as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR C:/Users/hp/Desktop/Docker-containers/Lichess_ETL/app

# Copy your local ETL code and requirements.txt into the container
# Assuming your code is in the same directory as the Dockerfile
COPY ./requirements.txt .

# Install any Python dependencies listed in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables if needed (e.g., for configurations)
# ENV MY_ENV_VAR=value

# Specify the command to run when the container starts
CMD ["python"]