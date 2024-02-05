FROM ubuntu:22.04

# Install python and pip, and ffmpeg and git
RUN apt-get update && apt-get install -y python3 python3-pip ffmpeg git

# install pip packages
RUN pip install requests git+https://github.com/openai/whisper.git

WORKDIR /work

# Default command is to run python with the main script
CMD python3 /app/main.py
