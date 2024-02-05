# vim:fileencoding=utf-8:ts=4:sw=4:sts=4:expandtab

import os
import subprocess
import requests
import time


# Get the value of an environment variable
URL = os.environ.get('URL')
SECRET_KEY = os.environ.get('SECRET_KEY')

params = {'SecretKey': SECRET_KEY}

def GetJob():
    try:
        # get the next job
        resp = requests.get(URL + '/GetJob', params=params)
        resp.raise_for_status()
        job = resp.json()

        return job
    except requests.exceptions.HTTPError as err:
        print(err)
        return None

while True:

    print('______________________________________________________')
    print('Starting a new iteration...')

    JobID = None
    Download_URL = None
    Upload_URL = None

    job = GetJob()

    if job is None:
        print('No job returned. Sleeping for 10 seconds...')
        time.sleep(10)
        continue

    JobID = job.get('JobID')
    Download_URL = job.get('Download_URL')
    Upload_URL = job.get('Upload_URL')

    if Download_URL is None:
        print('No job download specified.')
        requests.post(URL + '/PostJobError', params=params, json={'JobID': JobID, 'Error': 'No job download url found.'})
        print('Sleeping for 10 seconds...')
        time.sleep(10)
        continue

    FileName_Base = f'transcribe_{JobID}'
    FileName_Audio = f'{FileName_Base}'

    print(f'JobID: {JobID}')
    print(f'Download_URL: {Download_URL}')
    print(f'Upload_URL: {Upload_URL}')
    
    print('')

    try:
        print('Start file download...')

        # download the file
        resp = requests.get(Download_URL)
        resp.raise_for_status()

        with open('mediafile', 'wb') as file:
            file.write(resp.content)
        
        print('File downloaded.')
    except requests.exceptions.HTTPError as err:
        requests.post(URL + '/PostJobError', params=params, json={'JobID': JobID, 'Error': str(err)})
        print(err)
        continue

    try:
        print('Checking the file type using ffprobe...')
        
        # check the file's content type using ffprobe
        command = "ffprobe -v error -show_entries stream=codec_type -of default=noprint_wrappers=1 mediafile"
        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        print('Checked the codec type. It was ' + str(result.stdout))
    except subprocess.CalledProcessError as err:
        requests.post(URL + '/PostJobError', params=params, json={'JobID': JobID, 'Error': str(err) + ': ' + result.stderr})
        print(err)
        continue

    
    # If the output contains 'video', it's a video file
    if 'video' in result.stdout:
        print('It is a video file')

        # update the audio file name - ffmpeg is picky about the file extension
        FileName_Audio = FileName_Audio + '.wav'

        
        
        try:
            print('Extracting audio from the video file...')

            # Extract audio using ffmpeg, then clean up the video file
            command = f"ffmpeg -i mediafile -vn -acodec pcm_s16le -ar 44100 -ac 2 {FileName_Audio}"
            result = subprocess.run(command, shell=True, capture_output=True, text=True)

            if result.returncode != 0:
                error_output = result.stderr.strip()
                raise subprocess.CalledProcessError(result.returncode, 'ffmpeg', error_output)
        except subprocess.CalledProcessError as err:
            requests.post(URL + '/PostJobError', params=params, json={'JobID': JobID, 'Error': f"{err}: {err.stderr}"})
            print(err)
            continue

        print('Audio extraction complete.')
    elif 'audio' in result.stdout:
        print('It is an audio file.')

        # just rename it
        os.rename('mediafile', FileName_Audio)

        print('Renamed the audio file')
    # .m4a files seem to have subtitle stream, and are mp4 files without video stream
    elif 'subtitle' in result.stdout:
        print('It is a subtitle file, likely an m4a file with subtitle stream.')

        # just rename it
        os.rename('mediafile', FileName_Audio)

        print('Renamed the subtitle file')
    else:
        requests.post(URL + '/PostJobError', params=params, json={'JobID': JobID, 'Error': 'Incompatible codec type: ' + (result.stdout or 'None at all')})
        print('Unknown file type')
        continue
    
    
    try:
        print('Transcribing the audio file...')

        # transcribe the audio
        command = f"whisper {FileName_Audio} --model small --language en --output_format vtt"
        result = subprocess.run(command, shell=True, capture_output=True, text=True, env={"TORCH_HOME":"/work/.whisper_cache"})
        if result.returncode != 0:
            error_output = result.stderr.strip()
            raise subprocess.CalledProcessError(result.returncode, 'whisper', error_output)
    except subprocess.CalledProcessError as err:
        requests.post(URL + '/PostJobError', params=params, json={'JobID': JobID, 'Error': str(err) + ': ' + err.stderr})
        print(err)
        continue

    print('Audio file transcription complete.')

    try:
        print('Uploading the transcribed file to s3...')

        #read the file and upload it to s3 using the signed url
        with open(f'{FileName_Base}.vtt', 'rb') as file:
            resp = requests.put(Upload_URL['UploadURL'], data=file)
            resp.raise_for_status()
        
        print('Transcription upload to s3 complete.')
    except requests.exceptions.HTTPError as err:
        requests.post(URL + '/PostJobError', params=params, json={'JobID': JobID, 'Error': str(err)})
        print(err)
        continue
    
    try:
        print('Cleaning up...')
        subprocess.call("rm -rf /work/*", shell=True)

    except FileNotFoundError:
        pass

    print('Sleeping for 10 seconds...')
    time.sleep(10)
